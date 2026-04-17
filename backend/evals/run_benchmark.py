from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import settings
from app.services.analysis.scoring import ScoringEngine
from app.services.nlp.skill_grounding import SkillGroundingService


def _in_range(value: float, bounds: dict[str, Any] | None) -> bool:
    if not bounds:
        return True
    minimum = float(bounds.get("min", value))
    maximum = float(bounds.get("max", value))
    return minimum <= float(value) <= maximum


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            return _json_safe(value.item())
        except Exception:
            return value
    return value


async def run_case(case: dict[str, Any], *, service: SkillGroundingService, scorer: ScoringEngine) -> dict[str, Any]:
    resume_data = case["resume_data"]
    jobs = case.get("jobs", [])
    if not jobs:
        jobs = await service.build_fallback_jobs(
            role_query=case["role_query"],
            location=case.get("location", ""),
            resume_data=resume_data,
        )
    grounded_resume, grounded_jobs, _ = await service.ground(
        role_query=case["role_query"],
        resume_data=resume_data,
        jobs=jobs,
    )
    payload = scorer.score(grounded_resume, grounded_jobs)

    matched = set(payload["matched_skills"])
    missing = {item["skill"] for item in payload["missing_skills"]}
    expected = case.get("expected", {})

    checks = {
        "overall": _in_range(payload["overall_score"], expected.get("overall")),
        "ats": _in_range(payload["breakdown"]["ats_compliance"], expected.get("ats")),
        "resume_quality": _in_range(payload["breakdown"]["resume_quality"], expected.get("resume_quality")),
        "must_match": set(expected.get("must_match", [])).issubset(matched),
        "must_missing": set(expected.get("must_missing", [])).issubset(missing),
        "must_not_missing": set(expected.get("must_not_missing", [])).isdisjoint(missing),
    }

    return {
        "id": case["id"],
        "passed": all(checks.values()),
        "checks": checks,
        "overall_score": float(payload["overall_score"]),
        "breakdown": _json_safe(payload["breakdown"]),
        "matched_skills": payload["matched_skills"],
        "missing_skills": _json_safe(payload["missing_skills"]),
    }


async def main() -> int:
    cases_path = Path(__file__).with_name("benchmark_cases.json")
    cases = json.loads(cases_path.read_text(encoding="utf-8"))
    settings.llm_provider = "disabled"
    service = SkillGroundingService()
    scorer = ScoringEngine()
    results = [await run_case(case, service=service, scorer=scorer) for case in cases]

    print(json.dumps(results, indent=2))
    failures = [item for item in results if not item["passed"]]
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
