from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import settings
from app.services.jobs.aggregator import JobAggregator
from app.services.jobs.taxonomy import normalize_role, role_domain, role_family

DEFAULT_ROLE_MATRIX = [
    "Data Analyst",
    "Data Scientist",
    "Data Engineer",
    "Software Engineer",
    "Full Stack Developer",
    "Frontend Developer",
    "DevOps Engineer",
    "Cloud Engineer",
    "Cybersecurity Engineer",
    "QA Engineer",
    "Solutions Architect",
    "Technical Writer",
    "Salesforce Admin",
]


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


async def run_case(*, query: str, location: str, limit: int) -> dict[str, Any]:
    aggregator = JobAggregator(None)
    started = time.perf_counter()
    jobs = await aggregator.fetch_jobs(query=query, location=location, limit=limit)
    elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
    diagnostics = _json_safe(getattr(aggregator, "last_fetch_diagnostics", {}) or {})

    return {
        "query": query,
        "normalized_role": normalize_role(query),
        "family_role": role_family(query),
        "domain": role_domain(query),
        "elapsed_ms": elapsed_ms,
        "provider_plan": diagnostics.get("provider_plan", {}),
        "provider_request_summary": diagnostics.get("provider_request_summary", {}),
        "provider_match_counts": diagnostics.get("provider_match_counts", {}),
        "stage_results": diagnostics.get("stage_results", []),
        "underfill": diagnostics.get("underfill", {}),
        "final_live_count": len([job for job in jobs if job.get("source") != "role-baseline"]),
        "top_selected_titles": [str(job.get("title", "")) for job in jobs[:10] if job.get("source") != "role-baseline"],
        "selection_rejections": ((diagnostics.get("selection_debug") or {}).get("rejections") or {}),
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description="Run the production live-fetch matrix against representative role families.")
    parser.add_argument("--location", default="Global", help="Location to send with the fetch requests.")
    parser.add_argument("--limit", type=int, default=10, help="Requested live listing cap per query.")
    parser.add_argument("--query", action="append", dest="queries", help="Optional role query to run. Repeat for multiple queries.")
    args = parser.parse_args()

    settings.environment = "production"
    settings.llm_provider = "disabled"

    queries = args.queries or DEFAULT_ROLE_MATRIX
    results = [await run_case(query=query, location=args.location, limit=args.limit) for query in queries]
    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
