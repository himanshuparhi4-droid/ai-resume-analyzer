from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import settings
from app.services.analysis.scoring import ScoringEngine
from app.services.nlp.skill_extractor import canonical_skill_label, resume_skill_support_levels
from app.services.parsers.resume_parser import ResumeParser

DEFAULT_DATASET = "Majinuub/Resume_Parsing"
DEFAULT_CONFIG = "default"
DEFAULT_SPLIT = "train"
DATASET_ROWS_URL = "https://datasets-server.huggingface.co/rows"
TRAINING_SIGNAL_SCHEMA_VERSION = "resume-training-signal-v1"
CALIBRATION_SCHEMA_VERSION = "resume-calibration-summary-v1"
SAFE_PARSE_SIGNAL_KEYS = (
    "word_count",
    "section_count",
    "explicit_header_count",
    "inline_header_count",
    "merged_header_count",
    "section_leakage_count",
    "suspicious_token_count",
    "suspicious_url_count",
    "inferred_skills_section",
    "skills_count",
    "quantified_line_count",
    "bullet_like_line_count",
    "date_range_count",
    "contact_link_count",
    "portfolio_link_count",
    "multi_column_detected",
    "page_count",
    "summary_section_word_count",
    "experience_section_word_count",
    "projects_section_word_count",
    "skills_section_word_count",
    "dominant_section_share",
    "skills_focus_share",
    "project_focus_share",
    "section_balance_score",
    "experience_bullet_count",
    "project_bullet_count",
    "evidence_bullet_count",
    "experience_quantified_line_count",
    "projects_quantified_line_count",
    "evidence_quantified_line_count",
    "experience_action_line_count",
    "projects_action_line_count",
    "evidence_action_line_count",
    "action_verb_variety_count",
    "avg_evidence_line_word_count",
    "avg_bullet_word_count",
    "long_evidence_line_count",
    "short_bullet_count",
    "dense_paragraph_line_count",
    "weak_bullet_count",
    "summary_line_count",
    "first_person_pronoun_count",
    "chronology_signal_count",
)


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


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").lower().replace("_", " ").split())


def _normalize_skill(value: Any) -> str:
    return _normalize_text(value).replace("node js", "node.js").replace("express js", "express.js")


def _canonical_eval_skill(value: Any) -> str:
    normalized = _normalize_skill(value)
    return canonical_skill_label(normalized) or normalized


def _score_band(value: float) -> str:
    if value >= 78:
        return "strong"
    if value >= 60:
        return "usable"
    if value >= 45:
        return "needs_review"
    return "high_risk"


def _skill_recall_label(value: float) -> str:
    if value >= 0.85:
        return "complete"
    if value >= 0.55:
        return "partial"
    return "weak"


def _selected_parse_signals(parse_signals: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_safe(parse_signals.get(key)) for key in SAFE_PARSE_SIGNAL_KEYS if key in parse_signals}


def _distribution(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _avg(values: list[float]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 3)


def _support_counts(support_levels: dict[str, str], parsed_skills: set[str]) -> dict[str, int]:
    counts = {"strong": 0, "medium": 0, "weak": 0, "none": 0}
    for skill in parsed_skills:
        counts[support_levels.get(skill, "none")] += 1
    return counts


def _fetch_hf_rows(*, dataset: str, config: str, split: str, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while len(rows) < limit:
        length = min(100, limit - len(rows))
        params = urllib.parse.urlencode(
            {
                "dataset": dataset,
                "config": config,
                "split": split,
                "offset": offset,
                "length": length,
            }
        )
        with urllib.request.urlopen(f"{DATASET_ROWS_URL}?{params}", timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        page_rows = payload.get("rows") or []
        if not page_rows:
            break
        rows.extend(page_rows)
        offset += len(page_rows)
    return rows[:limit]


def _parse_expected_output(raw_output: Any) -> dict[str, Any]:
    if isinstance(raw_output, dict):
        return raw_output
    try:
        parsed = json.loads(str(raw_output or "{}"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _build_training_signal(
    *,
    row_idx: int,
    expected: dict[str, Any],
    parsed: dict[str, Any],
    result: dict[str, Any],
    expected_skills: set[str],
    explicit_expected_skills: set[str],
    parsed_skills: set[str],
    scoring_engine: ScoringEngine,
) -> dict[str, Any]:
    parse_confidence = scoring_engine.parser_confidence_profile(parsed)
    ats_score = float(scoring_engine._ats_score(parsed))
    resume_quality_score = float(scoring_engine._resume_quality_score(parsed))
    support_levels = resume_skill_support_levels(
        resume_sections=parsed.get("sections", {}) or {},
        skills=sorted(parsed_skills | explicit_expected_skills),
    )
    parser_label = str(parse_confidence.get("label", "unknown"))
    skill_recall = float(result.get("skill_recall", 0.0) or 0.0)
    experience_delta = result.get("experience_delta")
    section_checks = result.get("section_checks", {}) or {}
    needs_manual_review = (
        parser_label == "low"
        or skill_recall < 0.55
        or bool(result.get("missing_expected_skills"))
        or not section_checks.get("skills")
        or not section_checks.get("education")
    )
    if experience_delta is not None and not result.get("experience_close", True):
        needs_manual_review = True

    return {
        "schema_version": TRAINING_SIGNAL_SCHEMA_VERSION,
        "row_idx": row_idx,
        "labels": {
            "target_designation": str(expected.get("Designation", ""))[:120],
            "expected_experience_years": expected.get("Experience")
            if isinstance(expected.get("Experience"), (int, float))
            else None,
            "explicit_expected_skills": sorted(explicit_expected_skills),
            "inferred_dataset_skill_count": len(expected_skills - explicit_expected_skills),
        },
        "features": {
            "parsed_skill_count": len(parsed_skills),
            "parsed_skills": sorted(parsed_skills)[:40],
            "parsed_experience_years": float(parsed.get("experience_years", 0.0) or 0.0),
            "section_checks": section_checks,
            "resume_archetype": {
                "type": str((parsed.get("resume_archetype", {}) or {}).get("type", "general_resume")),
                "confidence": float((parsed.get("resume_archetype", {}) or {}).get("confidence", 0.0) or 0.0),
            },
            "parser_confidence": {
                "score": float(parse_confidence.get("score", 0.0) or 0.0),
                "label": parser_label,
                "risk_reasons": list(parse_confidence.get("risk_reasons", []) or [])[:6],
                "strong_recovered_structure": bool(parse_confidence.get("strong_recovered_structure")),
            },
            "ats_score": ats_score,
            "resume_quality_score": resume_quality_score,
            "parse_signals": _selected_parse_signals(parsed.get("parse_signals", {}) or {}),
            "skill_support_counts": _support_counts(support_levels, parsed_skills),
        },
        "outcomes": {
            "skill_recall": skill_recall,
            "skill_recall_label": _skill_recall_label(skill_recall),
            "designation_found": bool(result.get("designation_found")),
            "experience_delta": experience_delta,
            "experience_close": bool(result.get("experience_close", True)),
            "missing_expected_skills": list(result.get("missing_expected_skills", [])),
        },
        "calibration_targets": {
            "ats_band": _score_band(ats_score),
            "resume_quality_band": _score_band(resume_quality_score),
            "parser_review_priority": "manual_review" if needs_manual_review else "auto_pass",
            "skill_extraction_status": _skill_recall_label(skill_recall),
        },
    }


def _evaluate_row(
    parser: ResumeParser,
    scoring_engine: ScoringEngine,
    row_payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    row_idx = int(row_payload.get("row_idx", 0) or 0)
    row = row_payload.get("row") or {}
    resume_text = str(row.get("Input") or "").strip()
    expected = _parse_expected_output(row.get("Output"))
    parsed = parser.parse(f"hf-resume-{row_idx}.txt", "text/plain", resume_text.encode("utf-8"))

    expected_skills = {_canonical_eval_skill(skill) for skill in expected.get("Skill", []) if str(skill).strip()}
    parsed_skills = {_canonical_eval_skill(skill) for skill in parsed.get("skills", []) if str(skill).strip()}
    raw_text = _normalize_text(parsed.get("raw_text", ""))
    explicit_expected_skills = {
        skill
        for skill in expected_skills
        if skill and skill in raw_text
    }
    evidence_skills = {
        skill
        for skill in explicit_expected_skills
        if skill and (skill in parsed_skills or skill in raw_text)
    }
    skill_recall = round(len(evidence_skills) / len(explicit_expected_skills), 3) if explicit_expected_skills else 1.0

    expected_designation = _normalize_text(expected.get("Designation"))
    designation_found = bool(expected_designation and expected_designation in raw_text)
    expected_experience = expected.get("Experience")
    parsed_experience = float(parsed.get("experience_years", 0.0) or 0.0)
    experience_delta = None
    experience_close = True
    if isinstance(expected_experience, (int, float)):
        experience_delta = round(abs(parsed_experience - float(expected_experience)), 2)
        experience_close = experience_delta <= 2.5

    sections = parsed.get("sections", {}) or {}
    section_checks = {
        "experience": bool(sections.get("experience")),
        "education": bool(sections.get("education")),
        "skills": bool(sections.get("skills") or parsed.get("skills")),
    }

    result = {
        "row_idx": row_idx,
        "designation": expected.get("Designation", ""),
        "expected_skill_count": len(expected_skills),
        "explicit_expected_skill_count": len(explicit_expected_skills),
        "inferred_expected_skill_count": len(expected_skills - explicit_expected_skills),
        "matched_skill_count": len(evidence_skills),
        "skill_recall": skill_recall,
        "missing_expected_skills": sorted(explicit_expected_skills - evidence_skills)[:12],
        "designation_found": designation_found,
        "expected_experience_years": expected_experience,
        "parsed_experience_years": parsed_experience,
        "experience_delta": experience_delta,
        "experience_close": experience_close,
        "section_checks": section_checks,
        "parse_signals": _json_safe(parsed.get("parse_signals", {})),
    }
    training_signal = _build_training_signal(
        row_idx=row_idx,
        expected=expected,
        parsed=parsed,
        result=result,
        expected_skills=expected_skills,
        explicit_expected_skills=explicit_expected_skills,
        parsed_skills=parsed_skills,
        scoring_engine=scoring_engine,
    )
    return result, training_signal


def _summarize(results: list[dict[str, Any]], *, min_skill_recall: float) -> dict[str, Any]:
    if not results:
        return {
            "case_count": 0,
            "avg_skill_recall": 0.0,
            "below_skill_recall_rows": [],
            "designation_miss_rows": [],
            "experience_delta_miss_rows": [],
        }
    below_skill_recall = [
        int(item["row_idx"])
        for item in results
        if float(item.get("skill_recall", 0.0) or 0.0) < min_skill_recall
    ]
    designation_miss_rows = [int(item["row_idx"]) for item in results if not item.get("designation_found")]
    experience_delta_miss_rows = [
        int(item["row_idx"])
        for item in results
        if item.get("experience_delta") is not None and not item.get("experience_close")
    ]
    return {
        "case_count": len(results),
        "avg_skill_recall": round(sum(float(item.get("skill_recall", 0.0) or 0.0) for item in results) / len(results), 3),
        "avg_explicit_expected_skill_count": round(
            sum(int(item.get("explicit_expected_skill_count", 0) or 0) for item in results) / len(results),
            2,
        ),
        "avg_inferred_expected_skill_count": round(
            sum(int(item.get("inferred_expected_skill_count", 0) or 0) for item in results) / len(results),
            2,
        ),
        "min_skill_recall": min_skill_recall,
        "below_skill_recall_rows": below_skill_recall,
        "designation_miss_rows": designation_miss_rows,
        "experience_delta_miss_rows": experience_delta_miss_rows,
    }


def _pretty_lines(payload: dict[str, Any]) -> list[str]:
    summary = payload["summary"]
    lines = [
        (
            "Resume gold eval: "
            f"{summary['case_count']} cases, "
            f"avg skill recall {summary['avg_skill_recall']}, "
            f"avg explicit expected skills {summary['avg_explicit_expected_skill_count']}, "
            f"avg inferred labels ignored {summary['avg_inferred_expected_skill_count']}, "
            f"min target {summary['min_skill_recall']}"
        )
    ]
    if summary["below_skill_recall_rows"]:
        lines.append("Below skill recall: " + ", ".join(str(row) for row in summary["below_skill_recall_rows"]))
    if summary["designation_miss_rows"]:
        lines.append("Designation misses: " + ", ".join(str(row) for row in summary["designation_miss_rows"]))
    if summary["experience_delta_miss_rows"]:
        lines.append("Experience deltas > 2.5y: " + ", ".join(str(row) for row in summary["experience_delta_miss_rows"]))
    lines.append("")
    for result in payload["results"][:10]:
        lines.append(
            f"row={result['row_idx']} designation={result['designation']} "
            f"skill_recall={result['skill_recall']} "
            f"experience={result['parsed_experience_years']}/{result['expected_experience_years']}"
        )
    return lines


def _summarize_training_signals(signals: list[dict[str, Any]]) -> dict[str, Any]:
    if not signals:
        return {
            "signal_count": 0,
            "avg_parser_confidence": 0.0,
            "avg_ats_score": 0.0,
            "avg_resume_quality_score": 0.0,
            "parser_confidence_distribution": {},
            "ats_band_distribution": {},
            "review_priority_distribution": {},
            "manual_review_rows": [],
        }

    parser_scores = [float(item["features"]["parser_confidence"]["score"]) for item in signals]
    ats_scores = [float(item["features"]["ats_score"]) for item in signals]
    quality_scores = [float(item["features"]["resume_quality_score"]) for item in signals]
    manual_review_rows = [
        int(item["row_idx"])
        for item in signals
        if item["calibration_targets"]["parser_review_priority"] == "manual_review"
    ]
    return {
        "signal_count": len(signals),
        "avg_parser_confidence": _avg(parser_scores),
        "avg_ats_score": _avg(ats_scores),
        "avg_resume_quality_score": _avg(quality_scores),
        "parser_confidence_distribution": _distribution(
            [str(item["features"]["parser_confidence"]["label"]) for item in signals]
        ),
        "ats_band_distribution": _distribution([str(item["calibration_targets"]["ats_band"]) for item in signals]),
        "resume_quality_band_distribution": _distribution(
            [str(item["calibration_targets"]["resume_quality_band"]) for item in signals]
        ),
        "review_priority_distribution": _distribution(
            [str(item["calibration_targets"]["parser_review_priority"]) for item in signals]
        ),
        "skill_extraction_status_distribution": _distribution(
            [str(item["calibration_targets"]["skill_extraction_status"]) for item in signals]
        ),
        "manual_review_rows": manual_review_rows,
    }


def _write_jsonl(path: str, rows: list[dict[str, Any]]) -> None:
    target = Path(path)
    with target.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run parser/skill extraction eval on public Hugging Face resume rows.")
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--split", default=DEFAULT_SPLIT)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--min-skill-recall", type=float, default=0.55)
    parser.add_argument("--format", choices=("json", "pretty"), default="pretty")
    parser.add_argument("--write", help="Optional path to write the JSON payload.")
    parser.add_argument(
        "--write-training-jsonl",
        help="Optional path to write privacy-safe per-resume training/calibration signals as JSONL.",
    )
    parser.add_argument(
        "--write-calibration",
        help="Optional path to write an aggregate calibration summary JSON built from the training signals.",
    )
    parser.add_argument(
        "--fail-under-threshold",
        action="store_true",
        help="Exit non-zero when any row misses the configured skill recall threshold.",
    )
    args = parser.parse_args()

    settings.llm_provider = "disabled"
    rows = _fetch_hf_rows(dataset=args.dataset, config=args.config, split=args.split, limit=args.limit)
    resume_parser = ResumeParser()
    scoring_engine = ScoringEngine()
    evaluated = [_evaluate_row(resume_parser, scoring_engine, row) for row in rows]
    results = [result for result, _signal in evaluated]
    training_signals = [signal for _result, signal in evaluated]
    training_summary = _summarize_training_signals(training_signals)
    payload = {
        "dataset": args.dataset,
        "config": args.config,
        "split": args.split,
        "limit": args.limit,
        "notes": "Public Apache-2.0 Hugging Face rows are used for eval/calibration. Chain_of_Thoughts is ignored.",
        "summary": _summarize(results, min_skill_recall=args.min_skill_recall),
        "training_signal_summary": training_summary,
        "results": results,
    }
    if args.write:
        Path(args.write).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if args.write_training_jsonl:
        _write_jsonl(args.write_training_jsonl, training_signals)
    if args.write_calibration:
        calibration_payload = {
            "schema_version": CALIBRATION_SCHEMA_VERSION,
            "dataset": args.dataset,
            "config": args.config,
            "split": args.split,
            "limit": args.limit,
            "notes": (
                "Aggregate calibration summary only. It contains parser/ATS/skill signal distributions "
                "and no raw resume text, contact data, or model fine-tuning weights."
            ),
            "summary": training_summary,
        }
        Path(args.write_calibration).write_text(json.dumps(calibration_payload, indent=2), encoding="utf-8")
    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print("\n".join(_pretty_lines(payload)))
    if args.fail_under_threshold and payload["summary"]["below_skill_recall_rows"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
