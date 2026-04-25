from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import settings
from app.services.jobs.aggregator import JobAggregator
from app.services.jobs.taxonomy import normalize_role, role_domain, role_family

CORE_ROLE_MATRIX = [
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

UNIVERSAL_CANONICAL_ROLE_MATRIX = [
    "Software Engineer",
    "Frontend Developer",
    "Full Stack Developer",
    "Mobile Developer",
    "Embedded Engineer",
    "Data Analyst",
    "Data Scientist",
    "Machine Learning Engineer",
    "Data Engineer",
    "Database Engineer",
    "DevOps Engineer",
    "Cybersecurity Engineer",
    "QA Engineer",
    "Product Manager",
    "UI/UX Designer",
    "Support Engineer",
    "Solutions Architect",
    "Enterprise Applications Engineer",
    "Technical Writer",
    "Engineering Leadership",
]

UNIVERSAL_ALIAS_ROLE_MATRIX = [
    "Web Developer",
    "Cloud Engineer",
    "Site Reliability Engineer (SRE)",
    "SOC Analyst",
    "Security Analyst",
    "Technical Support Engineer",
    "Salesforce Admin",
    "FrontEndDeveloper",
    "FullStackDeveloper",
    "DataScientist",
]

BROAD_50_ROLE_MATRIX = [
    "Software Engineer",
    "Backend Developer",
    "Frontend Developer",
    "Full Stack Developer",
    "Web Developer",
    "Mobile Developer",
    "Android Developer",
    "iOS Developer",
    "React Native Developer",
    "DevOps Engineer",
    "Cloud Engineer",
    "Site Reliability Engineer",
    "QA Engineer",
    "SDET",
    "Data Analyst",
    "Business Analyst",
    "Data Scientist",
    "Machine Learning Engineer",
    "AI Engineer",
    "Data Engineer",
    "Database Engineer",
    "SQL Developer",
    "Cybersecurity",
    "SOC Analyst",
    "Security Analyst",
    "Product Manager",
    "Project Manager",
    "Program Manager",
    "Scrum Master",
    "UI/UX Designer",
    "Product Designer",
    "Technical Writer",
    "Content Writer",
    "Marketing Manager",
    "Digital Marketing Specialist",
    "SEO Specialist",
    "Social Media Specialist",
    "Account Executive",
    "Sales Development Representative",
    "Business Development Manager",
    "Customer Success Manager",
    "Customer Support Specialist",
    "Technical Support Engineer",
    "Recruiter",
    "Talent Acquisition Specialist",
    "HR Manager",
    "HR Generalist",
    "Financial Analyst",
    "Accountant",
    "Operations Manager",
    "Operations Analyst",
    "Business Operations Analyst",
    "Supply Chain Analyst",
    "Solutions Architect",
    "Salesforce Admin",
    "Enterprise Applications Engineer",
]


def _unique(items: list[str]) -> list[str]:
    return list(dict.fromkeys(item for item in items if item))


PRESET_ROLE_MATRICES = {
    "core": CORE_ROLE_MATRIX,
    "canonical": UNIVERSAL_CANONICAL_ROLE_MATRIX,
    "aliases": UNIVERSAL_ALIAS_ROLE_MATRIX,
    "universal": _unique(UNIVERSAL_CANONICAL_ROLE_MATRIX + UNIVERSAL_ALIAS_ROLE_MATRIX),
    "broad50": BROAD_50_ROLE_MATRIX,
}
PRESET_ROLE_MATRICES["all"] = _unique(CORE_ROLE_MATRIX + PRESET_ROLE_MATRICES["universal"] + BROAD_50_ROLE_MATRIX)


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


def _aggregate_provider_attempts(diagnostics: dict[str, Any]) -> dict[str, dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}
    for entry in diagnostics.get("providers", []) or []:
        source = str(entry.get("source") or entry.get("provider") or "unknown").lower()
        bucket = aggregated.setdefault(
            source,
            {
                "attempts": 0,
                "successes": 0,
                "skipped_budget": 0,
                "timeouts": 0,
                "errors": 0,
                "raw_returned": 0,
                "queries": [],
                "stages": [],
                "error_samples": [],
                "_elapsed_total_ms": 0.0,
                "_elapsed_count": 0,
                "max_elapsed_ms": 0.0,
            },
        )
        bucket["attempts"] += 1
        status = str(entry.get("status") or "").strip().lower()
        if status == "success":
            bucket["successes"] += 1
        elif status == "skipped_budget":
            bucket["skipped_budget"] += 1
        result_count = int(entry.get("result_count", 0) or 0)
        bucket["raw_returned"] += result_count
        query = str(entry.get("query", "")).strip()
        if query and query not in bucket["queries"]:
            bucket["queries"].append(query)
        stage = str(entry.get("stage", "")).strip()
        if stage and stage not in bucket["stages"]:
            bucket["stages"].append(stage)
        elapsed_ms = float(entry.get("elapsed_ms", 0.0) or 0.0)
        if elapsed_ms > 0:
            bucket["_elapsed_total_ms"] += elapsed_ms
            bucket["_elapsed_count"] += 1
            bucket["max_elapsed_ms"] = max(float(bucket["max_elapsed_ms"]), elapsed_ms)
        error_text = str(entry.get("error", "") or "").strip()
        if error_text:
            bucket["errors"] += 1
            if "timeout" in error_text.lower():
                bucket["timeouts"] += 1
            if error_text not in bucket["error_samples"]:
                bucket["error_samples"].append(error_text)

    rolled_up: dict[str, dict[str, Any]] = {}
    for source, payload in sorted(aggregated.items()):
        elapsed_count = int(payload.pop("_elapsed_count", 0) or 0)
        elapsed_total_ms = float(payload.pop("_elapsed_total_ms", 0.0) or 0.0)
        payload["avg_elapsed_ms"] = round(elapsed_total_ms / elapsed_count, 2) if elapsed_count else 0.0
        payload["max_elapsed_ms"] = round(float(payload.get("max_elapsed_ms", 0.0) or 0.0), 2)
        payload["queries"] = payload["queries"][:8]
        payload["stages"] = payload["stages"][:4]
        payload["error_samples"] = payload["error_samples"][:3]
        rolled_up[source] = payload
    return rolled_up


def _normalize_provider_rollup_shape(rollup: dict[str, dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for source, payload in (rollup or {}).items():
        normalized_payload = dict(payload)
        normalized_payload["attempts"] = int(
            normalized_payload.get("attempts", normalized_payload.get("requests", 0)) or 0
        )
        normalized_payload["successes"] = int(normalized_payload.get("successes", 0) or 0)
        normalized_payload["skipped_budget"] = int(normalized_payload.get("skipped_budget", 0) or 0)
        normalized_payload["timeouts"] = int(normalized_payload.get("timeouts", 0) or 0)
        normalized_payload["errors"] = int(normalized_payload.get("errors", 0) or 0)
        normalized_payload["raw_returned"] = int(normalized_payload.get("raw_returned", 0) or 0)
        normalized_payload["avg_elapsed_ms"] = float(normalized_payload.get("avg_elapsed_ms", 0.0) or 0.0)
        normalized_payload["max_elapsed_ms"] = float(normalized_payload.get("max_elapsed_ms", 0.0) or 0.0)
        normalized[source] = normalized_payload
    return normalized


def _summarize_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    provider_rollup: dict[str, dict[str, int]] = defaultdict(
        lambda: {"attempts": 0, "successes": 0, "skipped_budget": 0, "timeouts": 0, "errors": 0, "raw_returned": 0}
    )
    underfilled_queries: list[str] = []
    below_min_live_queries: list[str] = []
    timeout_queries: list[str] = []
    selector_over_pruning_queries: list[str] = []
    for result in results:
        underfill = result.get("underfill") or {}
        reason = str(underfill.get("reason") or "")
        query = str(result.get("query") or "")
        min_live = int(result.get("min_live", 0) or 0)
        final_live_count = int(result.get("final_live_count", 0) or 0)
        if min_live and final_live_count < min_live:
            below_min_live_queries.append(query)
        if reason and reason != "sufficient_live_supply":
            underfilled_queries.append(query)
        if (underfill.get("timeout_sources") or []):
            timeout_queries.append(query)
        if reason == "selector_over_pruning":
            selector_over_pruning_queries.append(query)
        for source, payload in (result.get("provider_attempts") or {}).items():
            provider_rollup[source]["attempts"] += int(payload.get("attempts", 0) or 0)
            provider_rollup[source]["successes"] += int(payload.get("successes", 0) or 0)
            provider_rollup[source]["skipped_budget"] += int(payload.get("skipped_budget", 0) or 0)
            provider_rollup[source]["timeouts"] += int(payload.get("timeouts", 0) or 0)
            provider_rollup[source]["errors"] += int(payload.get("errors", 0) or 0)
            provider_rollup[source]["raw_returned"] += int(payload.get("raw_returned", 0) or 0)

    elapsed_values = [float(result.get("elapsed_ms", 0.0) or 0.0) for result in results]
    final_live_values = [int(result.get("final_live_count", 0) or 0) for result in results]
    return {
        "query_count": len(results),
        "avg_elapsed_ms": round(sum(elapsed_values) / len(elapsed_values), 2) if elapsed_values else 0.0,
        "avg_final_live_count": round(sum(final_live_values) / len(final_live_values), 2) if final_live_values else 0.0,
        "below_min_live_queries": below_min_live_queries,
        "underfilled_queries": underfilled_queries,
        "timeout_queries": timeout_queries,
        "selector_over_pruning_queries": selector_over_pruning_queries,
        "provider_rollup": {source: payload for source, payload in sorted(provider_rollup.items())},
    }


def _pretty_lines(results: list[dict[str, Any]], summary: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    lines.append(
        "Live fetch matrix summary: "
        f"{summary.get('query_count', 0)} queries, "
        f"avg live {summary.get('avg_final_live_count', 0)}, "
        f"avg elapsed {summary.get('avg_elapsed_ms', 0)}ms"
    )
    if summary.get("underfilled_queries"):
        lines.append("Underfilled: " + ", ".join(summary["underfilled_queries"]))
    if summary.get("below_min_live_queries"):
        lines.append("Below min live: " + ", ".join(summary["below_min_live_queries"]))
    if summary.get("timeout_queries"):
        lines.append("Timeouts: " + ", ".join(summary["timeout_queries"]))
    lines.append("")

    for result in results:
        underfill = result.get("underfill") or {}
        timeout_sources = underfill.get("timeout_sources") or []
        plan = result.get("provider_plan") or {}
        lines.append(
            f"{result['query']} -> live={result['final_live_count']}/{result.get('min_live', 0) or '-'} "
            f"elapsed={result['elapsed_ms']}ms normalized={result['normalized_role']}"
        )
        lines.append(
            "  plan: "
            f"primary={','.join(plan.get('primary_sources', [])) or '-'} "
            f"supplemental={','.join(plan.get('supplemental_sources', [])) or '-'} "
            f"fallback={','.join(plan.get('fallback_sources', [])) or '-'}"
        )
        lines.append(
            "  underfill: "
            f"reason={underfill.get('reason', 'none')} "
            f"upstream={underfill.get('upstream_family_safe_count', 0)} "
            f"required={underfill.get('required_live_floor', 0)} "
            f"timeouts={','.join(timeout_sources) or '-'}"
        )
        stage_bits = []
        for stage in result.get("stage_results") or []:
            stage_bits.append(
                f"{stage.get('stage')}[selected={stage.get('selected_live', 0)} "
                f"upstream={stage.get('upstream_family_safe_count', 0)} "
                f"reason={stage.get('underfill_reason', 'none')}]"
            )
        if stage_bits:
            lines.append("  stages: " + "; ".join(stage_bits))
        provider_bits: list[str] = []
        for source, payload in (result.get("provider_attempts") or {}).items():
            queries = "/".join((payload.get("queries") or [])[:2]) or "-"
            provider_bits.append(
                f"{source}[a={payload.get('attempts', 0)} ok={payload.get('successes', 0)} "
                f"raw={payload.get('raw_returned', 0)} t={payload.get('timeouts', 0)} "
                f"skip={payload.get('skipped_budget', 0)} avg={payload.get('avg_elapsed_ms', 0)}ms "
                f"q={queries}]"
            )
        if provider_bits:
            lines.append("  providers: " + "; ".join(provider_bits))
        titles = result.get("top_selected_titles") or []
        if titles:
            lines.append("  titles: " + " | ".join(titles[:6]))
        lines.append("")

    if summary.get("provider_rollup"):
        lines.append("Provider rollup:")
        for source, payload in summary["provider_rollup"].items():
            lines.append(
                f"  {source}: attempts={payload['attempts']} successes={payload['successes']} "
                f"raw={payload['raw_returned']} timeouts={payload['timeouts']} "
                f"skipped_budget={payload['skipped_budget']} errors={payload['errors']}"
            )
    return lines


async def run_case(*, query: str, location: str, limit: int, min_live: int) -> dict[str, Any]:
    aggregator = JobAggregator(None)
    started = time.perf_counter()
    jobs = await aggregator.fetch_jobs(query=query, location=location, limit=limit)
    elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
    diagnostics = _json_safe(getattr(aggregator, "last_fetch_diagnostics", {}) or {})
    selection_debug = (diagnostics.get("selection_debug") or {})
    provider_attempt_rollup = _normalize_provider_rollup_shape(
        diagnostics.get("provider_attempt_rollup") or _aggregate_provider_attempts(diagnostics)
    )

    return {
        "query": query,
        "normalized_role": normalize_role(query),
        "family_role": role_family(query),
        "domain": role_domain(query),
        "elapsed_ms": elapsed_ms,
        "min_live": min_live,
        "provider_plan": diagnostics.get("provider_plan", {}),
        "provider_availability": diagnostics.get("provider_availability", []),
        "provider_attempts": provider_attempt_rollup,
        "provider_request_summary": diagnostics.get("provider_request_summary", {}),
        "provider_match_counts": diagnostics.get("provider_match_counts", {}),
        "stage_short_circuits": diagnostics.get("stage_short_circuits", []),
        "stage_results": diagnostics.get("stage_results", []),
        "underfill": diagnostics.get("underfill", {}),
        "selected_live_sources": diagnostics.get("selected_live_sources", {}),
        "final_live_count": len([job for job in jobs if job.get("source") != "role-baseline"]),
        "top_selected_titles": [str(job.get("title", "")) for job in jobs[:10] if job.get("source") != "role-baseline"],
        "selection_debug_snapshot": {
            "precision_guarded_candidates": int(selection_debug.get("precision_guarded_candidates", 0) or 0),
            "exact_backup_candidates": int(selection_debug.get("exact_backup_candidates", 0) or 0),
            "same_family_recovery_candidates": int(selection_debug.get("same_family_recovery_candidates", 0) or 0),
            "upstream_family_safe_count": int(selection_debug.get("upstream_family_safe_count", 0) or 0),
            "selected_count": int(selection_debug.get("selected_count", 0) or 0),
        },
        "selection_rejections": (selection_debug.get("rejections") or {}),
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description="Run the production live-fetch matrix against representative role families.")
    parser.add_argument(
        "--preset",
        choices=sorted(PRESET_ROLE_MATRICES.keys()),
        default="core",
        help="Named role matrix to run. Use 'universal' for the full canonical+alias sweep.",
    )
    parser.add_argument("--location", default="Global", help="Location to send with the fetch requests.")
    parser.add_argument("--limit", type=int, default=10, help="Requested live listing cap per query.")
    parser.add_argument("--min-live", type=int, default=0, help="Optional minimum acceptable live jobs per query.")
    parser.add_argument(
        "--fail-under-min-live",
        action="store_true",
        help="Exit non-zero when any query returns fewer than --min-live live jobs.",
    )
    parser.add_argument("--query", action="append", dest="queries", help="Optional role query to run. Repeat for multiple queries.")
    parser.add_argument(
        "--format",
        choices=("json", "pretty"),
        default="json",
        help="Output format. 'pretty' is compact for shell/log triage.",
    )
    parser.add_argument("--write", help="Optional path to write the JSON payload.")
    args = parser.parse_args()

    settings.environment = "production"
    settings.llm_provider = "disabled"

    queries = args.queries or PRESET_ROLE_MATRICES[args.preset]
    results = [await run_case(query=query, location=args.location, limit=args.limit, min_live=args.min_live) for query in queries]
    summary = _summarize_results(results)
    payload = {
        "preset": args.preset,
        "location": args.location,
        "limit": args.limit,
        "results": results,
        "summary": summary,
    }
    if args.write:
        Path(args.write).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if args.format == "pretty":
        print("\n".join(_pretty_lines(results, summary)))
    else:
        print(json.dumps(payload, indent=2))
    if args.fail_under_min_live and summary.get("below_min_live_queries"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
