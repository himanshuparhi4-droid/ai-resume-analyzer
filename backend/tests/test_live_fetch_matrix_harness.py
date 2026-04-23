from __future__ import annotations

import unittest

from app.core.config import settings
from app.services.jobs.aggregator import JobAggregator
from app.services.nlp.embeddings import EmbeddingService
from evals.run_live_fetch_matrix import (
    PRESET_ROLE_MATRICES,
    _aggregate_provider_attempts,
    _normalize_provider_rollup_shape,
    _summarize_results,
)


class LiveFetchMatrixHarnessTest(unittest.TestCase):
    def test_universal_preset_covers_canonical_and_alias_roles(self) -> None:
        universal = PRESET_ROLE_MATRICES["universal"]
        self.assertIn("Software Engineer", universal)
        self.assertIn("Data Scientist", universal)
        self.assertIn("DataScientist", universal)
        self.assertIn("SOC Analyst", universal)

    def test_provider_attempt_rollup_tracks_latency_timeouts_and_budget_skips(self) -> None:
        diagnostics = {
            "providers": [
                {
                    "source": "remotive",
                    "status": "success",
                    "result_count": 14,
                    "query": "data scientist",
                    "stage": "primary",
                    "elapsed_ms": 1200.0,
                },
                {
                    "source": "remotive",
                    "status": "timeout",
                    "error": "TimeoutError",
                    "query": "ml engineer",
                    "stage": "primary",
                    "elapsed_ms": 8500.0,
                },
                {
                    "source": "greenhouse",
                    "status": "skipped_budget",
                    "error": "skipped_insufficient_runtime_budget",
                    "query": "data scientist",
                    "stage": "supplemental",
                    "elapsed_ms": 0.0,
                },
            ]
        }

        rollup = _aggregate_provider_attempts(diagnostics)

        self.assertEqual(rollup["remotive"]["attempts"], 2)
        self.assertEqual(rollup["remotive"]["successes"], 1)
        self.assertEqual(rollup["remotive"]["timeouts"], 1)
        self.assertEqual(rollup["remotive"]["raw_returned"], 14)
        self.assertEqual(rollup["remotive"]["avg_elapsed_ms"], 4850.0)
        self.assertEqual(rollup["remotive"]["max_elapsed_ms"], 8500.0)
        self.assertEqual(rollup["greenhouse"]["skipped_budget"], 1)

    def test_summary_marks_underfilled_timeout_and_selector_pruning_queries(self) -> None:
        results = [
            {
                "query": "Data Scientist",
                "elapsed_ms": 1000.0,
                "final_live_count": 3,
                "underfill": {"reason": "provider_timeout_or_upstream_scarcity", "timeout_sources": ["remotive"]},
                "provider_attempts": {"remotive": {"attempts": 2, "successes": 1, "skipped_budget": 0, "timeouts": 1, "errors": 1, "raw_returned": 12}},
            },
            {
                "query": "Web Developer",
                "elapsed_ms": 900.0,
                "final_live_count": 4,
                "underfill": {"reason": "selector_over_pruning", "timeout_sources": []},
                "provider_attempts": {"jobicy": {"attempts": 1, "successes": 1, "skipped_budget": 0, "timeouts": 0, "errors": 0, "raw_returned": 7}},
            },
        ]

        summary = _summarize_results(results)

        self.assertEqual(summary["query_count"], 2)
        self.assertIn("Data Scientist", summary["underfilled_queries"])
        self.assertIn("Data Scientist", summary["timeout_queries"])
        self.assertIn("Web Developer", summary["selector_over_pruning_queries"])
        self.assertEqual(summary["provider_rollup"]["remotive"]["timeouts"], 1)
        self.assertEqual(summary["provider_rollup"]["jobicy"]["raw_returned"], 7)

    def test_provider_rollup_shape_normalizer_accepts_backend_request_keys(self) -> None:
        normalized = _normalize_provider_rollup_shape(
            {
                "remotive": {
                    "requests": 3,
                    "successes": 2,
                    "raw_returned": 18,
                    "timeouts": 1,
                    "errors": 1,
                    "avg_elapsed_ms": 1234.5,
                }
            }
        )

        self.assertEqual(normalized["remotive"]["attempts"], 3)
        self.assertEqual(normalized["remotive"]["successes"], 2)
        self.assertEqual(normalized["remotive"]["raw_returned"], 18)


class AggregatorProviderDiagnosticsTest(unittest.TestCase):
    def test_aggregator_rollup_keeps_provider_attempt_metrics(self) -> None:
        aggregator = JobAggregator(None)
        aggregator.last_fetch_diagnostics = {
            "providers": [
                {
                    "source": "adzuna",
                    "status": "success",
                    "result_count": 5,
                    "query": "soc analyst",
                    "stage": "supplemental",
                    "elapsed_ms": 1800.0,
                },
                {
                    "source": "adzuna",
                    "status": "error",
                    "error": "ConnectionError",
                    "query": "cybersecurity engineer",
                    "stage": "supplemental",
                    "elapsed_ms": 2400.0,
                },
            ]
        }

        rollup = aggregator._aggregate_provider_attempt_rollup()

        self.assertEqual(rollup["adzuna"]["requests"], 2)
        self.assertEqual(rollup["adzuna"]["successes"], 1)
        self.assertEqual(rollup["adzuna"]["errors"], 1)
        self.assertEqual(rollup["adzuna"]["raw_returned"], 5)
        self.assertEqual(rollup["adzuna"]["avg_elapsed_ms"], 2100.0)
        self.assertEqual(rollup["adzuna"]["max_elapsed_ms"], 2400.0)


class ProductionScoringLatencyGuardTest(unittest.TestCase):
    def test_production_disables_sentence_transformer_loading_by_default(self) -> None:
        previous_environment = settings.environment
        previous_enable_embeddings = settings.enable_embeddings
        previous_enable_production_embeddings = settings.enable_production_embeddings
        settings.environment = "production"
        settings.enable_embeddings = True
        settings.enable_production_embeddings = False
        try:
            service = EmbeddingService()
        finally:
            settings.environment = previous_environment
            settings.enable_embeddings = previous_enable_embeddings
            settings.enable_production_embeddings = previous_enable_production_embeddings

        self.assertFalse(service._enabled)


if __name__ == "__main__":
    unittest.main()
