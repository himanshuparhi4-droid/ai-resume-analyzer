from __future__ import annotations

import unittest

from app.core.config import settings
from app.services.jobs.aggregator import JobAggregator
from app.services.jobs.greenhouse import GreenhouseProvider
from app.services.jobs.taxonomy import provider_query_variations


class AggregatorPrecisionGuardTest(unittest.TestCase):
    def setUp(self) -> None:
        self.aggregator = JobAggregator(None)

    def test_data_analyst_rejects_adjacent_business_analyst_title(self) -> None:
        item = {
            "title": "Senior Business Analyst",
            "description": "Own dashboards, SQL reporting, and analytics for commercial teams.",
            "tags": [],
            "normalized_data": {"skills": ["sql", "excel", "analytics"]},
        }
        self.assertFalse(self.aggregator._passes_precise_query_guard("Data Analyst", item))

    def test_data_scientist_rejects_data_engineer_title(self) -> None:
        item = {
            "title": "Tech Lead Databricks Data Engineer",
            "description": "Build ETL pipelines, Spark jobs, and warehouse automation.",
            "tags": [],
            "normalized_data": {"skills": ["spark", "etl", "sql", "databricks"]},
        }
        self.assertFalse(self.aggregator._passes_precise_query_guard("Data Scientist", item))
        self.assertFalse(self.aggregator._passes_final_live_guard("Data Scientist", item))

    def test_compact_data_scientist_query_keeps_compact_title(self) -> None:
        item = {
            "title": "Datascientist",
            "description": "Build machine learning models, Python pipelines, and statistical analysis workflows.",
            "tags": [],
            "normalized_data": {
                "skills": ["python", "machine learning", "statistics"],
                "role_fit_score": 5.0,
                "market_quality_score": 22.0,
                "title_alignment_score": 14.0,
            },
        }
        self.assertTrue(self.aggregator._passes_precise_query_guard("Datascientist", item))
        self.assertTrue(self.aggregator._passes_precise_query_guard("Data Scientist", item))
        self.assertTrue(self.aggregator._passes_final_live_guard("Datascientist", item))

    def test_data_analyst_keeps_data_operations_analyst_title(self) -> None:
        item = {
            "title": "Data Operations Analyst",
            "description": "Own SQL dashboards, reporting workflows, and analytics operations.",
            "tags": [],
            "normalized_data": {"skills": ["sql", "reporting", "analytics", "power bi"]},
        }
        self.assertTrue(self.aggregator._passes_precise_query_guard("Data Analyst", item))

    def test_data_analyst_keeps_insights_analyst_title(self) -> None:
        item = {
            "title": "Insights Analyst",
            "description": "Own reporting workflows, dashboard analysis, and business intelligence insights.",
            "tags": [],
            "normalized_data": {"skills": ["sql", "reporting", "analytics", "power bi"]},
        }
        self.assertTrue(self.aggregator._passes_precise_query_guard("Data Analyst", item))

    def test_data_analyst_same_family_recovery_keeps_operations_analyst_with_strong_data_signals(self) -> None:
        item = {
            "title": "Operations Analyst",
            "description": "Own SQL reporting, dashboards, KPI analysis, and business intelligence workflows.",
            "tags": [],
            "normalized_data": {
                "skills": ["sql", "excel", "dashboarding", "business intelligence"],
                "role_fit_score": 5.0,
                "market_quality_score": 22.0,
                "title_alignment_score": 8.0,
            },
        }
        self.assertTrue(self.aggregator._passes_exact_query_backup_guard("Data Analyst", "Global", item))
        self.assertTrue(self.aggregator._passes_same_family_recovery_guard("Data Analyst", "Global", item))
        self.assertTrue(self.aggregator._passes_final_live_guard("Data Analyst", item))

    def test_greenhouse_index_fallback_builds_usable_data_analyst_signal(self) -> None:
        provider = GreenhouseProvider()
        fallback = {
            "source": "greenhouse",
            "external_id": "123",
            "title": "Data Operations Analyst",
            "company": "Example ATS",
            "location": "Global",
            "remote": True,
            "url": "https://boards.greenhouse.io/example/jobs/123",
            "description": "Data Operations Analyst role at Example ATS. Greenhouse ATS listing.",
            "preview": "Data Operations Analyst role at Example ATS.",
            "tags": ["Example ATS", "data analyst"],
            "normalized_data": {"board_token": "example", "index_only": True, "skills": []},
        }

        job = provider._index_fallback_job(fallback, detail_hydration_skipped=True)

        self.assertIsNotNone(job)
        self.assertTrue(job["normalized_data"]["detail_hydration_skipped"])
        self.assertFalse(job["normalized_data"]["detail_fetch_failed"])
        self.assertGreater(len(job["normalized_data"]["skills"]), 0)
        self.assertTrue(self.aggregator._passes_final_live_guard("Data Analyst", job))

    def test_greenhouse_alternate_boards_keep_untried_role_family_boards_available(self) -> None:
        provider = GreenhouseProvider()
        fallback_boards = provider._fallback_boards_for_query(
            "Data Analyst",
            tried={"yipitdata", "instacart", "affirm", "robinhood"},
            limit=2,
        )

        self.assertEqual(fallback_boards, ["asana", "discord"])

    def test_contextual_recovery_guard_rejects_adjacent_business_analyst_noise(self) -> None:
        item = {
            "title": "Senior Business Analyst",
            "description": "Own dashboards, SQL reporting, and analytics for commercial teams.",
            "tags": [],
            "normalized_data": {
                "skills": ["sql", "excel", "analytics"],
                "role_fit_score": 5.5,
                "market_quality_score": 26.0,
                "title_alignment_score": 8.0,
            },
        }
        self.assertFalse(self.aggregator._passes_contextual_family_recovery_guard("Data Analyst", item))

    def test_contextual_recovery_guard_keeps_data_engineering_analyst_with_analyst_signals(self) -> None:
        item = {
            "title": "Data Engineering Analyst Lead/Scientist",
            "description": "Own SQL reporting, dashboarding, data visualization, and business intelligence workflows.",
            "tags": [],
            "normalized_data": {
                "skills": ["sql", "reporting", "dashboarding", "tableau", "data visualization"],
                "role_fit_score": 2.0,
                "market_quality_score": 28.0,
                "title_alignment_score": 8.0,
            },
        }
        self.assertTrue(self.aggregator._passes_contextual_family_recovery_guard("Data Analyst", item))

    def test_data_analyst_rejects_data_entry_operator_live_card(self) -> None:
        item = {
            "title": "Data Entry Operator",
            "description": "Enter records, maintain spreadsheets, and support administrative workflows.",
            "tags": [],
            "normalized_data": {"skills": ["excel", "data entry"]},
        }
        self.assertFalse(self.aggregator._passes_final_live_guard("Data Analyst", item))

    def test_data_analyst_rejects_generic_non_data_analyst_titles_even_with_high_role_fit(self) -> None:
        item = {
            "title": "Actuarial Analyst II (Intermediate) - P&C Personal Lines",
            "description": "Own actuarial modeling, risk reporting, and operations analytics for insurance pricing.",
            "tags": [],
            "normalized_data": {
                "skills": ["sql", "analytics", "reporting"],
                "role_fit_score": 11.0,
                "market_quality_score": 42.0,
                "title_alignment_score": 8.0,
            },
        }
        self.assertFalse(self.aggregator._passes_final_live_guard("Data Analyst", item))

    def test_data_analyst_rejects_credit_business_analyst_noise_even_with_sql_signals(self) -> None:
        item = {
            "title": "Credit Business Analyst, Banking Fraud",
            "description": "Own SQL reporting, dashboarding, fraud analytics, and Excel-based banking insights.",
            "tags": [],
            "normalized_data": {
                "skills": ["sql", "excel", "reporting", "analytics"],
                "role_fit_score": 4.0,
                "market_quality_score": 20.0,
                "title_alignment_score": 8.0,
            },
        }
        self.assertFalse(self.aggregator._passes_contextual_family_recovery_guard("Data Analyst", item))
        self.assertFalse(self.aggregator._passes_final_live_guard("Data Analyst", item))

    def test_exact_query_rejects_manager_title_when_manager_not_requested(self) -> None:
        item = {
            "title": "Data Manager",
            "description": "Lead the reporting team and own analytics delivery planning.",
            "tags": [],
            "normalized_data": {"skills": ["sql", "analytics", "reporting"]},
        }
        self.assertFalse(self.aggregator._passes_final_live_guard("Data Analyst", item))

    def test_cybersecurity_engineer_keeps_application_security_title(self) -> None:
        item = {
            "title": "Application Security Engineer",
            "description": "Own SIEM rules, IAM hardening, and secure development lifecycle work.",
            "tags": [],
            "normalized_data": {"skills": ["iam", "siem", "application security"]},
        }
        self.assertTrue(self.aggregator._passes_precise_query_guard("Cybersecurity Engineer", item))

    def test_soc_analyst_keeps_security_analyst_title(self) -> None:
        item = {
            "title": "Security Analyst",
            "description": "Work in SOC operations, SIEM alerting, incident response, and threat detection.",
            "tags": [],
            "normalized_data": {"skills": ["siem", "splunk", "incident response", "threat detection"]},
        }
        self.assertTrue(self.aggregator._passes_precise_query_guard("SOC Analyst", item))
        self.assertTrue(self.aggregator._passes_final_live_guard("SOC Analyst", item))

    def test_devops_engineer_keeps_site_reliability_engineer_title(self) -> None:
        item = {
            "title": "Site Reliability Engineer",
            "description": "Own Kubernetes reliability, CI/CD, observability, and incident response.",
            "tags": [],
            "normalized_data": {"skills": ["kubernetes", "terraform", "ci/cd", "observability"]},
        }
        self.assertTrue(self.aggregator._passes_precise_query_guard("DevOps Engineer", item))

    def test_web_developer_alias_keeps_frontend_developer_title(self) -> None:
        item = {
            "title": "Frontend Developer",
            "description": "Build React user interfaces and modern web experiences.",
            "tags": [],
            "normalized_data": {"skills": ["react", "typescript", "css"]},
        }
        self.assertTrue(self.aggregator._passes_precise_query_guard("Web Developer", item))

    def test_compact_full_stack_title_keeps_full_stack_query(self) -> None:
        item = {
            "title": "FullstackDeveloper",
            "description": "Build React and Node web applications across the full product stack.",
            "tags": [],
            "normalized_data": {
                "skills": ["react", "node", "typescript"],
                "role_fit_score": 4.0,
                "market_quality_score": 18.0,
                "title_alignment_score": 12.0,
            },
        }
        self.assertTrue(self.aggregator._passes_precise_query_guard("Full Stack Developer", item))
        self.assertTrue(self.aggregator._passes_precise_query_guard("FullstackDeveloper", item))

    def test_web_developer_alias_uses_balanced_precision_not_exact_alias_locking(self) -> None:
        self.assertFalse(self.aggregator._uses_strict_precision_guard("Web Developer"))
        self.assertTrue(self.aggregator._uses_strict_precision_guard("Frontend Developer"))

    def test_frontend_developer_fast_source_queries_include_web_developer(self) -> None:
        self.assertIn("web developer", provider_query_variations("Frontend Developer", "remotive", production=True))
        self.assertIn("web developer", provider_query_variations("Frontend Developer", "jobicy", production=True))

    def test_web_developer_alias_does_not_require_literal_web_specialty_token(self) -> None:
        self.assertFalse(self.aggregator._requires_specialty_guard("Web Developer"))

    def test_soc_analyst_alias_does_not_require_literal_soc_token(self) -> None:
        self.assertFalse(self.aggregator._requires_specialty_guard("SOC Analyst"))

    def test_soc_analyst_provider_plan_prioritizes_analyst_friendly_sources(self) -> None:
        source_groups = {name: [object()] for name in ["remotive", "jobicy", "greenhouse", "themuse", "jooble", "adzuna"]}
        plan = self.aggregator._build_production_provider_plan(
            query="SOC Analyst",
            location="Global",
            source_groups=source_groups,
        )
        self.assertEqual(plan["primary_sources"], ["remotive", "greenhouse", "themuse"])
        self.assertEqual(plan["fallback_sources"], [])

    def test_soc_analyst_global_search_caps_slow_primary_fanout(self) -> None:
        provider = type("Provider", (), {"source_name": "adzuna", "supports_query_variations": True})()
        previous_environment = settings.environment
        settings.environment = "production"
        try:
            queries = self.aggregator._search_queries(provider, "SOC Analyst", "Global")
        finally:
            settings.environment = previous_environment
        self.assertEqual(queries, ["soc analyst"])

    def test_soc_analyst_global_keeps_remotive_multi_query_coverage(self) -> None:
        provider = type("Provider", (), {"source_name": "remotive", "supports_query_variations": True})()
        previous_environment = settings.environment
        settings.environment = "production"
        try:
            queries = self.aggregator._search_queries(provider, "SOC Analyst", "Global")
        finally:
            settings.environment = previous_environment
        self.assertLessEqual(len(queries), 3)
        self.assertIn("soc analyst", queries)

    def test_web_developer_provider_plan_deprioritizes_slow_global_sources(self) -> None:
        source_groups = {name: [object()] for name in ["remotive", "jobicy", "greenhouse", "themuse", "jooble", "adzuna"]}
        plan = self.aggregator._build_production_provider_plan(
            query="Web Developer",
            location="Global",
            source_groups=source_groups,
        )
        self.assertEqual(plan["primary_sources"], ["remotive", "jobicy", "jooble"])
        self.assertEqual(plan["supplemental_sources"], ["greenhouse", "themuse"])

    def test_data_analyst_provider_plan_prioritizes_fast_high_yield_sources(self) -> None:
        source_groups = {name: [object()] for name in ["remotive", "jobicy", "greenhouse", "themuse", "jooble", "adzuna"]}
        plan = self.aggregator._build_production_provider_plan(
            query="Data Analyst",
            location="Global",
            source_groups=source_groups,
        )
        self.assertEqual(plan["primary_sources"], ["jobicy", "adzuna", "greenhouse", "remotive"])
        self.assertEqual(plan["supplemental_sources"], ["jooble", "themuse"])

    def test_business_analyst_reuses_the_fast_analyst_global_provider_plan(self) -> None:
        source_groups = {name: [object()] for name in ["remotive", "jobicy", "greenhouse", "themuse", "jooble", "adzuna"]}
        plan = self.aggregator._build_production_provider_plan(
            query="Business Analyst",
            location="Global",
            source_groups=source_groups,
        )
        self.assertEqual(plan["primary_sources"], ["jobicy", "adzuna", "greenhouse", "remotive"])
        self.assertEqual(plan["supplemental_sources"], ["jooble", "themuse"])

    def test_frontend_developer_global_caps_jooble_to_single_query(self) -> None:
        provider = type("Provider", (), {"source_name": "jooble", "supports_query_variations": True})()
        previous_environment = settings.environment
        settings.environment = "production"
        try:
            queries = self.aggregator._search_queries(provider, "Frontend Developer", "Global")
        finally:
            settings.environment = previous_environment
        self.assertEqual(len(queries), 1)

    def test_web_developer_final_guard_keeps_full_stack_react_title(self) -> None:
        item = {
            "title": "Senior Full-stack React Developer",
            "description": "Build React interfaces, component systems, and modern web experiences.",
            "tags": [],
            "normalized_data": {
                "skills": ["react", "javascript", "typescript", "css"],
                "role_fit_score": 4.2,
                "market_quality_score": 18.0,
                "title_alignment_score": 10.0,
            },
        }
        self.assertTrue(self.aggregator._passes_final_live_guard("Web Developer", item))

    def test_web_developer_family_candidate_keeps_fullstack_adjacent_title(self) -> None:
        item = {
            "title": "Fullstack Developer",
            "description": "Build React and web application features across the frontend stack.",
            "tags": [],
            "normalized_data": {
                "skills": ["react", "javascript", "typescript", "css"],
                "role_fit_score": 2.0,
                "market_quality_score": 18.0,
                "title_alignment_score": 8.0,
            },
        }
        self.assertTrue(self.aggregator._is_family_live_candidate("Web Developer", "Global", item))

    def test_devops_engineer_rejects_generic_cloud_developer_title(self) -> None:
        item = {
            "title": "Sr Applications Developer_Atlassian Cloud",
            "description": "Build integrations and application features for enterprise cloud products.",
            "tags": [],
            "normalized_data": {"skills": ["cloud", "api", "javascript"]},
        }
        self.assertFalse(self.aggregator._passes_precise_query_guard("DevOps Engineer", item))

    def test_cloud_engineer_alias_rejects_cloud_flavored_data_engineer(self) -> None:
        item = {
            "title": "Senior Data Engineer (Cloud & Analytics)",
            "description": "Build data pipelines and analytics systems on cloud infrastructure.",
            "tags": [],
            "normalized_data": {"skills": ["spark", "sql", "cloud"]},
        }
        self.assertFalse(self.aggregator._passes_precise_query_guard("Cloud Engineer", item))

    def test_embedded_engineer_keeps_firmware_developer_title(self) -> None:
        item = {
            "title": "Firmware Developer",
            "description": "Build low-level C/C++ firmware, RTOS workflows, and embedded device communication.",
            "tags": [],
            "normalized_data": {
                "skills": ["c", "c++", "firmware", "rtos", "microcontroller"],
                "role_fit_score": 8.0,
                "market_quality_score": 24.0,
                "title_alignment_score": 16.0,
            },
        }
        self.assertTrue(self.aggregator._passes_precise_query_guard("Embedded Engineer", item))
        self.assertTrue(self.aggregator._passes_final_live_guard("Embedded Engineer", item))

    def test_embedded_engineer_rejects_embedded_data_specialist_title(self) -> None:
        item = {
            "title": "Embedded Data Specialist",
            "description": "Support data cleanup and reporting for internal business teams.",
            "tags": [],
            "normalized_data": {
                "skills": ["excel", "reporting", "data analysis"],
                "role_fit_score": 4.0,
                "market_quality_score": 18.0,
                "title_alignment_score": 6.0,
            },
        }
        self.assertFalse(self.aggregator._passes_final_live_guard("Embedded Engineer", item))

    def test_enterprise_applications_rejects_enterprise_ae_title(self) -> None:
        item = {
            "title": "Enterprise AE",
            "description": "Drive enterprise sales, pipeline generation, and account growth.",
            "tags": [],
            "normalized_data": {
                "skills": ["crm", "sales"],
                "role_fit_score": 6.0,
                "market_quality_score": 18.0,
                "title_alignment_score": 7.0,
            },
        }
        self.assertFalse(self.aggregator._passes_final_live_guard("Enterprise Applications Engineer", item))

    def test_compact_salesforce_admin_query_keeps_compact_title(self) -> None:
        item = {
            "title": "SalesforceAdmin",
            "description": "Own Salesforce administration, CRM workflows, automation, and platform support.",
            "tags": [],
            "normalized_data": {
                "skills": ["salesforce", "crm", "automation"],
                "role_fit_score": 5.0,
                "market_quality_score": 20.0,
                "title_alignment_score": 12.0,
            },
        }
        self.assertTrue(self.aggregator._passes_precise_query_guard("SalesforceAdmin", item))
        self.assertTrue(self.aggregator._passes_final_live_guard("SalesforceAdmin", item))

    def test_full_stack_developer_rejects_full_time_driver_ad(self) -> None:
        item = {
            "title": "Drivers wanted - Great alternative to part-time, full-time and seasonal work",
            "description": "Drive with flexible schedules and weekly payouts.",
            "tags": [],
            "normalized_data": {"skills": []},
        }
        self.assertFalse(self.aggregator._passes_precise_query_guard("Full Stack Developer", item))

    def test_exact_canonical_selection_keeps_precise_pool_without_reintroducing_adjacent_titles(self) -> None:
        jobs = [
            {
                "title": "Data Analyst",
                "company": "Alpha",
                "source": "remotive",
                "description": "Own SQL dashboards, reporting, and analytics workflows.",
                "tags": ["data analyst"],
                "normalized_data": {
                    "skills": ["sql", "excel", "analytics"],
                    "title_alignment_score": 20.0,
                    "role_fit_score": 18.0,
                    "market_quality_score": 25.0,
                },
            },
            {
                "title": "Senior Data Analyst",
                "company": "Beta",
                "source": "jobicy",
                "description": "Analyze product data, build BI reporting, and partner with operations.",
                "tags": ["data analyst"],
                "normalized_data": {
                    "skills": ["sql", "power bi", "analytics"],
                    "title_alignment_score": 19.0,
                    "role_fit_score": 17.0,
                    "market_quality_score": 24.0,
                },
            },
            {
                "title": "Online Data Analyst United States",
                "company": "Gamma",
                "source": "themuse",
                "description": "Evaluate data quality, reporting, and analytical trends.",
                "tags": ["data analyst"],
                "normalized_data": {
                    "skills": ["sql", "excel", "data analysis"],
                    "title_alignment_score": 18.0,
                    "role_fit_score": 16.0,
                    "market_quality_score": 23.0,
                },
            },
            {
                "title": "Senior Business Analyst",
                "company": "Noise One",
                "source": "greenhouse",
                "description": "Lead business reporting and commercial analytics across teams.",
                "tags": [],
                "normalized_data": {
                    "skills": ["sql", "excel", "analytics"],
                    "title_alignment_score": 14.0,
                    "role_fit_score": 11.0,
                    "market_quality_score": 21.0,
                },
            },
            {
                "title": "Analyst II, International Credit Risk",
                "company": "Noise Two",
                "source": "lever",
                "description": "Credit analytics, risk controls, and policy reporting.",
                "tags": [],
                "normalized_data": {
                    "skills": ["sql", "risk", "reporting"],
                    "title_alignment_score": 12.0,
                    "role_fit_score": 9.0,
                    "market_quality_score": 20.0,
                },
            },
        ]

        selected = self.aggregator._select_production_live_jobs(
            query="Data Analyst",
            location="India",
            jobs=jobs,
            limit=8,
        )

        selected_titles = {item["title"] for item in selected}
        self.assertIn("Data Analyst", selected_titles)
        self.assertIn("Senior Data Analyst", selected_titles)
        self.assertIn("Online Data Analyst United States", selected_titles)
        self.assertNotIn("Senior Business Analyst", selected_titles)
        self.assertNotIn("Analyst II, International Credit Risk", selected_titles)

    def test_exact_alias_selection_keeps_only_precise_alias_matches(self) -> None:
        jobs = [
            {
                "title": "Salesforce Administrator (App Builder)",
                "company": "Precise",
                "source": "jobicy",
                "description": "Own Salesforce configuration, automation, and CRM administration.",
                "tags": ["salesforce administrator"],
                "normalized_data": {
                    "skills": ["salesforce", "crm", "apex"],
                    "title_alignment_score": 18.0,
                    "role_fit_score": 17.0,
                    "market_quality_score": 24.0,
                },
            },
            {
                "title": "CRM Implementation Consultant",
                "company": "Noise",
                "source": "greenhouse",
                "description": "Lead CRM process mapping and enterprise rollout planning.",
                "tags": [],
                "normalized_data": {
                    "skills": ["crm", "workflow automation"],
                    "title_alignment_score": 10.0,
                    "role_fit_score": 8.0,
                    "market_quality_score": 20.0,
                },
            },
        ]

        selected = self.aggregator._select_production_live_jobs(
            query="Salesforce Admin",
            location="India",
            jobs=jobs,
            limit=8,
        )

        self.assertEqual([item["title"] for item in selected], ["Salesforce Administrator (App Builder)"])

    def test_secondary_candidate_path_is_more_lenient_than_strict_path(self) -> None:
        item = {
            "title": "Analytics Manager",
            "company": "Example",
            "source": "jobicy",
            "description": "Lead reporting workflows, dashboards, and business analytics for operations teams.",
            "location": "India",
            "tags": [],
            "normalized_data": {
                "skills": ["sql", "analytics", "reporting"],
                "role_fit_score": 4.0,
                "market_quality_score": 45.0,
                "title_alignment_score": 6.0,
            },
        }

        self.assertFalse(
            self.aggregator._is_production_live_candidate("Data Analyst", "India", item, strict=True)
        )
        self.assertTrue(
            self.aggregator._is_production_live_candidate("Data Analyst", "India", item, strict=False)
        )

    def test_dense_roles_do_not_reinsert_lever_into_fallback(self) -> None:
        source_groups = {
            "greenhouse": [object()],
            "jobicy": [object()],
            "lever": [object()],
            "remotive": [object()],
            "themuse": [object()],
        }
        plan = self.aggregator._build_production_provider_plan(
            query="Data Analyst",
            location="India",
            source_groups=source_groups,
        )
        self.assertEqual(plan["fallback_sources"], [])
        self.assertNotIn("lever", plan["fallback_sources"])

    def test_india_focused_dense_roles_keep_fast_public_sources_ahead_of_indianapi(self) -> None:
        source_groups = {
            "adzuna": [object()],
            "greenhouse": [object()],
            "jobicy": [object()],
            "jooble": [object()],
            "remotive": [object()],
            "themuse": [object()],
        }
        plan = self.aggregator._build_production_provider_plan(
            query="DevOps Engineer",
            location="India",
            source_groups=source_groups,
        )
        self.assertEqual(plan["primary_sources"][:3], ["remotive", "jobicy", "jooble"])
        self.assertNotIn("indianapi", plan["primary_sources"])
        self.assertNotIn("indianapi", plan["supplemental_sources"])

    def test_themuse_stays_supplemental_only_for_dense_roles(self) -> None:
        source_groups = {
            "greenhouse": [object()],
            "jobicy": [object()],
            "remotive": [object()],
            "themuse": [object()],
        }
        plan = self.aggregator._build_production_provider_plan(
            query="Cybersecurity Engineer",
            location="India",
            source_groups=source_groups,
        )
        self.assertIn("themuse", plan["supplemental_sources"])
        self.assertNotIn("themuse", plan["primary_sources"])

    def test_web_developer_prioritizes_fast_frontend_sources(self) -> None:
        source_groups = {
            "adzuna": [object()],
            "greenhouse": [object()],
            "jobicy": [object()],
            "jooble": [object()],
            "remotive": [object()],
            "themuse": [object()],
        }
        plan = self.aggregator._build_production_provider_plan(
            query="Web Developer",
            location="Global",
            source_groups=source_groups,
        )
        self.assertEqual(plan["primary_sources"][:3], ["remotive", "jobicy", "jooble"])
        self.assertGreaterEqual(len(plan["supplemental_sources"]), 1)
        self.assertEqual(plan["supplemental_sources"][:2], ["greenhouse", "themuse"])
        self.assertEqual(plan["fallback_sources"], [])

    def test_dense_underfill_grace_window_extends_underfilled_primary_stage(self) -> None:
        grace_seconds = self.aggregator._production_underfill_grace_seconds(
            stage="primary",
            query="Data Analyst",
            current_live_count=1,
            pending_task_count=3,
            live_floor=6,
            partial_live_floor=4,
        )
        self.assertGreaterEqual(grace_seconds, 0.75)

    def test_data_analyst_supplemental_stage_has_room_for_jooble_completion(self) -> None:
        timeout_seconds = self.aggregator._production_stage_soft_timeout(
            stage="supplemental",
            query="Data Analyst",
            sparse_role=False,
        )

        self.assertGreaterEqual(timeout_seconds, 9.0)

    def test_dense_underfill_grace_window_stays_off_once_floor_is_met(self) -> None:
        grace_seconds = self.aggregator._production_underfill_grace_seconds(
            stage="supplemental",
            query="Web Developer",
            current_live_count=4,
            pending_task_count=2,
            live_floor=6,
            partial_live_floor=4,
        )
        self.assertEqual(grace_seconds, 0.0)

    def test_selection_debug_tracks_rejection_reasons(self) -> None:
        jobs = [
            {
                "title": "Data Analyst",
                "company": "Alpha",
                "source": "remotive",
                "description": "Own SQL dashboards and executive reporting for finance.",
                "location": "India",
                "tags": ["data analyst"],
                "normalized_data": {
                    "skills": ["sql", "excel", "analytics"],
                    "title_alignment_score": 18.0,
                    "role_fit_score": 12.0,
                    "market_quality_score": 24.0,
                },
            },
            {
                "title": "Data Analyst",
                "company": "Alpha",
                "source": "jobicy",
                "description": "Own experimentation analysis and performance reporting for growth.",
                "location": "India",
                "tags": ["data analyst"],
                "normalized_data": {
                    "skills": ["sql", "power bi", "analytics"],
                    "title_alignment_score": 17.0,
                    "role_fit_score": 11.0,
                    "market_quality_score": 23.0,
                },
            },
            {
                "title": "Senior Data Analyst",
                "company": "Alpha",
                "source": "greenhouse",
                "description": "Own analytics for product planning and operations forecasting.",
                "location": "India",
                "tags": ["data analyst"],
                "normalized_data": {
                    "skills": ["sql", "tableau", "analytics"],
                    "title_alignment_score": 16.5,
                    "role_fit_score": 10.5,
                    "market_quality_score": 22.5,
                },
            },
            {
                "title": "Lead Data Analyst",
                "company": "Alpha",
                "source": "themuse",
                "description": "Own analytics strategy for supply chain and planning teams.",
                "location": "India",
                "tags": ["data analyst"],
                "normalized_data": {
                    "skills": ["sql", "excel", "analytics"],
                    "title_alignment_score": 16.0,
                    "role_fit_score": 10.0,
                    "market_quality_score": 22.0,
                },
            },
        ]

        self.aggregator._select_production_live_jobs(
            query="Data Analyst",
            location="India",
            jobs=jobs,
            limit=8,
        )

        debug = self.aggregator.last_fetch_diagnostics["selection_debug"]
        self.assertIn("rejections", debug)
        self.assertGreaterEqual(sum(debug["rejections"].values()), 1)

    def test_unknown_company_jobs_do_not_collapse_same_title_live_cards(self) -> None:
        jobs = [
            {
                "title": "Product Designer",
                "company": "",
                "external_id": "ux-1",
                "source": "themuse",
                "description": "Own Figma prototypes, UX research synthesis, and visual design systems for growth surfaces.",
                "location": "Global",
                "tags": ["product designer"],
                "normalized_data": {
                    "skills": ["figma", "ux design", "ui design"],
                    "title_alignment_score": 18.0,
                    "role_fit_score": 10.0,
                    "market_quality_score": 22.0,
                },
            },
            {
                "title": "Product Designer",
                "company": "",
                "external_id": "ux-2",
                "source": "themuse",
                "description": "Design mobile-first user journeys, wireframes, and prototyping systems for onboarding.",
                "location": "Global",
                "tags": ["product designer"],
                "normalized_data": {
                    "skills": ["figma", "prototyping", "ux design"],
                    "title_alignment_score": 17.5,
                    "role_fit_score": 9.5,
                    "market_quality_score": 21.5,
                },
            },
            {
                "title": "Product Designer",
                "company": "",
                "external_id": "ux-3",
                "source": "themuse",
                "description": "Lead interaction design, accessibility reviews, and design system documentation for web experiences.",
                "location": "Global",
                "tags": ["product designer"],
                "normalized_data": {
                    "skills": ["figma", "ui design", "ux design"],
                    "title_alignment_score": 17.0,
                    "role_fit_score": 9.0,
                    "market_quality_score": 21.0,
                },
            },
        ]

        selected = self.aggregator._select_production_live_jobs(
            query="UI/UX Designer",
            location="Global",
            jobs=jobs,
            limit=10,
        )

        self.assertEqual(len(selected), 3)

    def test_baseline_jobs_are_never_counted_as_live_cards(self) -> None:
        jobs = [
            {
                "title": "Data Analyst",
                "company": "Alpha",
                "source": "remotive",
                "description": "Own SQL dashboards, reporting, and analytics workflows.",
                "location": "India",
                "tags": ["data analyst"],
                "normalized_data": {
                    "skills": ["sql", "excel", "analytics"],
                    "title_alignment_score": 20.0,
                    "role_fit_score": 18.0,
                    "market_quality_score": 25.0,
                },
            },
            {
                "title": "Entry-Level Data Analyst",
                "company": "Fallback",
                "source": "role-baseline",
                "description": "Calibration baseline only.",
                "location": "India",
                "tags": ["data analyst"],
                "normalized_data": {
                    "skills": ["sql", "excel"],
                    "title_alignment_score": 18.0,
                    "role_fit_score": 12.0,
                    "market_quality_score": 22.0,
                },
            },
        ]

        selected = self.aggregator._select_production_live_jobs(
            query="Data Analyst",
            location="India",
            jobs=jobs,
            limit=10,
        )

        self.assertEqual(len(selected), 1)
        self.assertTrue(all(item["source"] != "role-baseline" for item in selected))

    def test_dense_role_reaches_six_live_cards_when_upstream_has_six_family_safe_matches(self) -> None:
        jobs = []
        titles = [
            ("Data Analyst", "Alpha", "remotive"),
            ("Senior Data Analyst", "Beta", "jobicy"),
            ("Data Operations Analyst", "Gamma", "themuse"),
            ("Business Intelligence Analyst", "Delta", "greenhouse"),
            ("Reporting Analyst", "Epsilon", "remotive"),
            ("Analytics Analyst", "Zeta", "jobicy"),
        ]
        for index, (title, company, source) in enumerate(titles, start=1):
            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "source": source,
                    "description": "Own SQL, Tableau, Power BI, dashboarding, and analytics workflows.",
                    "location": "India",
                    "tags": ["data analyst"],
                    "normalized_data": {
                        "skills": ["sql", "excel", "analytics", "power bi"],
                        "title_alignment_score": 22.0 - index,
                        "role_fit_score": 16.0 - (index * 0.5),
                        "market_quality_score": 24.0,
                    },
                }
            )

        selected = self.aggregator._select_production_live_jobs(
            query="Data Analyst",
            location="India",
            jobs=jobs,
            limit=10,
        )

        self.assertGreaterEqual(len(selected), 6)
        debug = self.aggregator.last_fetch_diagnostics["selection_debug"]
        self.assertGreaterEqual(debug["upstream_family_safe_count"], 6)

    def test_dense_role_uses_exact_backup_matches_to_fill_target_count(self) -> None:
        jobs = []
        titles = [
            ("Data Analyst", "Alpha", "remotive"),
            ("Senior Data Analyst", "Beta", "jobicy"),
            ("Lead Data Analyst", "Gamma", "greenhouse"),
            ("Online Data Analyst United States", "Delta", "remotive"),
            ("Reporting Analyst", "Epsilon", "jobicy"),
            ("Analytics Analyst", "Zeta", "greenhouse"),
            ("BI Analyst", "Eta", "themuse"),
            ("Operations Analyst", "Theta", "greenhouse"),
            ("Operations Analyst", "Iota", "jobicy"),
            ("Operations Analyst", "Kappa", "themuse"),
        ]
        for index, (title, company, source) in enumerate(titles, start=1):
            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "source": source,
                    "external_id": f"job-{index}",
                    "url": f"https://example.test/jobs/{index}",
                    "description": "Own SQL reporting, dashboards, KPI analysis, and business intelligence workflows.",
                    "location": "India",
                    "tags": ["data analyst"],
                    "normalized_data": {
                        "skills": ["sql", "excel", "analytics", "power bi", "reporting", "dashboarding"],
                        "title_alignment_score": 22.0 - index,
                        "role_fit_score": 16.0 - (index * 0.25),
                        "market_quality_score": 24.0,
                    },
                }
            )

        selected = self.aggregator._select_production_live_jobs(
            query="Data Analyst",
            location="India",
            jobs=jobs,
            limit=10,
        )

        self.assertEqual(len(selected), 10)
        selected_companies = {item["company"] for item in selected}
        self.assertTrue({"Theta", "Iota", "Kappa"}.issubset(selected_companies))

    def test_sparse_live_state_stays_honest_when_upstream_supply_is_below_six(self) -> None:
        jobs = [
            {
                "title": "Technical Writer",
                "company": "Alpha",
                "source": "jobicy",
                "description": "Write product documentation, API guides, and release notes.",
                "location": "India",
                "tags": ["technical writer"],
                "normalized_data": {
                    "skills": ["documentation", "api documentation"],
                    "title_alignment_score": 18.0,
                    "role_fit_score": 9.0,
                    "market_quality_score": 24.0,
                },
            },
            {
                "title": "Documentation Engineer",
                "company": "Beta",
                "source": "themuse",
                "description": "Own developer docs, release notes, and internal documentation tooling.",
                "location": "India",
                "tags": ["technical writer"],
                "normalized_data": {
                    "skills": ["documentation", "developer docs"],
                    "title_alignment_score": 17.0,
                    "role_fit_score": 8.0,
                    "market_quality_score": 23.0,
                },
            },
        ]

        selected = self.aggregator._select_production_live_jobs(
            query="Technical Writer",
            location="India",
            jobs=jobs,
            limit=10,
        )

        self.assertEqual(len(selected), 2)
        underfill = self.aggregator.last_fetch_diagnostics["selection_debug"]["underfill"]
        self.assertEqual(underfill["reason"], "upstream_scarcity")


if __name__ == "__main__":
    unittest.main()
