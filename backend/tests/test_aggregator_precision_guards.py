from __future__ import annotations

import asyncio
import time
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

    def test_greenhouse_india_search_uses_india_heavy_board_mix(self) -> None:
        provider = GreenhouseProvider()
        previous_environment = settings.environment
        settings.environment = "production"
        try:
            boards = provider._boards_for_query("Data Analyst", location="India")
        finally:
            settings.environment = previous_environment

        self.assertEqual(boards[:4], ["yipitdata", "okta", "rubrik", "airbnb"])
        self.assertGreaterEqual(len(boards), 6)

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

    def test_generic_cybersecurity_query_accepts_security_analyst_titles(self) -> None:
        item = {
            "title": "Security Operations Analyst",
            "description": "Work in SOC operations, SIEM alerting, incident response, threat detection, and IAM review.",
            "tags": ["security analyst"],
            "location": "India",
            "source": "jooble",
            "external_id": "security-ops-analyst",
            "url": "https://example.test/security-ops-analyst",
            "normalized_data": {
                "skills": ["siem", "iam", "incident response", "threat detection", "security operations"],
                "role_fit_score": 4.5,
                "market_quality_score": 42.0,
                "title_alignment_score": 18.0,
            },
        }

        self.assertFalse(self.aggregator._requires_specialty_guard("cybersecurity"))
        self.assertTrue(self.aggregator._passes_precise_query_guard("cybersecurity", item))
        self.assertTrue(self.aggregator._passes_final_live_guard("cybersecurity", item))
        self.assertTrue(self.aggregator._is_production_live_candidate("cybersecurity", "India", item, strict=True))

    def test_financial_analyst_rejects_accountant_adjacent_titles(self) -> None:
        for title in ["Accountant", "Junior Accountant", "Tax Compliance Specialist", "Finance Executive"]:
            with self.subTest(title=title):
                item = {
                    "title": title,
                    "description": "Own accounting close, tax compliance, reconciliation, and finance reporting.",
                    "tags": [],
                    "normalized_data": {
                        "skills": ["accounting", "tax", "reconciliation", "excel"],
                        "role_fit_score": 6.0,
                        "market_quality_score": 30.0,
                        "title_alignment_score": 10.0,
                    },
                }
                self.assertFalse(self.aggregator._passes_final_live_guard("Financial Analyst", item))
                self.assertFalse(self.aggregator._passes_exact_query_backup_guard("Financial Analyst", "India", item))
                self.assertFalse(self.aggregator._is_production_live_candidate("Financial Analyst", "India", item, strict=False))

    def test_financial_analyst_keeps_true_analyst_titles(self) -> None:
        item = {
            "title": "FP&A Analyst",
            "description": "Own financial modeling, forecasting, budgeting, revenue analysis, and Excel reporting.",
            "tags": [],
            "normalized_data": {
                "skills": ["financial modeling", "forecasting", "budgeting", "excel"],
                "role_fit_score": 6.0,
                "market_quality_score": 30.0,
                "title_alignment_score": 18.0,
            },
        }

        self.assertTrue(self.aggregator._passes_final_live_guard("Financial Analyst", item))
        self.assertTrue(self.aggregator._passes_exact_query_backup_guard("Financial Analyst", "India", item))

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
        self.assertEqual(plan["primary_sources"], ["jooble"])
        self.assertEqual(plan["supplemental_sources"], ["remotive", "greenhouse", "themuse", "jobicy", "adzuna"])
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
        self.assertEqual(plan["primary_sources"], ["adzuna", "jooble"])
        self.assertEqual(plan["supplemental_sources"], ["remotive", "jobicy", "greenhouse", "themuse"])

    def test_data_analyst_provider_plan_prioritizes_fast_high_yield_sources(self) -> None:
        source_groups = {name: [object()] for name in ["remotive", "jobicy", "greenhouse", "themuse", "jooble", "adzuna"]}
        plan = self.aggregator._build_production_provider_plan(
            query="Data Analyst",
            location="Global",
            source_groups=source_groups,
        )
        self.assertEqual(plan["primary_sources"], ["adzuna", "jooble"])
        self.assertEqual(plan["supplemental_sources"], ["remotive", "themuse", "jobicy", "greenhouse"])

    def test_data_analyst_india_provider_plan_prioritizes_fast_india_sources(self) -> None:
        source_groups = {name: [object()] for name in ["remotive", "jobicy", "greenhouse", "themuse", "jooble", "adzuna"]}
        plan = self.aggregator._build_production_provider_plan(
            query="Data Analyst",
            location="India",
            source_groups=source_groups,
        )

        self.assertEqual(plan["primary_sources"], ["jooble"])
        self.assertEqual(plan["supplemental_sources"], ["greenhouse", "jobicy", "remotive", "themuse", "adzuna"])

    def test_cybersecurity_india_provider_plan_keeps_exact_pass_short(self) -> None:
        source_groups = {name: [object()] for name in ["remotive", "jobicy", "greenhouse", "themuse", "jooble", "adzuna"]}
        plan = self.aggregator._build_production_provider_plan(
            query="cybersecurity",
            location="India",
            source_groups=source_groups,
        )

        self.assertEqual(plan["primary_sources"], ["jooble"])
        self.assertEqual(plan["supplemental_sources"], ["greenhouse", "jobicy", "remotive", "themuse", "adzuna"])
        self.assertEqual(
            self.aggregator._production_search_query_cap(
                source_name="jooble",
                query="cybersecurity",
                location="India",
            ),
            1,
        )

    def test_financial_analyst_india_provider_plan_uses_adzuna_first(self) -> None:
        source_groups = {name: [object()] for name in ["remotive", "jobicy", "greenhouse", "themuse", "jooble", "adzuna"]}
        plan = self.aggregator._build_production_provider_plan(
            query="Financial Analyst",
            location="India",
            source_groups=source_groups,
        )

        self.assertEqual(plan["primary_sources"], ["adzuna"])
        self.assertEqual(plan["supplemental_sources"], ["jooble", "jobicy", "remotive", "themuse", "greenhouse"])

    def test_business_analyst_reuses_the_fast_analyst_global_provider_plan(self) -> None:
        source_groups = {name: [object()] for name in ["remotive", "jobicy", "greenhouse", "themuse", "jooble", "adzuna"]}
        plan = self.aggregator._build_production_provider_plan(
            query="Business Analyst",
            location="Global",
            source_groups=source_groups,
        )
        self.assertEqual(plan["primary_sources"], ["adzuna", "jooble"])
        self.assertEqual(plan["supplemental_sources"], ["remotive", "themuse", "jobicy", "greenhouse"])

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

    def test_web_developer_keeps_full_stack_software_engineer_with_strong_frontend_signals(self) -> None:
        item = {
            "title": "Senior Software Engineer, Full-Stack",
            "description": "Build modern web applications with React, TypeScript, frontend systems, and full-stack product delivery.",
            "tags": [],
            "normalized_data": {
                "skills": ["react", "typescript", "javascript", "frontend", "css"],
                "role_fit_score": 10.0,
                "market_quality_score": 20.0,
                "title_alignment_score": 8.0,
            },
        }
        self.assertTrue(self.aggregator._passes_contextual_family_recovery_guard("Web Developer", item))
        self.assertTrue(self.aggregator._passes_final_live_guard("Web Developer", item))

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
        self.assertEqual(plan["primary_sources"], ["jooble"])
        self.assertEqual(plan["supplemental_sources"], ["greenhouse", "jobicy", "remotive", "themuse", "adzuna"])
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

    def test_enterprise_india_provider_plan_prioritizes_location_aware_sources(self) -> None:
        source_groups = {
            "adzuna": [object()],
            "greenhouse": [object()],
            "jobicy": [object()],
            "jooble": [object()],
            "remotive": [object()],
            "themuse": [object()],
        }
        plan = self.aggregator._build_production_provider_plan(
            query="Salesforce Admin",
            location="India",
            source_groups=source_groups,
        )
        self.assertEqual(plan["primary_sources"], ["adzuna", "jooble"])
        self.assertEqual(plan["supplemental_sources"], ["jobicy", "remotive", "themuse", "greenhouse"])

    def test_enterprise_india_allows_extra_adzuna_variation_for_platform_supply(self) -> None:
        provider = type("Provider", (), {"source_name": "adzuna", "supports_query_variations": True})()
        previous_environment = settings.environment
        settings.environment = "production"
        try:
            queries = self.aggregator._search_queries(provider, "Enterprise Applications Engineer", "India")
        finally:
            settings.environment = previous_environment
        self.assertGreaterEqual(len(queries), 3)
        self.assertIn("salesforce administrator", queries)

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
        self.assertEqual(plan["primary_sources"], ["adzuna", "jooble"])
        self.assertGreaterEqual(len(plan["supplemental_sources"]), 1)
        self.assertEqual(plan["supplemental_sources"], ["remotive", "jobicy", "greenhouse", "themuse"])
        self.assertEqual(plan["fallback_sources"], [])

    def test_web_developer_uses_lever_as_late_recovery_source_when_available(self) -> None:
        source_groups = {
            "greenhouse": [object()],
            "jobicy": [object()],
            "jooble": [object()],
            "lever": [object()],
            "remotive": [object()],
            "themuse": [object()],
        }
        plan = self.aggregator._build_production_provider_plan(
            query="Web Developer",
            location="Global",
            source_groups=source_groups,
        )
        self.assertEqual(plan["primary_sources"], ["jooble"])
        self.assertEqual(plan["supplemental_sources"][:3], ["remotive", "jobicy", "greenhouse"])
        self.assertEqual(plan["fallback_sources"], ["lever"])

    def test_dense_underfill_grace_window_does_not_delay_next_stage(self) -> None:
        grace_seconds = self.aggregator._production_underfill_grace_seconds(
            stage="primary",
            query="Data Analyst",
            current_live_count=1,
            pending_task_count=3,
            live_floor=6,
            partial_live_floor=4,
        )
        self.assertEqual(grace_seconds, 0.0)

    def test_data_analyst_supplemental_stage_has_room_for_jooble_completion(self) -> None:
        timeout_seconds = self.aggregator._production_stage_soft_timeout(
            stage="supplemental",
            query="Data Analyst",
            sparse_role=False,
        )

        self.assertGreaterEqual(timeout_seconds, 9.0)

    def test_data_analyst_primary_stage_has_room_for_slow_but_useful_sources(self) -> None:
        timeout_seconds = self.aggregator._production_stage_soft_timeout(
            stage="primary",
            query="Data Analyst",
            sparse_role=False,
        )

        self.assertGreaterEqual(timeout_seconds, 10.0)

    def test_dense_roles_target_requested_sample_above_ten(self) -> None:
        self.assertEqual(self.aggregator._production_live_target(query="Data Analyst", limit=12), 12)
        self.assertGreaterEqual(self.aggregator._production_display_floor(query="Data Analyst", limit=12), 8)
        self.assertGreaterEqual(self.aggregator._production_partial_live_floor(query="Data Analyst", limit=12), 6)

    def test_underfilled_live_jobs_blend_with_safe_cached_jobs(self) -> None:
        def make_job(idx: int, source_name: str) -> dict:
            return {
                "title": f"Senior Data Analyst {idx}",
                "company": f"Data Co {idx}",
                "source": source_name,
                "description": "Own SQL dashboards, Power BI reporting, Excel analysis, and analytics workflows.",
                "url": f"https://example.com/{source_name}/{idx}",
                "tags": ["data analyst", "analytics"],
                "normalized_data": {
                    "skills": ["sql", "power bi", "excel", "analytics", "reporting"],
                    "title_alignment_score": 18.0,
                    "role_fit_score": 14.0,
                    "market_quality_score": 24.0,
                },
            }

        live_jobs = [make_job(idx, "remotive") for idx in range(3)]
        cached_jobs = [make_job(idx, "jobicy") for idx in range(3, 8)]

        selected = self.aggregator._blend_underfilled_production_live_jobs(
            query="Data Analyst",
            location="Global",
            limit=10,
            live_jobs=live_jobs,
            cached_jobs=cached_jobs,
        )

        self.assertGreaterEqual(len(selected), 6)
        self.assertEqual(
            self.aggregator.last_fetch_diagnostics["underfill_cache_blend"]["fresh_live_count"],
            3,
        )

    def test_production_fetch_preserves_stronger_primary_live_set_when_later_stage_is_weaker(self) -> None:
        class FakeProvider:
            supports_query_variations = False
            supports_location_variations = False

            def __init__(self, source_name: str, jobs: list[dict]) -> None:
                self.source_name = source_name
                self._jobs = jobs

            async def search(self, query: str, location: str, limit: int) -> list[dict]:
                return list(self._jobs)

        class ScriptedAggregator(JobAggregator):
            def __init__(self, scripted_live_results: list[list[dict]], providers: list[object]) -> None:
                super().__init__(None)
                self.providers = providers
                self._scripted_live_results = scripted_live_results
                self._selection_call_index = 0

            def _build_production_provider_plan(self, *, query: str, location: str, source_groups: dict[str, list[object]]) -> dict[str, object]:
                return {
                    "family_group": "data",
                    "dense_family": True,
                    "primary_sources": ["jobicy"],
                    "supplemental_sources": ["themuse"],
                    "fallback_sources": [],
                    "active_sources": ["jobicy", "themuse"],
                }

            def _select_production_live_jobs(self, *, query: str, location: str, jobs: list[dict], limit: int) -> list[dict]:
                index = min(self._selection_call_index, len(self._scripted_live_results) - 1)
                self._selection_call_index += 1
                self.last_fetch_diagnostics["selection_debug"] = {
                    "precision_guarded_candidates": len(self._scripted_live_results[index]),
                    "upstream_family_safe_count": len(self._scripted_live_results[index]),
                    "underfill": {"reason": "scripted_test"},
                }
                return list(self._scripted_live_results[index])

        def make_live_jobs(count: int, source_name: str) -> list[dict]:
            return [
                {
                    "title": f"Data Analyst {idx}",
                    "company": f"Company {idx}",
                    "source": source_name,
                    "description": "Own SQL dashboards, reporting, and analytics workflows.",
                    "url": f"https://example.com/{source_name}/{idx}",
                    "normalized_data": {
                        "skills": ["sql", "analytics"],
                        "role_fit_score": 10.0,
                        "market_quality_score": 20.0,
                        "title_alignment_score": 15.0,
                    },
                }
                for idx in range(count)
            ]

        primary_live = make_live_jobs(7, "jobicy")
        weaker_supplemental_live = make_live_jobs(4, "themuse")
        providers = [
            FakeProvider("jobicy", make_live_jobs(2, "jobicy")),
            FakeProvider("themuse", make_live_jobs(2, "themuse")),
        ]
        aggregator = ScriptedAggregator(
            scripted_live_results=[primary_live, primary_live, weaker_supplemental_live, weaker_supplemental_live],
            providers=providers,
        )
        previous_environment = settings.environment
        settings.environment = "production"
        try:
            selected = asyncio.run(
                aggregator._fetch_production_jobs(
                    query="Data Analyst",
                    location="Global",
                    limit=10,
                )
            )
        finally:
            settings.environment = previous_environment

        self.assertEqual(len(selected), 7)
        self.assertEqual(aggregator.last_fetch_diagnostics["selected_live_count"], 7)
        self.assertEqual(aggregator.last_fetch_diagnostics["best_live_result"]["stage"], "primary")
        self.assertEqual(
            aggregator.last_fetch_diagnostics["best_live_preserved_after_stage"]["preserved_selected_live"],
            7,
        )

    def test_india_dense_underfill_uses_global_role_fallback_after_exact_market(self) -> None:
        class LocationAwareFakeProvider:
            supports_query_variations = False
            supports_location_variations = False

            def __init__(self, source_name: str, india_jobs: list[dict], global_jobs: list[dict]) -> None:
                self.source_name = source_name
                self._india_jobs = india_jobs
                self._global_jobs = global_jobs

            async def search(self, query: str, location: str, limit: int) -> list[dict]:
                if str(location).lower() == "global":
                    return list(self._global_jobs)
                return list(self._india_jobs)

        def make_security_job(idx: int, location: str, *, source: str = "jooble") -> dict:
            variants = [
                "Cyber Security Analyst",
                "Cybersecurity Engineer",
                "Information Security Analyst",
                "SOC Analyst",
                "Cloud Security Engineer",
                "Application Security Engineer",
                "Security Operations Analyst",
                "Threat Detection Engineer",
                "Vulnerability Analyst",
                "Incident Response Analyst",
                "Product Security Engineer",
                "Cyber Defense Analyst",
            ]
            title = variants[idx % len(variants)]
            return {
                "source": source,
                "external_id": f"{source}-{location}-{idx}",
                "url": f"https://example.com/{source}/{location}/{idx}",
                "title": title,
                "company": f"Security Company {variants[idx % len(variants)].replace(' ', '')}",
                "location": location,
                "remote": location == "Worldwide",
                "description": f"{title} role covering SIEM, incident response, IAM, and vulnerability management.",
                "tags": ["cyber security", "security analyst"],
                "normalized_data": {
                    "skills": ["siem", "incident response", "iam", "vulnerability management"],
                    "role_fit_score": 8.0,
                    "market_quality_score": 36.0,
                    "title_alignment_score": 20.0,
                },
            }

        india_jobs = [make_security_job(idx, "India") for idx in range(2)]
        global_jobs = [make_security_job(idx, "Worldwide") for idx in range(12)]
        aggregator = JobAggregator(None)
        aggregator.providers = [
            LocationAwareFakeProvider("jooble", india_jobs, global_jobs),
            LocationAwareFakeProvider("remotive", [], []),
        ]
        previous_environment = settings.environment
        settings.environment = "production"
        try:
            selected = asyncio.run(
                aggregator._fetch_production_jobs(
                    query="Cybersecurity",
                    location="India",
                    limit=15,
                )
            )
        finally:
            settings.environment = previous_environment

        self.assertGreaterEqual(len(selected), 10)
        self.assertEqual([item["location"] for item in selected[:2]], ["India", "India"])
        self.assertTrue(
            any(
                str(stage["stage"]).startswith("global_fallback")
                for stage in aggregator.last_fetch_diagnostics["stage_results"]
            ),
        )
        partial_return = aggregator.last_fetch_diagnostics.get("partial_live_return")
        if partial_return:
            self.assertIn(
                partial_return["reason"],
                {
                    "india_underfill_role_accurate_global_fallback",
                    "acceptable_partial_live_set",
                },
            )

    def test_india_dense_underfill_global_fill_runs_before_slow_primary_source_exhausts_request(self) -> None:
        class SlowProvider:
            supports_query_variations = False
            supports_location_variations = False
            source_name = "adzuna"

            async def search(self, query: str, location: str, limit: int) -> list[dict]:
                await asyncio.sleep(5)
                return []

        class LocationAwareFakeProvider:
            supports_query_variations = False
            supports_location_variations = False
            source_name = "jooble"

            async def search(self, query: str, location: str, limit: int) -> list[dict]:
                if str(location).lower() == "global":
                    return [make_security_job(idx, "Worldwide") for idx in range(12)]
                return [make_security_job(0, "India")]

        class FastTimeoutAggregator(JobAggregator):
            def _production_stage_soft_timeout(
                self,
                *,
                stage: str,
                query: str,
                sparse_role: bool,
                india_focused_location: bool = False,
            ) -> float:
                return 0.6 if stage in {"primary", "global_fallback_fast", "global_fallback_slow"} else super()._production_stage_soft_timeout(
                    stage=stage,
                    query=query,
                    sparse_role=sparse_role,
                    india_focused_location=india_focused_location,
                )

        def make_security_job(idx: int, location: str) -> dict:
            titles = [
                "Cyber Security Analyst",
                "Security Engineer",
                "Cloud Security Engineer",
                "Application Security Engineer",
                "SOC Analyst",
                "Information Security Analyst",
                "Threat Detection Engineer",
                "Vulnerability Analyst",
                "Incident Response Analyst",
                "Security Operations Analyst",
                "Product Security Engineer",
                "Cyber Defense Analyst",
            ]
            title = titles[idx % len(titles)]
            return {
                "source": "jooble",
                "external_id": f"jooble-{location}-{idx}",
                "url": f"https://example.com/{location}/{idx}",
                "title": title,
                "company": f"Security Co {idx}",
                "location": location,
                "remote": location == "Worldwide",
                "description": f"{title} role covering SIEM, IAM, vulnerability management, and incident response.",
                "tags": ["cybersecurity", "security analyst"],
                "normalized_data": {
                    "skills": ["siem", "iam", "incident response", "vulnerability management"],
                    "role_fit_score": 8.0,
                    "market_quality_score": 34.0,
                    "title_alignment_score": 18.0,
                },
            }

        aggregator = FastTimeoutAggregator(None)
        aggregator.providers = [SlowProvider(), LocationAwareFakeProvider()]
        previous_environment = settings.environment
        settings.environment = "production"
        started_at = time.perf_counter()
        try:
            selected = asyncio.run(
                aggregator._fetch_production_jobs(
                    query="Cybersecurity",
                    location="India",
                    limit=15,
                )
            )
        finally:
            settings.environment = previous_environment

        provider_sources = [entry["source"] for entry in aggregator.last_fetch_diagnostics.get("providers", [])]
        self.assertLess(time.perf_counter() - started_at, 5.0)
        self.assertGreaterEqual(len(selected), 10)
        self.assertEqual(selected[0]["location"], "India")
        self.assertNotIn("adzuna", provider_sources)
        self.assertTrue(
            any(
                str(stage["stage"]).startswith("global_fallback")
                for stage in aggregator.last_fetch_diagnostics["stage_results"]
            ),
        )

    def test_india_global_fast_fill_does_not_wait_for_slow_credential_sources(self) -> None:
        class SlowCredentialProvider:
            supports_query_variations = False
            supports_location_variations = False
            source_name = "adzuna"

            async def search(self, query: str, location: str, limit: int) -> list[dict]:
                await asyncio.sleep(5)
                return []

        class ExactIndiaProvider:
            supports_query_variations = False
            supports_location_variations = False
            source_name = "jooble"

            async def search(self, query: str, location: str, limit: int) -> list[dict]:
                return [make_security_job(0, "India", "jooble")]

        class FastAtsProvider:
            supports_query_variations = False
            supports_location_variations = False
            source_name = "greenhouse"

            async def search(self, query: str, location: str, limit: int) -> list[dict]:
                await asyncio.sleep(0.05)
                return [make_security_job(idx, "Worldwide", "greenhouse") for idx in range(12)]

        class FastTimeoutAggregator(JobAggregator):
            def _production_stage_soft_timeout(
                self,
                *,
                stage: str,
                query: str,
                sparse_role: bool,
                india_focused_location: bool = False,
            ) -> float:
                if stage == "primary":
                    return 0.6
                if stage == "global_fallback_fast":
                    return 1.2
                if stage == "global_fallback_slow":
                    return 0.6
                return super()._production_stage_soft_timeout(
                    stage=stage,
                    query=query,
                    sparse_role=sparse_role,
                    india_focused_location=india_focused_location,
                )

        def make_security_job(idx: int, location: str, source: str) -> dict:
            titles = [
                "Cyber Security Analyst",
                "Security Engineer",
                "Cloud Security Engineer",
                "Application Security Engineer",
                "SOC Analyst",
                "Information Security Analyst",
                "Threat Detection Engineer",
                "Vulnerability Analyst",
                "Incident Response Analyst",
                "Security Operations Analyst",
                "Product Security Engineer",
                "Cyber Defense Analyst",
            ]
            title = titles[idx % len(titles)]
            return {
                "source": source,
                "external_id": f"{source}-{location}-{idx}",
                "url": f"https://example.com/{source}/{location}/{idx}",
                "title": title,
                "company": f"Security Co {idx}",
                "location": location,
                "remote": location == "Worldwide",
                "description": f"{title} role covering SIEM, IAM, vulnerability management, and incident response.",
                "tags": ["cybersecurity", "security analyst"],
                "normalized_data": {
                    "skills": ["siem", "iam", "incident response", "vulnerability management"],
                    "role_fit_score": 8.0,
                    "market_quality_score": 34.0,
                    "title_alignment_score": 18.0,
                },
            }

        aggregator = FastTimeoutAggregator(None)
        aggregator.providers = [SlowCredentialProvider(), ExactIndiaProvider(), FastAtsProvider()]
        previous_environment = settings.environment
        settings.environment = "production"
        started_at = time.perf_counter()
        try:
            selected = asyncio.run(
                aggregator._fetch_production_jobs(
                    query="Cybersecurity",
                    location="India",
                    limit=15,
                )
            )
        finally:
            settings.environment = previous_environment

        stages = [stage["stage"] for stage in aggregator.last_fetch_diagnostics["stage_results"]]
        provider_sources = [entry["source"] for entry in aggregator.last_fetch_diagnostics.get("providers", [])]
        self.assertLess(time.perf_counter() - started_at, 5.0)
        self.assertGreaterEqual(len(selected), 10)
        self.assertEqual(stages[0], "global_fallback_fast")
        self.assertIn("global_fallback_fast", stages)
        self.assertNotIn("global_fallback_slow", stages)
        self.assertNotIn("adzuna", provider_sources)

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

    def test_dense_role_can_select_twelve_when_upstream_has_twelve_safe_matches(self) -> None:
        jobs = []
        sources = ["remotive", "jobicy", "themuse", "greenhouse", "jooble", "adzuna"]
        titles = [
            "Data Analyst",
            "Senior Data Analyst",
            "Product Data Analyst",
            "Marketing Data Analyst",
            "Operations Data Analyst",
            "Business Intelligence Analyst",
            "Reporting Analyst",
            "Analytics Analyst",
            "Insights Analyst",
            "Data Operations Analyst",
            "BI Analyst",
            "Lead Data Analyst",
        ]
        for index, title in enumerate(titles, start=1):
            jobs.append(
                {
                    "title": title,
                    "company": f"Analytics Co {index}",
                    "source": sources[index % len(sources)],
                    "external_id": f"dense-{index}",
                    "url": f"https://example.test/data/{index}",
                    "description": (
                        f"Own SQL reporting, Power BI dashboards, Excel analysis, and analytics workflow {index}."
                    ),
                    "location": "India",
                    "tags": ["data analyst"],
                    "normalized_data": {
                        "skills": ["sql", "excel", "analytics", "power bi", "reporting", "dashboarding"],
                        "title_alignment_score": 24.0 - (index * 0.2),
                        "role_fit_score": 18.0 - (index * 0.1),
                        "market_quality_score": 26.0,
                    },
                }
            )

        selected = self.aggregator._select_production_live_jobs(
            query="Data Analyst",
            location="India",
            jobs=jobs,
            limit=12,
        )

        self.assertEqual(len(selected), 12)
        self.assertEqual(
            self.aggregator.last_fetch_diagnostics["selection_debug"]["target_live_count"],
            12,
        )

    def test_dense_india_role_can_fill_capped_fifteen_from_distinct_live_matches(self) -> None:
        jobs = []
        sources = ["jooble", "adzuna", "jobicy", "remotive", "themuse", "greenhouse"]
        for index in range(1, 19):
            jobs.append(
                {
                    "title": f"Data Analyst {index}",
                    "company": f"India Analytics {index}",
                    "source": sources[index % len(sources)],
                    "external_id": f"india-data-{index}",
                    "url": f"https://example.test/india-data/{index}",
                    "description": "Own SQL reporting, Power BI dashboards, Excel analysis, and analytics workflows.",
                    "location": "India",
                    "tags": ["data analyst"],
                    "normalized_data": {
                        "skills": ["sql", "excel", "analytics", "power bi", "reporting", "dashboarding"],
                        "title_alignment_score": 24.0 - (index * 0.1),
                        "role_fit_score": 18.0 - (index * 0.05),
                        "market_quality_score": 28.0,
                    },
                }
            )

        selected = self.aggregator._select_production_live_jobs(
            query="Data Analyst",
            location="India",
            jobs=jobs,
            limit=18,
        )

        self.assertEqual(len(selected), 15)
        debug = self.aggregator.last_fetch_diagnostics["selection_debug"]
        self.assertEqual(debug["target_live_count"], 15)
        self.assertEqual(debug["underfill"]["required_live_floor"], 10)
        self.assertEqual(debug["underfill"]["reason"], "sufficient_live_supply")

    def test_generic_cybersecurity_india_can_fill_capped_fifteen_from_security_matches(self) -> None:
        jobs = []
        titles = [
            "Security Analyst",
            "SOC Analyst",
            "Cybersecurity Analyst",
            "Information Security Analyst",
            "Security Operations Analyst",
            "Vulnerability Analyst",
        ]
        sources = ["jooble", "adzuna", "remotive", "themuse", "jobicy", "greenhouse"]
        for index in range(1, 19):
            jobs.append(
                {
                    "title": f"{titles[index % len(titles)]} {index}",
                    "company": f"India Security {index}",
                    "source": sources[index % len(sources)],
                    "external_id": f"india-security-{index}",
                    "url": f"https://example.test/india-security/{index}",
                    "description": (
                        "Own SOC monitoring, SIEM alerting, IAM reviews, incident response, "
                        "threat detection, vulnerability management, and security operations."
                    ),
                    "location": "India",
                    "tags": ["security analyst", "soc analyst"],
                    "normalized_data": {
                        "skills": [
                            "siem",
                            "iam",
                            "incident response",
                            "threat detection",
                            "vulnerability management",
                            "security operations",
                        ],
                        "title_alignment_score": 24.0 - (index * 0.1),
                        "role_fit_score": 5.0,
                        "market_quality_score": 48.0,
                    },
                }
            )

        selected = self.aggregator._select_production_live_jobs(
            query="cybersecurity",
            location="India",
            jobs=jobs,
            limit=18,
        )

        self.assertEqual(len(selected), 15)
        self.assertTrue(
            all(
                any(token in item["title"].lower() for token in {"cybersecurity", "security", "soc", "vulnerability"})
                for item in selected
            )
        )
        debug = self.aggregator.last_fetch_diagnostics["selection_debug"]
        self.assertEqual(debug["selected_count"], 15)
        self.assertEqual(debug["underfill"]["reason"], "sufficient_live_supply")

    def test_security_aliases_recover_broader_security_family_without_sales_noise(self) -> None:
        safe_titles = [
            "Cloud Security Analyst",
            "Application Security Engineer",
            "Cyber Resilience Architect",
            "Enterprise IAM Software Engineer",
            "Security GRC Lead",
            "Threat Detection Engineer",
            "Vulnerability Management Specialist",
            "Information Security Manager",
            "SOC Analyst",
            "Incident Response Engineer",
            "Product Security Researcher",
            "Privacy Security Engineer",
        ]
        noisy_titles = [
            "Security Account Executive",
            "Product Marketing Manager - Security",
            "Legal Counsel, Privacy",
        ]
        jobs = []
        for index, title in enumerate([*safe_titles, *noisy_titles], start=1):
            jobs.append(
                {
                    "title": title,
                    "company": f"Market Co {index}",
                    "source": "greenhouse",
                    "external_id": f"security-family-{index}",
                    "url": f"https://example.test/security-family/{index}",
                    "description": "Work across IAM, SIEM, incident response, vulnerability management, and security operations.",
                    "location": "Global",
                    "tags": ["security", "cybersecurity"],
                    "normalized_data": {
                        "skills": ["iam", "siem", "incident response", "vulnerability management"],
                        "title_alignment_score": 12.0,
                        "role_fit_score": 5.0,
                        "market_quality_score": 120.0,
                    },
                }
            )

        selected = self.aggregator._select_production_live_jobs(
            query="Security Analyst",
            location="Global",
            jobs=jobs,
            limit=15,
        )

        selected_titles = {item["title"] for item in selected}
        self.assertGreaterEqual(len(selected), 10)
        self.assertFalse(selected_titles & set(noisy_titles))
        self.assertIn("security_recovery_candidates", self.aggregator.last_fetch_diagnostics["selection_debug"])

    def test_dense_india_role_can_overflow_source_cap_after_clean_floor(self) -> None:
        jobs = []
        for index in range(1, 19):
            jobs.append(
                {
                    "title": f"Data Analyst {index}",
                    "company": f"Jooble India {index}",
                    "source": "jooble",
                    "external_id": f"jooble-data-{index}",
                    "url": f"https://example.test/jooble-data/{index}",
                    "description": "Own SQL reporting, Power BI dashboards, Excel analysis, and analytics workflows.",
                    "location": "India",
                    "tags": ["data analyst"],
                    "normalized_data": {
                        "skills": ["sql", "excel", "analytics", "power bi", "reporting", "dashboarding"],
                        "title_alignment_score": 24.0 - (index * 0.1),
                        "role_fit_score": 18.0 - (index * 0.05),
                        "market_quality_score": 28.0,
                    },
                }
            )
        for source in ["remotive", "jobicy", "themuse"]:
            jobs.append(
                {
                    "title": f"Data Analyst {source}",
                    "company": f"Non India {source}",
                    "source": source,
                    "external_id": f"non-india-{source}",
                    "url": f"https://example.test/non-india/{source}",
                    "description": "Own SQL reporting, dashboards, and analytics workflows.",
                    "location": "United States",
                    "tags": ["data analyst"],
                    "normalized_data": {
                        "skills": ["sql", "analytics", "reporting"],
                        "title_alignment_score": 20.0,
                        "role_fit_score": 14.0,
                        "market_quality_score": 25.0,
                    },
                }
            )

        selected = self.aggregator._select_production_live_jobs(
            query="Data Analyst",
            location="India",
            jobs=jobs,
            limit=18,
        )

        self.assertEqual(len(selected), 15)
        debug = self.aggregator.last_fetch_diagnostics["selection_debug"]
        self.assertEqual(debug["selected_sources"], {"jooble": 15})
        self.assertGreaterEqual(debug["india_target_fill_candidates"], 15)

    def test_placeholder_company_names_do_not_collapse_distinct_live_listings(self) -> None:
        jobs = []
        for index in range(1, 16):
            jobs.append(
                {
                    "title": "SQL Developer",
                    "company": "Unknown Company",
                    "source": "jooble",
                    "external_id": f"https://example.test/sql-developer/{index}",
                    "url": f"https://example.test/sql-developer/{index}",
                    "description": "Build SQL queries, database procedures, reporting views, and performance tuning workflows.",
                    "location": "India",
                    "tags": ["sql developer", "database developer"],
                    "normalized_data": {
                        "skills": ["sql", "postgresql", "mysql", "database design", "performance tuning"],
                        "title_alignment_score": 22.0,
                        "role_fit_score": 15.0,
                        "market_quality_score": 28.0,
                    },
                }
            )

        selected = self.aggregator._select_production_live_jobs(
            query="SQL Developer",
            location="India",
            jobs=jobs,
            limit=15,
        )

        self.assertEqual(len(selected), 15)
        debug = self.aggregator.last_fetch_diagnostics["selection_debug"]
        self.assertEqual(debug["selected_sources"], {"jooble": 15})

    def test_trusted_distinct_feed_titles_can_fill_floor_for_database_aliases(self) -> None:
        jobs = []
        for index in range(1, 13):
            jobs.append(
                {
                    "title": "Database Developer",
                    "company": f"Database Co {index % 3}",
                    "source": "jooble",
                    "external_id": f"https://example.test/database-developer/{index}",
                    "url": f"https://example.test/database-developer/{index}",
                    "description": "Build SQL queries, database procedures, reporting views, and performance tuning workflows.",
                    "location": "India",
                    "tags": ["database developer", "sql developer"],
                    "normalized_data": {
                        "skills": ["sql", "postgresql", "mysql", "database design", "performance tuning"],
                        "title_alignment_score": 20.0,
                        "role_fit_score": 12.0,
                        "market_quality_score": 28.0,
                    },
                }
            )

        selected = self.aggregator._select_production_live_jobs(
            query="SQL Developer",
            location="India",
            jobs=jobs,
            limit=15,
        )

        self.assertGreaterEqual(len(selected), 10)
        debug = self.aggregator.last_fetch_diagnostics["selection_debug"]
        self.assertEqual(debug["underfill"]["reason"], "sufficient_live_supply")

    def test_india_search_prefers_india_aligned_live_cards_before_global_fill(self) -> None:
        jobs = []
        for index in range(1, 5):
            jobs.append(
                {
                    "title": f"Data Analyst {index}",
                    "company": f"US Analytics {index}",
                    "source": "greenhouse",
                    "external_id": f"us-{index}",
                    "url": f"https://example.test/us/{index}",
                    "description": "Own SQL reporting, Power BI dashboards, and analytics workflows.",
                    "location": "United States",
                    "tags": ["data analyst"],
                    "normalized_data": {
                        "skills": ["sql", "power bi", "analytics", "reporting"],
                        "title_alignment_score": 22.0,
                        "role_fit_score": 18.0,
                        "market_quality_score": 40.0,
                    },
                }
            )
        for index in range(1, 5):
            jobs.append(
                {
                    "title": f"Data Analyst India {index}",
                    "company": f"India Analytics {index}",
                    "source": "greenhouse",
                    "external_id": f"in-{index}",
                    "url": f"https://example.test/in/{index}",
                    "description": "Own SQL reporting, Power BI dashboards, and analytics workflows.",
                    "location": "Bengaluru, India",
                    "tags": ["data analyst"],
                    "normalized_data": {
                        "skills": ["sql", "power bi", "analytics", "reporting"],
                        "title_alignment_score": 22.0,
                        "role_fit_score": 18.0,
                        "market_quality_score": 34.0,
                    },
                }
            )

        selected = self.aggregator._select_production_live_jobs(
            query="Data Analyst",
            location="India",
            jobs=jobs,
            limit=4,
        )

        self.assertEqual(len(selected), 4)
        self.assertTrue(all("india" in str(item["location"]).lower() for item in selected))
        self.assertEqual(
            self.aggregator.last_fetch_diagnostics["selection_debug"]["india_focused_location"],
            1,
        )

    def test_india_search_rejects_explicit_non_india_remote_markets(self) -> None:
        jobs = [
            {
                "title": "Senior Data Analyst",
                "company": "US Remote Co",
                "source": "remotive",
                "external_id": "us-remote",
                "url": "https://example.test/us-remote",
                "description": "Own SQL reporting, Power BI dashboards, and analytics workflows.",
                "location": "United States",
                "remote": True,
                "tags": ["data analyst"],
                "normalized_data": {
                    "skills": ["sql", "power bi", "analytics", "reporting"],
                    "title_alignment_score": 24.0,
                    "role_fit_score": 18.0,
                    "market_quality_score": 52.0,
                },
            },
            {
                "title": "Data Analyst",
                "company": "Generic Remote",
                "source": "themuse",
                "external_id": "global-role",
                "url": "https://example.test/global-role",
                "description": "Own SQL reporting, dashboards, and analytics workflows.",
                "location": "Flexible / Remote",
                "remote": True,
                "tags": ["data analyst"],
                "normalized_data": {
                    "skills": ["sql", "analytics", "reporting"],
                    "title_alignment_score": 21.0,
                    "role_fit_score": 15.0,
                    "market_quality_score": 27.0,
                },
            },
            {
                "title": "Data Analyst",
                "company": "India Analytics",
                "source": "greenhouse",
                "external_id": "india-role",
                "url": "https://example.test/india-role",
                "description": "Own SQL reporting, Power BI dashboards, and analytics workflows.",
                "location": "Bengaluru, India",
                "remote": False,
                "tags": ["data analyst"],
                "normalized_data": {
                    "skills": ["sql", "power bi", "analytics", "reporting"],
                    "title_alignment_score": 20.0,
                    "role_fit_score": 14.0,
                    "market_quality_score": 28.0,
                },
            },
        ]

        selected = self.aggregator._select_production_live_jobs(
            query="Data Analyst",
            location="India",
            jobs=jobs,
            limit=4,
        )

        self.assertEqual([item["company"] for item in selected], ["India Analytics"])
        self.assertTrue(self.aggregator._is_location_hard_mismatch("India", jobs[0]))
        self.assertTrue(self.aggregator._is_location_hard_mismatch("India", jobs[1]))
        self.assertFalse(self.aggregator._is_location_hard_mismatch("India", jobs[2]))

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
