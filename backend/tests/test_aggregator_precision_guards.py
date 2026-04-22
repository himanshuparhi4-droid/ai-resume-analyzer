from __future__ import annotations

import unittest

from app.services.jobs.aggregator import JobAggregator


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

    def test_data_analyst_keeps_data_operations_analyst_title(self) -> None:
        item = {
            "title": "Data Operations Analyst",
            "description": "Own SQL dashboards, reporting workflows, and analytics operations.",
            "tags": [],
            "normalized_data": {"skills": ["sql", "reporting", "analytics", "power bi"]},
        }
        self.assertTrue(self.aggregator._passes_precise_query_guard("Data Analyst", item))

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

    def test_india_focused_dense_roles_prioritize_jooble_and_skip_indianapi(self) -> None:
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
        self.assertEqual(plan["primary_sources"][0], "jooble")
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
