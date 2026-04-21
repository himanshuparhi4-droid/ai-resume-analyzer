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

    def test_cybersecurity_engineer_keeps_application_security_title(self) -> None:
        item = {
            "title": "Application Security Engineer",
            "description": "Own SIEM rules, IAM hardening, and secure development lifecycle work.",
            "tags": [],
            "normalized_data": {"skills": ["iam", "siem", "application security"]},
        }
        self.assertTrue(self.aggregator._passes_precise_query_guard("Cybersecurity Engineer", item))

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


if __name__ == "__main__":
    unittest.main()
