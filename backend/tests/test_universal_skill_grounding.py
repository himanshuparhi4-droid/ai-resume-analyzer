from __future__ import annotations

import unittest

from app.services.nlp.skill_grounding import SkillGroundingService


class UniversalSkillGroundingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service = SkillGroundingService()

    def test_baseline_support_profiles_cover_broad_tech_role_families(self) -> None:
        cases = {
            "Solutions Architect": {"solution architecture", "system design", "integration"},
            "Salesforce Admin": {"salesforce", "apex", "workflow automation"},
            "Technical Support Engineer": {"technical support", "troubleshooting", "ticketing"},
            "Technical Writer": {"technical writing", "api documentation", "openapi"},
            "Engineering Manager": {"leadership", "engineering management", "mentoring"},
        }
        for role, expected_skills in cases.items():
            with self.subTest(role=role):
                support = self.service._baseline_support_profile(role)
                self.assertEqual(support["confidence"], "high")
                self.assertTrue(expected_skills <= set(support["skills"]))

    def test_dense_role_market_sample_blends_when_live_skill_coverage_is_narrow(self) -> None:
        jobs = [
            {
                "source": "remotive",
                "company": "Alpha",
                "title": "Data Scientist",
                "normalized_data": {"skills": ["python", "sql", "machine learning"]},
            },
            {
                "source": "greenhouse",
                "company": "Beta",
                "title": "Applied Scientist",
                "normalized_data": {"skills": ["python", "sql", "statistics"]},
            },
            {
                "source": "jobicy",
                "company": "Gamma",
                "title": "Machine Learning Scientist",
                "normalized_data": {"skills": ["python", "machine learning", "statistics"]},
            },
            {
                "source": "adzuna",
                "company": "Delta",
                "title": "Data Scientist",
                "normalized_data": {"skills": ["python", "sql", "pandas"]},
            },
        ]

        sample = self.service._inspect_market_sample(role_query="Data Scientist", jobs=jobs)

        self.assertTrue(sample["needs_blend"])
        self.assertLess(sample["role_coverage"], 0.5)


if __name__ == "__main__":
    unittest.main()
