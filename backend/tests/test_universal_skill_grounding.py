from __future__ import annotations

import unittest

from app.services.nlp.skill_grounding import SkillGroundingService
from app.services.nlp.skill_extractor import augment_missing_skills


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

    def test_dense_role_missing_gaps_are_backfilled_beyond_single_live_skill(self) -> None:
        jobs = [
            {
                "source": "greenhouse",
                "company": "Alpha",
                "title": "Data Scientist",
                "description": "Requirements: Python, machine learning, feature engineering, model deployment, and forecasting.",
                "normalized_data": {"skills": ["python", "machine learning", "feature engineering", "model deployment", "forecasting"]},
            },
            {
                "source": "jobicy",
                "company": "Beta",
                "title": "Machine Learning Scientist",
                "description": "Requirements: Python, PyTorch, deep learning, and model deployment.",
                "normalized_data": {"skills": ["python", "pytorch", "deep learning", "model deployment"]},
            },
            {
                "source": "remotive",
                "company": "Gamma",
                "title": "Data Scientist",
                "description": "Requirements: Python, SQL, statistics, and forecasting.",
                "normalized_data": {"skills": ["python", "sql", "statistics", "forecasting"]},
            },
        ]

        gaps = augment_missing_skills(
            role_query="Data Scientist",
            resume_skills={"python", "sql", "statistics", "machine learning", "pandas", "numpy", "scikit-learn"},
            job_items=jobs,
            existing_missing_skills=[{"skill": "forecasting", "share": 13.0, "signal_source": "live"}],
        )

        gap_names = {item["skill"] for item in gaps}
        self.assertIn("forecasting", gap_names)
        self.assertIn("feature engineering", gap_names)
        self.assertIn("model deployment", gap_names)
        self.assertGreaterEqual(len(gaps), 3)

    def test_weak_skills_section_only_proof_does_not_hide_live_gap(self) -> None:
        jobs = [
            {
                "source": "greenhouse",
                "company": "Alpha",
                "title": "Data Scientist",
                "description": "Requirements: PyTorch, model deployment, and feature engineering.",
                "normalized_data": {"skills": ["pytorch", "model deployment", "feature engineering"]},
            },
            {
                "source": "remotive",
                "company": "Beta",
                "title": "Applied Scientist",
                "description": "Requirements: PyTorch, feature engineering, and forecasting.",
                "normalized_data": {"skills": ["pytorch", "feature engineering", "forecasting"]},
            },
        ]

        gaps = augment_missing_skills(
            role_query="Data Scientist",
            resume_skills={"python", "sql", "pytorch"},
            resume_sections={"skills": "Python, SQL, PyTorch"},
            job_items=jobs,
            existing_missing_skills=[],
        )

        pytorch_gap = next((item for item in gaps if item["skill"] == "pytorch"), None)
        self.assertIsNotNone(pytorch_gap)
        self.assertEqual(pytorch_gap["signal_source"], "weak-resume-proof")


if __name__ == "__main__":
    unittest.main()
