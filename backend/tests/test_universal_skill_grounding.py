from __future__ import annotations

import unittest

from app.services.nlp.skill_grounding import SkillGroundingService
from app.services.nlp.skill_extractor import (
    augment_missing_skills,
    canonical_skill_label,
    extract_skill_evidence,
    extract_skills,
    resume_skill_support_levels,
    split_missing_and_weak_skill_proofs,
)


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

    def test_beginner_dense_roles_require_stronger_proof_for_high_demand_skills(self) -> None:
        jobs = [
            {
                "source": "greenhouse",
                "company": "Alpha",
                "title": "Data Scientist",
                "description": "Requirements: Python, model deployment, feature engineering, and forecasting.",
                "normalized_data": {"skills": ["python", "model deployment", "feature engineering", "forecasting"]},
            },
            {
                "source": "remotive",
                "company": "Beta",
                "title": "Applied Scientist",
                "description": "Requirements: Python, model deployment, experimentation, and forecasting.",
                "normalized_data": {"skills": ["python", "model deployment", "experimentation", "forecasting"]},
            },
            {
                "source": "jobicy",
                "company": "Gamma",
                "title": "Machine Learning Scientist",
                "description": "Requirements: Python, feature engineering, and model deployment.",
                "normalized_data": {"skills": ["python", "feature engineering", "model deployment"]},
            },
        ]

        gaps = augment_missing_skills(
            role_query="Data Scientist",
            resume_skills={"python", "sql", "model deployment"},
            resume_sections={
                "summary": "Aspiring data scientist with Python, SQL, and model deployment exposure.",
                "certifications": "Completed certification modules covering model deployment and machine learning workflows.",
                "skills": "Python, SQL, Model Deployment",
            },
            job_items=jobs,
            existing_missing_skills=[],
            experience_years=0.4,
        )

        gap_names = {item["skill"] for item in gaps}
        self.assertIn("model deployment", gap_names)
        self.assertIn("feature engineering", gap_names)
        self.assertIn("forecasting", gap_names)
        self.assertGreaterEqual(len(gaps), 3)

    def test_summary_and_skills_only_mentions_stay_weak(self) -> None:
        support = resume_skill_support_levels(
            resume_sections={
                "summary": "Aspiring data scientist with machine learning and Python knowledge.",
                "skills": "Python, Machine Learning, SQL",
            },
            skills=["machine learning", "python", "sql"],
        )

        self.assertEqual(support["machine learning"], "weak")
        self.assertEqual(support["python"], "weak")
        self.assertEqual(support["sql"], "weak")

    def test_project_backed_skill_counts_as_stronger_proof(self) -> None:
        support = resume_skill_support_levels(
            resume_sections={
                "summary": "Aspiring data scientist with SQL and dashboard experience.",
                "projects": "Built a sales dashboard in SQL and Power BI for churn analysis.",
                "skills": "SQL, Power BI",
            },
            skills=["sql", "power bi"],
        )

        self.assertEqual(support["sql"], "strong")
        self.assertEqual(support["power bi"], "strong")

    def test_skill_extraction_handles_case_spacing_and_punctuation_variants(self) -> None:
        text = (
            "TECHNICAL SKILLS: SQL, PYTHON, PowerBI, Power_BI, Power B.I., "
            "DataVisualisation, data-visualization, DASH BOARD, dashboards."
        )

        skills = set(extract_skills(text))

        self.assertIn("sql", skills)
        self.assertIn("python", skills)
        self.assertIn("power bi", skills)
        self.assertIn("data visualization", skills)
        self.assertIn("dashboarding", skills)

    def test_skill_evidence_finds_canonical_variants_without_role_hardcoding(self) -> None:
        evidence = extract_skill_evidence(
            "Built KPI dash-board reports in POWER_BI with DataVisualisation for operations.",
            ["power bi", "data visualization", "dashboarding"],
            source="resume:projects",
        )

        evidence_skills = {item["skill"] for item in evidence}
        self.assertEqual({"power bi", "data visualization", "dashboarding"}, evidence_skills)

    def test_skill_canonicalization_collapses_common_tool_variants(self) -> None:
        cases = {
            "PowerBI": "power bi",
            "POWER-B.I.": "power bi",
            "DataVisualisation": "data visualization",
            "dash_board": "dashboarding",
            "Structured Query Language": "sql",
            "CI CD": "ci/cd",
            "Scikit Learn": "scikit-learn",
        }

        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(canonical_skill_label(raw), expected)

    def test_missing_skill_model_does_not_flag_canonical_resume_variants_as_absent(self) -> None:
        jobs = [
            {
                "source": "greenhouse",
                "company": "Alpha",
                "title": "Data Analyst",
                "description": "Requirements: Power BI, SQL dashboards, reporting, and data visualization.",
                "normalized_data": {
                    "skills": ["power bi", "sql", "dashboarding", "reporting", "data visualization"],
                    "skill_weights": {"power bi": 0.9, "sql": 0.86, "dashboarding": 0.8},
                },
            }
        ]

        gaps = augment_missing_skills(
            role_query="Data Analyst",
            resume_skills={"POWER-BI", "SQL", "DataVisualisation", "dash_board"},
            resume_sections={
                "projects": "Built executive dash-board views using POWER_BI and SQL.",
                "skills": "SQL, POWER-BI, DataVisualisation, Dashboard",
            },
            job_items=jobs,
            existing_missing_skills=[{"skill": "PowerBI", "share": 32.0, "signal_source": "live"}],
        )

        gap_names = {item["skill"] for item in gaps}
        self.assertNotIn("power bi", gap_names)

    def test_detected_skills_are_split_out_of_missing_gap_payload(self) -> None:
        missing, weak = split_missing_and_weak_skill_proofs(
            [
                {"skill": "SQL", "share": 50.0, "signal_source": "weak-resume-proof"},
                {"skill": "PowerBI", "share": 45.0, "signal_source": "live"},
                {"skill": "Excel", "share": 30.0, "signal_source": "live"},
            ],
            resume_skills={"sql", "POWER_BI"},
        )

        self.assertEqual({"excel"}, {item["skill"] for item in missing})
        self.assertEqual({"sql", "power bi"}, {item["skill"] for item in weak})
        self.assertTrue(all(item["signal_source"] == "weak-resume-proof" for item in weak))


if __name__ == "__main__":
    unittest.main()
