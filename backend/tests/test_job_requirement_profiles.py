from __future__ import annotations

import unittest

from app.services.nlp.job_requirements import extract_job_requirement_profile


class JobRequirementProfileTest(unittest.TestCase):
    def test_computer_vision_titles_boost_specialist_tools_over_generic_reporting(self) -> None:
        profile = extract_job_requirement_profile(
            title="Computer Vision Engineer",
            description=(
                "Requirements: experience with Python, PyTorch, TensorFlow, OpenCV and machine learning. "
                "Nice to have reporting dashboards for stakeholders."
            ),
            source="job",
        )

        skills = profile["skills"]
        weights = profile["skill_weights"]

        self.assertIn("computer vision", skills)
        self.assertIn("pytorch", skills)
        self.assertIn("tensorflow", skills)
        self.assertIn("opencv", skills)
        self.assertGreater(weights["pytorch"], weights.get("reporting", 0.0))
        self.assertGreater(weights["tensorflow"], weights.get("reporting", 0.0))

    def test_salesforce_titles_keep_enterprise_core_tools(self) -> None:
        profile = extract_job_requirement_profile(
            title="Salesforce Administrator",
            description=(
                "Requirements: Salesforce administration, Apex triggers, CRM workflows and API integrations. "
                "Preferred: reporting for internal stakeholders."
            ),
            source="job",
        )

        skills = profile["skills"]
        weights = profile["skill_weights"]

        self.assertIn("salesforce", skills)
        self.assertIn("apex", skills)
        self.assertIn("crm", skills)
        self.assertGreater(weights["salesforce"], weights.get("reporting", 0.0))
        self.assertGreater(weights["apex"], weights.get("reporting", 0.0))

    def test_data_scientist_profiles_keep_advanced_specialist_tools(self) -> None:
        profile = extract_job_requirement_profile(
            title="Data Scientist",
            description=(
                "Requirements: Python, machine learning, feature engineering, model deployment, forecasting, "
                "TensorFlow, PyTorch, and statistics. Nice to have stakeholder reporting."
            ),
            source="job",
        )

        skills = profile["skills"]
        weights = profile["skill_weights"]

        self.assertIn("feature engineering", skills)
        self.assertIn("model deployment", skills)
        self.assertIn("forecasting", skills)
        self.assertIn("tensorflow", skills)
        self.assertIn("pytorch", skills)
        self.assertGreater(weights["feature engineering"], weights.get("reporting", 0.0))
        self.assertGreater(weights["model deployment"], weights.get("reporting", 0.0))

    def test_technical_writer_profiles_keep_documentation_tools_over_reporting(self) -> None:
        profile = extract_job_requirement_profile(
            title="Technical Writer",
            description=(
                "Requirements: technical writing, API documentation, OpenAPI, Markdown, and developer documentation. "
                "Nice to have internal reporting dashboards."
            ),
            source="job",
        )

        skills = profile["skills"]
        weights = profile["skill_weights"]

        self.assertIn("technical writing", skills)
        self.assertIn("api documentation", skills)
        self.assertIn("openapi", skills)
        self.assertIn("markdown", skills)
        self.assertGreater(weights["technical writing"], weights.get("reporting", 0.0))
        self.assertGreater(weights["api documentation"], weights.get("reporting", 0.0))

    def test_support_titles_keep_troubleshooting_and_ticketing_over_generic_reporting(self) -> None:
        profile = extract_job_requirement_profile(
            title="Application Support Engineer",
            description=(
                "Responsibilities include troubleshooting incidents, root cause analysis, ticketing, and monitoring. "
                "Preferred: build weekly reporting for stakeholders."
            ),
            source="job",
        )

        skills = profile["skills"]
        weights = profile["skill_weights"]

        self.assertIn("troubleshooting", skills)
        self.assertIn("ticketing", skills)
        self.assertIn("incident management", skills)
        self.assertGreater(weights["troubleshooting"], weights.get("reporting", 0.0))
        self.assertGreater(weights["ticketing"], weights.get("reporting", 0.0))

    def test_mobile_titles_keep_platform_skills_over_generic_reporting(self) -> None:
        profile = extract_job_requirement_profile(
            title="Android Developer",
            description=(
                "Requirements: Kotlin, Android SDK, APIs, and mobile app development. "
                "Nice to have stakeholder reporting."
            ),
            source="job",
        )

        skills = profile["skills"]
        weights = profile["skill_weights"]

        self.assertIn("android", skills)
        self.assertIn("kotlin", skills)
        self.assertIn("api", skills)
        self.assertGreater(weights["android"], weights.get("reporting", 0.0))
        self.assertGreater(weights["kotlin"], weights.get("reporting", 0.0))


if __name__ == "__main__":
    unittest.main()
