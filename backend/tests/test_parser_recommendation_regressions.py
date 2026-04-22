import io
import unittest

from PyPDF2 import PdfWriter

from app.services.analysis.orchestrator import AnalysisOrchestrator
from app.services.nlp.skill_grounding import SkillGroundingService
from app.services.parsers.resume_parser import ResumeParser


class ParserRecommendationRegressionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = ResumeParser()
        self.grounding = SkillGroundingService()
        self.orchestrator = AnalysisOrchestrator(db=None)

    def test_pdf_annotation_links_are_extracted_from_hidden_hyperlinks(self) -> None:
        writer = PdfWriter()
        writer.add_blank_page(width=300, height=300)
        writer.add_uri(0, "https://www.linkedin.com/in/example-profile", [0, 0, 20, 20])
        writer.add_uri(0, "https://github.com/example-user", [25, 0, 45, 20])
        payload = io.BytesIO()
        writer.write(payload)

        links = self.parser._extract_pdf_annotation_links(payload.getvalue())

        self.assertIn("https://www.linkedin.com/in/example-profile", links)
        self.assertIn("https://github.com/example-user", links)

    def test_parse_signals_count_hidden_links_even_when_text_has_no_visible_urls(self) -> None:
        signals = self.parser._analyze_parse_signals(
            text="SUMMARY\nBuilt analytics dashboards for internal operations.",
            normalized="SUMMARY Built analytics dashboards for internal operations.",
            sections={"summary": "Built analytics dashboards for internal operations."},
            grouped_sections={"summary": ["Built analytics dashboards for internal operations."]},
            skills=["sql", "power bi"],
            extract_meta={
                "multi_column_detected": False,
                "page_count": 1,
                "synthetic_skills_section": False,
                "link_urls": [
                    "https://www.linkedin.com/in/example-profile",
                    "https://github.com/example-user",
                ],
            },
        )

        self.assertEqual(signals["contact_link_count"], 2)
        self.assertEqual(signals["portfolio_link_count"], 1)
        self.assertFalse(signals["synthetic_skills_section"])

    def test_seeded_resume_skills_are_kept_when_raw_text_support_exists(self) -> None:
        resume_data = {
            "raw_text": "Microsoft Power BI dashboards and SQL reporting for sales analysis.",
            "sections": {},
            "skills": ["sql", "power bi"],
            "parse_signals": {
                "synthetic_skills_section": True,
            },
        }

        evidence = self.grounding._extract_resume_skill_evidence(resume_data)
        evidence_skills = {item["skill"] for item in evidence}

        self.assertIn("sql", evidence_skills)
        self.assertIn("power bi", evidence_skills)

    def test_weak_resume_proof_uses_proof_language_not_add_or_learn_language(self) -> None:
        resume_data = {
            "skills": ["sql", "power bi"],
            "sections": {"summary": "Data analyst", "experience": "Built dashboards in SQL."},
            "parse_signals": {
                "contact_link_count": 2,
                "synthetic_skills_section": False,
                "section_count": 4,
                "skills_section_word_count": 12,
                "merged_header_count": 1,
                "section_leakage_count": 0,
                "suspicious_token_count": 0,
                "suspicious_url_count": 0,
                "date_range_count": 3,
                "quantified_line_count": 3,
                "multi_column_detected": False,
            },
            "resume_archetype": {"type": "general_resume"},
        }
        payload = {
            "breakdown": {"ats_compliance": 74.0, "resume_quality": 74.0, "experience_match": 62.0},
            "missing_skills": [{"skill": "power bi", "share": 12.0, "signal_source": "weak-resume-proof"}],
        }

        recommendations = self.orchestrator._build_recommendations(payload, resume_data, role_query="Data Analyst")
        titles = [item.title for item in recommendations]

        self.assertIn("Build stronger proof for power bi", titles)
        self.assertNotIn("Add or learn power bi", titles)

    def test_low_noise_recovered_structure_does_not_emit_generic_ats_cleanup_recommendations(self) -> None:
        resume_data = {
            "skills": ["sql", "power bi"],
            "sections": {
                "summary": "Data analyst focused on dashboards and reporting.",
                "experience": "Built internal reporting dashboards and KPI views.",
                "languages": "English",
                "skills": "sql, power bi",
            },
            "parse_signals": {
                "contact_link_count": 2,
                "synthetic_skills_section": True,
                "section_count": 4,
                "skills_section_word_count": 17,
                "merged_header_count": 1,
                "section_leakage_count": 0,
                "suspicious_token_count": 0,
                "suspicious_url_count": 0,
                "date_range_count": 4,
                "quantified_line_count": 4,
                "multi_column_detected": True,
            },
            "resume_archetype": {"type": "general_resume"},
        }
        payload = {
            "breakdown": {"ats_compliance": 68.0, "resume_quality": 73.0, "experience_match": 62.0},
            "missing_skills": [],
        }

        recommendations = self.orchestrator._build_recommendations(payload, resume_data, role_query="Data Analyst")
        titles = {item.title for item in recommendations}

        self.assertNotIn("Use a cleaner one-column resume export", titles)
        self.assertNotIn("Make ATS parsing easier", titles)
        self.assertNotIn("Separate Skills, Experience, and Projects into clean blocks", titles)
        self.assertNotIn("Add clean portfolio or profile links in the header", titles)


if __name__ == "__main__":
    unittest.main()
