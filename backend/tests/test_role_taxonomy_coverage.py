from __future__ import annotations

import unittest

from app.services.jobs.taxonomy import (
    normalize_role,
    provider_query_variations,
    production_query_variations,
    role_domain,
    role_family,
    role_market_hints,
    role_primary_hints,
    role_profile,
    role_title_hints,
)


ROLE_EXPECTATIONS = {
    "Software Engineer": ("software engineer", "software"),
    "Software Developer": ("software engineer", "software"),
    "Application Developer": ("software engineer", "software"),
    "Full Stack Developer": ("full stack developer", "software"),
    "Frontend Developer": ("frontend developer", "software"),
    "Backend Developer": ("software engineer", "software"),
    "Web Developer": ("frontend developer", "software"),
    "Mobile App Developer": ("mobile developer", "software"),
    "Android Developer": ("mobile developer", "software"),
    "iOS Developer": ("mobile developer", "software"),
    "Desktop Application Developer": ("software engineer", "software"),
    "Game Developer": ("software engineer", "software"),
    "Embedded Software Engineer": ("embedded engineer", "software"),
    "Firmware Engineer": ("embedded engineer", "software"),
    "Systems Developer": ("software engineer", "software"),
    "API Developer": ("software engineer", "software"),
    "Data Analyst": ("data analyst", "data"),
    "Business Analyst": ("data analyst", "data"),
    "Data Scientist": ("data scientist", "data"),
    "Data Engineer": ("data engineer", "data"),
    "Analytics Engineer": ("data engineer", "data"),
    "Machine Learning Engineer": ("machine learning engineer", "data"),
    "AI Engineer": ("machine learning engineer", "data"),
    "Deep Learning Engineer": ("machine learning engineer", "data"),
    "NLP Engineer": ("machine learning engineer", "data"),
    "Computer Vision Engineer": ("machine learning engineer", "data"),
    "MLOps Engineer": ("machine learning engineer", "data"),
    "BI Developer": ("data engineer", "data"),
    "Data Architect": ("data engineer", "data"),
    "Statistician": ("data scientist", "data"),
    "Quantitative Analyst": ("data scientist", "data"),
    "DevOps Engineer": ("devops engineer", "software"),
    "Site Reliability Engineer (SRE)": ("devops engineer", "software"),
    "Cloud Engineer": ("devops engineer", "software"),
    "Cloud Architect": ("devops engineer", "software"),
    "Platform Engineer": ("devops engineer", "software"),
    "Infrastructure Engineer": ("devops engineer", "software"),
    "Build and Release Engineer": ("devops engineer", "software"),
    "Systems Engineer": ("devops engineer", "software"),
    "Linux Engineer": ("devops engineer", "software"),
    "Network Engineer": ("devops engineer", "software"),
    "Virtualization Engineer": ("devops engineer", "software"),
    "Storage Engineer": ("devops engineer", "software"),
    "Cybersecurity Analyst": ("cybersecurity engineer", "security"),
    "Security Analyst": ("cybersecurity engineer", "security"),
    "SOC Analyst": ("cybersecurity engineer", "security"),
    "Security Engineer": ("cybersecurity engineer", "security"),
    "Cybersecurity Engineer": ("cybersecurity engineer", "security"),
    "Information Security Analyst": ("cybersecurity engineer", "security"),
    "Penetration Tester": ("cybersecurity engineer", "security"),
    "Ethical Hacker": ("cybersecurity engineer", "security"),
    "Vulnerability Analyst": ("cybersecurity engineer", "security"),
    "Incident Responder": ("cybersecurity engineer", "security"),
    "Digital Forensics Analyst": ("cybersecurity engineer", "security"),
    "Malware Analyst": ("cybersecurity engineer", "security"),
    "Threat Intelligence Analyst": ("cybersecurity engineer", "security"),
    "Security Architect": ("cybersecurity engineer", "security"),
    "IAM Engineer": ("cybersecurity engineer", "security"),
    "GRC Analyst": ("cybersecurity engineer", "security"),
    "Application Security Engineer": ("cybersecurity engineer", "security"),
    "Cloud Security Engineer": ("cybersecurity engineer", "security"),
    "QA Engineer": ("qa engineer", "software"),
    "Test Engineer": ("qa engineer", "software"),
    "Software Test Analyst": ("qa engineer", "software"),
    "Automation Test Engineer": ("qa engineer", "software"),
    "Manual Tester": ("qa engineer", "software"),
    "Performance Test Engineer": ("qa engineer", "software"),
    "SDET": ("qa engineer", "software"),
    "Quality Assurance Analyst": ("qa engineer", "software"),
    "Database Administrator (DBA)": ("database engineer", "data"),
    "Database Developer": ("database engineer", "data"),
    "SQL Developer": ("database engineer", "data"),
    "Data Warehouse Engineer": ("database engineer", "data"),
    "Database Architect": ("database engineer", "data"),
    "Product Manager": ("product manager", "product"),
    "Associate Product Manager": ("product manager", "product"),
    "Project Manager": ("product manager", "product"),
    "Program Manager": ("product manager", "product"),
    "Scrum Master": ("product manager", "product"),
    "Product Analyst": ("data analyst", "data"),
    "UX Designer": ("ui/ux designer", "design"),
    "UI Designer": ("ui/ux designer", "design"),
    "UX Researcher": ("ui/ux designer", "design"),
    "Interaction Designer": ("ui/ux designer", "design"),
    "Technical Business Analyst": ("data analyst", "data"),
    "Solutions Consultant": ("solutions architect", "software"),
    "SAP Consultant": ("enterprise applications engineer", "software"),
    "Salesforce Developer": ("enterprise applications engineer", "software"),
    "Salesforce Admin": ("enterprise applications engineer", "software"),
    "ERP Consultant": ("enterprise applications engineer", "software"),
    "CRM Developer": ("enterprise applications engineer", "software"),
    "Oracle Developer": ("enterprise applications engineer", "software"),
    "Microsoft Dynamics Consultant": ("enterprise applications engineer", "software"),
    "Technical Support Engineer": ("support engineer", "software"),
    "Application Support Engineer": ("support engineer", "software"),
    "IT Support Specialist": ("support engineer", "software"),
    "Help Desk Engineer": ("support engineer", "software"),
    "NOC Engineer": ("support engineer", "software"),
    "Production Support Engineer": ("support engineer", "software"),
    "Operations Analyst": ("data analyst", "data"),
    "Solutions Architect": ("solutions architect", "software"),
    "Software Architect": ("solutions architect", "software"),
    "Enterprise Architect": ("solutions architect", "software"),
    "Technical Architect": ("solutions architect", "software"),
    "Principal Engineer": ("engineering leadership", "software"),
    "Staff Engineer": ("engineering leadership", "software"),
    "Engineering Manager": ("engineering leadership", "software"),
    "CTO": ("engineering leadership", "software"),
    "Blockchain Developer": ("software engineer", "software"),
    "AR/VR Developer": ("software engineer", "software"),
    "Robotics Software Engineer": ("software engineer", "software"),
    "IoT Engineer": ("embedded engineer", "software"),
    "GIS Developer": ("software engineer", "software"),
    "Simulation Engineer": ("software engineer", "software"),
    "Search Engineer": ("software engineer", "software"),
    "Technical Writer": ("technical writer", "product"),
    "Documentation Engineer": ("technical writer", "product"),
    "Developer Advocate": ("technical writer", "product"),
    "Solutions Engineer": ("solutions architect", "software"),
    "Sales Engineer": ("solutions architect", "software"),
}


class RoleTaxonomyCoverageTest(unittest.TestCase):
    def test_broad_role_list_maps_to_supported_canonicals(self) -> None:
        for role, (expected_canonical, expected_domain) in ROLE_EXPECTATIONS.items():
            with self.subTest(role=role):
                canonical = normalize_role(role)
                self.assertEqual(canonical, expected_canonical)
                self.assertEqual(role_domain(canonical), expected_domain)

    def test_unlisted_specialized_roles_infer_domain_and_keep_specialty(self) -> None:
        cases = {
            "Robotics Software Engineer": ("software", {"robotics software engineer", "robotics engineer"}),
            "Cloud Security Architect": ("security", {"cloud security architect", "security engineer"}),
            "Computer Vision Engineer": ("data", {"computer vision engineer", "machine learning engineer"}),
            "Salesforce Admin": ("software", {"salesforce admin", "salesforce developer"}),
        }
        for role, (expected_domain, expected_queries) in cases.items():
            with self.subTest(role=role):
                profile = role_profile(role)
                self.assertEqual(profile.domain, expected_domain)
                queries = set(provider_query_variations(role, "remotive", production=True))
                self.assertTrue(expected_queries & queries)

    def test_jooble_uses_same_universal_query_planner(self) -> None:
        queries = provider_query_variations("Technical Support Engineer", "jooble", production=True)
        self.assertIn("technical support engineer", queries)
        self.assertLessEqual(len(queries), 4)

    def test_specialty_queries_contribute_dynamic_hints(self) -> None:
        salesforce_primary = role_primary_hints("Salesforce Admin")
        self.assertIn("salesforce", salesforce_primary)
        self.assertIn("apex", salesforce_primary)
        self.assertIn("salesforce", role_title_hints("Salesforce Admin"))

        vision_market = role_market_hints("Computer Vision Engineer")
        self.assertIn("computer vision", vision_market)
        self.assertIn("opencv", vision_market)
        self.assertIn("pytorch", vision_market)

        security_primary = role_primary_hints("Cloud Security Architect")
        self.assertIn("cloud security", security_primary)
        self.assertIn("iam", security_primary)
        self.assertIn("splunk", security_primary)

    def test_unlisted_roles_inherit_family_role_and_family_fallback_queries(self) -> None:
        cases = {
            "Release Engineer": ("devops engineer", {"release engineer", "devops engineer"}),
            "Threat Hunter": ("cybersecurity engineer", {"threat hunter", "cybersecurity engineer"}),
            "Customer Data Platform Engineer": ("data engineer", {"customer data platform engineer", "data engineer"}),
            "Prompt Engineer": ("machine learning engineer", {"prompt engineer", "machine learning engineer"}),
            "CRM Administrator": ("enterprise applications engineer", {"crm administrator", "crm developer"}),
            "NLP Research Scientist": ("data scientist", {"nlp research scientist", "data scientist"}),
        }
        for role, (expected_family, expected_queries) in cases.items():
            with self.subTest(role=role):
                self.assertEqual(role_family(role), expected_family)
                queries = set(provider_query_variations(role, "remotive", production=True))
                self.assertTrue(expected_queries & queries)

    def test_inference_beats_loose_keyword_family_for_data_platform_roles(self) -> None:
        profile = role_profile("Customer Data Platform Engineer")
        self.assertEqual(profile.normalized_role, "data engineer")
        self.assertEqual(profile.family_role, "data engineer")
        self.assertEqual(profile.domain, "data")

    def test_explicit_canonical_roles_do_not_drift_into_sibling_production_queries(self) -> None:
        analyst_queries = production_query_variations("Data Analyst")
        self.assertIn("data analyst", analyst_queries)
        self.assertIn("reporting analyst", analyst_queries)
        self.assertNotIn("data scientist", analyst_queries)
        self.assertNotIn("data engineer", analyst_queries)
        self.assertNotIn("data specialist", analyst_queries)

        scientist_queries = production_query_variations("Data Scientist")
        self.assertIn("data scientist", scientist_queries)
        self.assertIn("applied scientist", scientist_queries)
        self.assertNotIn("data analyst", scientist_queries)
        self.assertNotIn("data engineer", scientist_queries)


if __name__ == "__main__":
    unittest.main()
