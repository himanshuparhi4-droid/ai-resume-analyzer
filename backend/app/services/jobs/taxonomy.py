from __future__ import annotations

import re

ROLE_SYNONYMS = {
    "software engineer": "software engineer",
    "backend developer": "software engineer",
    "backend engineer": "software engineer",
    "full stack developer": "full stack developer",
    "fullstack developer": "full stack developer",
    "frontend developer": "frontend developer",
    "frontend engineer": "frontend developer",
    "web developer": "frontend developer",
    "python developer": "software engineer",
    "data analyst": "data analyst",
    "data scientist": "data scientist",
    "business analyst": "data analyst",
    "reporting analyst": "data analyst",
    "analytics analyst": "data analyst",
    "bi analyst": "data analyst",
    "data engineer": "data engineer",
    "ml engineer": "machine learning engineer",
    "ai engineer": "machine learning engineer",
    "devops engineer": "devops engineer",
    "aws": "devops engineer",
    "amazon web services": "devops engineer",
    "aws engineer": "devops engineer",
    "aws devops engineer": "devops engineer",
    "cloud engineer": "devops engineer",
    "cloud architect": "devops engineer",
    "cloud infrastructure engineer": "devops engineer",
    "site reliability engineer": "devops engineer",
    "sre": "devops engineer",
    "qa engineer": "qa engineer",
    "test engineer": "qa engineer",
    "product manager": "product manager",
    "ui ux designer": "ui/ux designer",
    "ux designer": "ui/ux designer",
    "ui designer": "ui/ux designer",
    "teacher": "teacher",
    "educator": "teacher",
    "instructor": "teacher",
    "lecturer": "teacher",
    "faculty": "teacher",
    "professor": "teacher",
    "assistant professor": "teacher",
    "associate professor": "teacher",
    "carpenter": "carpenter",
    "finish carpenter": "carpenter",
    "rough carpenter": "carpenter",
    "painter": "painter",
    "industrial painter": "painter",
    "spray painter": "painter",
}
STOPWORDS = {"and", "the", "for", "with", "role", "remote"}
ROLE_SEARCH_VARIATIONS = {
    "data analyst": [
        "data analyst",
        "reporting analyst",
        "business analyst",
        "bi analyst",
        "analytics analyst",
        "product analyst",
        "operations analyst",
    ],
    "data scientist": [
        "data scientist",
        "applied scientist",
        "ml scientist",
        "machine learning scientist",
    ],
    "machine learning engineer": [
        "machine learning engineer",
        "ml engineer",
        "ai engineer",
    ],
    "data engineer": [
        "data engineer",
        "etl engineer",
        "analytics engineer",
    ],
    "software engineer": [
        "software engineer",
        "backend developer",
        "python developer",
        "backend engineer",
    ],
    "frontend developer": [
        "frontend developer",
        "frontend engineer",
        "web developer",
        "react developer",
    ],
    "full stack developer": [
        "full stack developer",
        "fullstack developer",
        "software engineer",
    ],
    "devops engineer": [
        "devops engineer",
        "aws engineer",
        "site reliability engineer",
        "cloud engineer",
        "platform engineer",
        "cloud architect",
    ],
    "qa engineer": [
        "qa engineer",
        "test engineer",
        "automation tester",
        "quality assurance engineer",
    ],
    "product manager": [
        "product manager",
        "associate product manager",
        "product owner",
    ],
    "ui/ux designer": [
        "ui ux designer",
        "ux designer",
        "ui designer",
        "product designer",
    ],
    "teacher": [
        "teacher",
        "educator",
        "instructor",
        "lecturer",
        "faculty",
        "assistant professor",
        "tutor",
    ],
    "carpenter": [
        "carpenter",
        "finish carpenter",
        "rough carpenter",
        "construction carpenter",
    ],
    "painter": [
        "painter",
        "industrial painter",
        "spray painter",
        "coating technician",
    ],
}
ROLE_PRODUCTION_VARIATIONS = {
    "data analyst": ["data analyst", "analytics", "reporting analyst", "business intelligence analyst", "product analyst", "data"],
    "data scientist": ["data scientist", "machine learning", "data"],
    "machine learning engineer": ["machine learning engineer", "machine learning", "ai"],
    "data engineer": ["data engineer", "data", "etl"],
    "software engineer": ["software engineer", "developer", "backend"],
    "frontend developer": ["frontend developer", "frontend", "react"],
    "full stack developer": ["full stack developer", "full stack", "developer"],
    "devops engineer": ["aws engineer", "cloud engineer", "devops engineer", "platform engineer", "site reliability engineer", "aws"],
    "qa engineer": ["qa engineer", "testing", "quality assurance"],
    "product manager": ["product manager", "product", "product owner"],
    "ui/ux designer": ["ui ux designer", "product designer", "design"],
    "teacher": ["lecturer", "teacher", "faculty", "assistant professor", "instructor", "educator"],
    "carpenter": ["carpenter", "finish carpenter", "rough carpenter", "woodworking", "framing"],
    "painter": ["painter", "painting", "surface preparation", "coating", "spray"],
}
ROLE_MARKET_HINTS = {
    "data analyst": {"sql", "excel", "power bi", "tableau", "statistics", "data analysis", "pandas", "python", "reporting", "dashboarding", "data visualization", "business intelligence"},
    "data scientist": {"python", "pandas", "numpy", "machine learning", "scikit-learn", "sql", "statistics"},
    "machine learning engineer": {"python", "machine learning", "scikit-learn", "tensorflow", "pytorch", "sql"},
    "data engineer": {"python", "sql", "spark", "hadoop", "etl", "aws", "postgresql"},
    "software engineer": {"python", "java", "javascript", "sql", "docker", "api", "backend"},
    "frontend developer": {"javascript", "typescript", "react", "next.js", "html", "css", "figma"},
    "full stack developer": {"react", "javascript", "typescript", "node.js", "sql", "api"},
    "devops engineer": {"aws", "amazon web services", "docker", "kubernetes", "terraform", "linux", "ci/cd", "ec2", "lambda", "iam", "cloudformation", "monitoring"},
    "qa engineer": {"testing", "pytest", "ci/cd", "javascript", "java", "api"},
    "product manager": {"data analysis", "sql", "communication", "leadership", "excel"},
    "ui/ux designer": {"figma", "ui design", "ux design", "communication"},
    "teacher": {"lesson planning", "classroom management", "curriculum development", "student assessment", "pedagogy", "differentiated instruction", "teaching", "academic instruction"},
    "carpenter": {"woodworking", "framing", "blueprint reading", "finish carpentry", "power tools", "measuring", "safety compliance"},
    "painter": {"painting", "surface preparation", "color matching", "spray painting", "safety compliance", "coating"},
}
ROLE_PRIMARY_HINTS = {
    "data analyst": {"sql", "excel", "pandas", "power bi", "tableau", "statistics", "python", "reporting", "dashboarding"},
    "data scientist": {"python", "pandas", "numpy", "machine learning", "scikit-learn", "sql", "statistics"},
    "machine learning engineer": {"python", "machine learning", "tensorflow", "pytorch", "scikit-learn"},
    "data engineer": {"python", "sql", "spark", "etl", "postgresql", "aws"},
    "software engineer": {"python", "java", "javascript", "api", "docker", "sql"},
    "frontend developer": {"javascript", "typescript", "react", "next.js", "html", "css"},
    "full stack developer": {"javascript", "typescript", "react", "node.js", "sql", "api"},
    "devops engineer": {"aws", "amazon web services", "docker", "kubernetes", "terraform", "linux", "ci/cd", "ec2", "lambda", "iam"},
    "qa engineer": {"testing", "pytest", "ci/cd", "javascript", "java", "api"},
    "product manager": {"sql", "excel", "data analysis"},
    "ui/ux designer": {"figma", "ui design", "ux design"},
    "teacher": {"lesson planning", "classroom management", "curriculum development", "student assessment", "differentiated instruction", "teaching"},
    "carpenter": {"woodworking", "framing", "blueprint reading", "finish carpentry", "power tools"},
    "painter": {"painting", "surface preparation", "color matching", "spray painting", "coating"},
}
ROLE_TITLE_HINTS = {
    "data analyst": {"data analyst", "analytics", "reporting", "insights", "business intelligence", "bi", "product analyst", "operations analyst", "decision scientist"},
    "data scientist": {"scientist", "ml", "machine learning", "applied"},
    "machine learning engineer": {"machine learning", "ml engineer", "ai engineer"},
    "data engineer": {"data engineer", "etl", "analytics engineer"},
    "software engineer": {"software engineer", "backend", "developer", "engineer"},
    "frontend developer": {"frontend", "react", "web developer", "ui"},
    "full stack developer": {"full stack", "fullstack", "developer", "engineer"},
    "devops engineer": {"devops", "site reliability", "sre", "platform", "cloud", "aws", "cloud engineer", "cloud architect"},
    "qa engineer": {"qa", "quality assurance", "test", "automation"},
    "product manager": {"product manager", "product owner", "product"},
    "ui/ux designer": {"designer", "ui", "ux", "product designer"},
    "teacher": {"teacher", "educator", "instructor", "lecturer", "professor", "faculty", "tutor", "assistant professor"},
    "carpenter": {"carpenter", "finish carpenter", "rough carpenter", "cabinet maker", "woodworker"},
    "painter": {"painter", "painting", "coating", "spray", "finisher"},
}
ROLE_KEYWORD_FAMILIES = {
    "data analyst": ("data analyst", "reporting analyst", "analytics analyst", "bi analyst", "business analyst"),
    "data scientist": ("data scientist", "applied scientist", "ml scientist"),
    "machine learning engineer": ("machine learning engineer", "ml engineer", "ai engineer"),
    "data engineer": ("data engineer", "etl engineer", "analytics engineer"),
    "frontend developer": ("frontend", "react developer", "web developer"),
    "software engineer": ("backend", "api developer", "python developer", "software engineer"),
    "full stack developer": ("full stack", "fullstack"),
    "devops engineer": ("aws", "aws engineer", "devops", "site reliability", "sre", "platform engineer", "cloud engineer", "cloud architect"),
    "qa engineer": ("qa", "quality assurance", "test engineer", "automation tester"),
    "product manager": ("product manager", "product owner", "associate product manager"),
    "ui/ux designer": ("ui ux", "ux designer", "ui designer", "product designer"),
    "teacher": ("teacher", "educator", "instructor", "lecturer", "professor", "assistant professor", "faculty", "tutor"),
    "carpenter": ("carpenter", "finish carpenter", "rough carpenter", "cabinet maker", "woodworker"),
    "painter": ("painter", "painting", "coating", "spray"),
}
ROLE_FAMILY_CANONICALS = set(ROLE_SEARCH_VARIATIONS.keys())
ROLE_DOMAIN_MAP = {
    "data analyst": "data",
    "data scientist": "data",
    "machine learning engineer": "data",
    "data engineer": "data",
    "software engineer": "software",
    "frontend developer": "software",
    "full stack developer": "software",
    "devops engineer": "software",
    "qa engineer": "software",
    "product manager": "product",
    "ui/ux designer": "design",
    "teacher": "education",
    "carpenter": "trades",
    "painter": "trades",
}
SPARSE_LIVE_MARKET_ROLES = {"teacher", "carpenter", "painter"}


def normalize_role(query: str) -> str:
    cleaned = re.sub(r"[^a-z0-9+ ]+", " ", query.lower()).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if cleaned in ROLE_SYNONYMS:
        return ROLE_SYNONYMS[cleaned]
    for canonical, keywords in ROLE_KEYWORD_FAMILIES.items():
        if any(keyword in cleaned for keyword in keywords):
            return canonical
    return cleaned


def query_variations(query: str) -> list[str]:
    normalized = normalize_role(query)
    raw_cleaned = re.sub(r"[^a-z0-9+ ]+", " ", query.lower()).strip()
    raw_cleaned = re.sub(r"\s+", " ", raw_cleaned)
    variations = ROLE_SEARCH_VARIATIONS.get(normalized, [normalized])
    if normalized not in variations:
        variations = [normalized, *variations]
    if raw_cleaned and raw_cleaned not in variations:
        variations = [raw_cleaned, *variations]
    return list(dict.fromkeys(item for item in variations if item))


def production_query_variations(query: str) -> list[str]:
    normalized = normalize_role(query)
    raw_cleaned = re.sub(r"[^a-z0-9+ ]+", " ", query.lower()).strip()
    raw_cleaned = re.sub(r"\s+", " ", raw_cleaned)
    variations = ROLE_PRODUCTION_VARIATIONS.get(normalized, [normalized])
    if normalized not in variations:
        variations = [normalized, *variations]
    if raw_cleaned and raw_cleaned not in variations:
        variations = [raw_cleaned, *variations]
    return list(dict.fromkeys(item for item in variations if item))[:6]


def role_market_hints(query: str) -> set[str]:
    return ROLE_MARKET_HINTS.get(normalize_role(query), set())


def role_primary_hints(query: str) -> set[str]:
    return ROLE_PRIMARY_HINTS.get(normalize_role(query), set())


def role_title_hints(query: str) -> set[str]:
    return ROLE_TITLE_HINTS.get(normalize_role(query), set())


def role_domain(query: str) -> str | None:
    return ROLE_DOMAIN_MAP.get(normalize_role(query))


def is_sparse_live_market_role(query: str) -> bool:
    return normalize_role(query) in SPARSE_LIVE_MARKET_ROLES


def dedupe_key(item: dict) -> str:
    source = str(item.get("source", "unknown")).strip().lower()
    external_id = str(item.get("external_id", "")).strip().lower()
    if external_id:
        return f"{source}::{external_id}"
    url = str(item.get("url", "")).strip().lower()
    if url:
        return f"{source}::{url}"
    title = normalize_role(item.get("title", ""))
    company = re.sub(r"\s+", " ", item.get("company", "").lower()).strip()
    return f"{source}::{title}::{company}"


def role_fit_score(query: str, item: dict) -> float:
    raw_query = re.sub(r"[^a-z0-9+ ]+", " ", str(query).lower()).strip()
    raw_query = re.sub(r"\s+", " ", raw_query)
    normalized_query = normalize_role(query)
    query_tokens = list(
        dict.fromkeys(
            token
            for token in [*raw_query.split(), *normalized_query.split()]
            if token and token not in STOPWORDS
        )
    )
    title = normalize_role(item.get("title", ""))
    raw_title = re.sub(r"[^a-z0-9+ ]+", " ", str(item.get("title", "")).lower())
    description = re.sub(r"[^a-z0-9+ ]+", " ", item.get("description", "").lower())
    tags = " ".join(str(tag).lower() for tag in item.get("tags", []))
    normalized_data = item.get("normalized_data", {}) or {}
    market_hints = role_market_hints(query)
    title_hints = role_title_hints(query)
    extracted_skills = {str(skill).lower() for skill in normalized_data.get("skills", []) or []}

    score = 0.0
    if raw_query and raw_query in raw_title:
        score += 4.0
    elif raw_query and raw_query in description:
        score += 1.75
    elif raw_query and raw_query in tags:
        score += 1.0
    if normalized_query and normalized_query in title:
        score += 6.0
    if normalized_query and normalized_query in description:
        score += 3.0
    for token in query_tokens:
        if token in title.split():
            score += 2.0
        elif re.search(rf"\\b{re.escape(token)}\\b", description):
            score += 0.75
        elif re.search(rf"\\b{re.escape(token)}\\b", tags):
            score += 0.5
    if title_hints and any(hint in raw_title for hint in title_hints):
        score += 2.5
    if market_hints:
        hint_overlap = len(extracted_skills & market_hints)
        score += min(3.0, hint_overlap * 0.75)
    if (
        normalized_query in ROLE_FAMILY_CANONICALS
        and title in ROLE_FAMILY_CANONICALS
        and title != normalized_query
    ):
        score *= 0.6
    return round(score, 2)
