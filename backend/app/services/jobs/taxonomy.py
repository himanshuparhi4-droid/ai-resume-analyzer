from __future__ import annotations

from difflib import get_close_matches
from functools import lru_cache
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
    "mern developer": "full stack developer",
    "mern stack developer": "full stack developer",
    "mern engineer": "full stack developer",
    "mean stack developer": "full stack developer",
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
    "cybersecurity engineer": "cybersecurity engineer",
    "cyber security engineer": "cybersecurity engineer",
    "security engineer": "cybersecurity engineer",
    "information security engineer": "cybersecurity engineer",
    "cloud security engineer": "cybersecurity engineer",
    "application security engineer": "cybersecurity engineer",
    "product security engineer": "cybersecurity engineer",
    "security operations engineer": "cybersecurity engineer",
    "soc engineer": "cybersecurity engineer",
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
GENERIC_ROLE_MATCH_TOKENS = {
    "engineer",
    "developer",
    "manager",
    "analyst",
    "specialist",
    "executive",
    "representative",
    "associate",
    "assistant",
    "consultant",
    "coordinator",
    "lead",
    "director",
    "architect",
    "technician",
    "officer",
    "administrator",
    "designer",
    "scientist",
    "owner",
    "intern",
}
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
    "cybersecurity engineer": [
        "cybersecurity engineer",
        "security engineer",
        "cloud security engineer",
        "application security engineer",
        "information security engineer",
        "security operations engineer",
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
    "data analyst": ["data analyst", "reporting analyst", "business analyst", "business intelligence analyst", "bi analyst", "analytics"],
    "data scientist": ["data scientist", "machine learning scientist", "applied scientist"],
    "machine learning engineer": ["machine learning engineer", "machine learning", "ai"],
    "data engineer": ["data engineer", "etl engineer", "analytics engineer"],
    "software engineer": ["software engineer", "backend engineer", "python developer"],
    "frontend developer": ["frontend developer", "react developer", "frontend engineer"],
    "full stack developer": ["full stack developer", "mern developer", "mern stack developer", "fullstack developer"],
    "devops engineer": ["aws engineer", "cloud engineer", "devops engineer", "platform engineer", "site reliability engineer"],
    "cybersecurity engineer": ["cybersecurity engineer", "security engineer", "cloud security engineer", "application security engineer", "security operations engineer", "information security engineer"],
    "qa engineer": ["qa engineer", "testing", "quality assurance"],
    "product manager": ["product manager", "associate product manager", "product owner"],
    "ui/ux designer": ["ui ux designer", "ux designer", "product designer"],
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
    "cybersecurity engineer": {"network security", "cloud security", "siem", "splunk", "incident response", "iam", "vulnerability management", "soc", "ids/ips", "firewall", "threat hunting", "security operations", "penetration testing", "python", "linux"},
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
    "cybersecurity engineer": {"cloud security", "siem", "splunk", "incident response", "iam", "vulnerability management", "soc", "firewall", "threat hunting", "security operations"},
    "qa engineer": {"testing", "pytest", "ci/cd", "javascript", "java", "api"},
    "product manager": {"sql", "excel", "data analysis"},
    "ui/ux designer": {"figma", "ui design", "ux design"},
    "teacher": {"lesson planning", "classroom management", "curriculum development", "student assessment", "differentiated instruction", "teaching"},
    "carpenter": {"woodworking", "framing", "blueprint reading", "finish carpentry", "power tools"},
    "painter": {"painting", "surface preparation", "color matching", "spray painting", "coating"},
}
ROLE_TITLE_HINTS = {
    "data analyst": {"data analyst", "reporting analyst", "business intelligence", "business intelligence analyst", "bi analyst", "analytics analyst", "insights analyst", "data analytics"},
    "data scientist": {"scientist", "ml", "machine learning", "applied"},
    "machine learning engineer": {"machine learning", "ml engineer", "ai engineer"},
    "data engineer": {"data engineer", "etl", "analytics engineer"},
    "software engineer": {"software engineer", "software developer", "backend engineer", "backend developer", "application engineer", "python developer"},
    "frontend developer": {"frontend", "front end", "react", "react developer", "ui engineer", "web developer"},
    "full stack developer": {"full stack", "fullstack", "mern", "mern stack", "full stack developer", "full stack engineer", "web developer"},
    "devops engineer": {"devops", "site reliability", "sre", "platform", "cloud", "aws", "cloud engineer", "cloud architect"},
    "cybersecurity engineer": {"cybersecurity engineer", "cyber security engineer", "security engineer", "cloud security", "application security", "information security", "security operations", "soc", "security architect", "security analyst"},
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
    "full stack developer": ("full stack", "fullstack", "mern", "mern stack", "mean stack"),
    "devops engineer": ("aws", "aws engineer", "devops", "site reliability", "sre", "platform engineer", "cloud engineer", "cloud architect"),
    "cybersecurity engineer": ("cybersecurity", "cyber security", "security engineer", "cloud security", "application security", "information security", "security operations", "soc engineer", "security architect"),
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
    "cybersecurity engineer": "security",
    "qa engineer": "software",
    "product manager": "product",
    "ui/ux designer": "design",
    "teacher": "education",
    "carpenter": "trades",
    "painter": "trades",
}
ROLE_NEGATIVE_TITLE_HINTS = {
    "data": {"sales", "customer support", "customer service", "travel", "nurse", "billing", "revenue cycle", "onboarding", "talent", "hr", "recruiter"},
    "software": {"sales", "customer support", "customer service", "travel", "billing", "talent", "hr", "recruiter", "account executive"},
    "security": {
        "sales",
        "customer support",
        "customer service",
        "travel",
        "billing",
        "talent",
        "hr",
        "recruiter",
        "onboarding",
        "tax",
        "design verification",
        "silicon",
        "rtl",
        "semiconductor",
        "chip",
    },
    "product": {"sales representative", "travel", "billing", "hr", "recruiter"},
}
SPARSE_LIVE_MARKET_ROLES = {"teacher", "carpenter", "painter"}
ROLE_ADJACENT_CANONICALS = {
    "frontend developer": {"full stack developer"},
    "full stack developer": {"frontend developer", "software engineer"},
    "software engineer": {"full stack developer"},
    "machine learning engineer": {"data scientist", "data engineer"},
    "data scientist": {"machine learning engineer", "data engineer"},
    "data engineer": {"data scientist", "machine learning engineer"},
}
ROLE_INFERENCE_TOKEN_HINTS = {
    "data analyst": {
        "analyst",
        "analytics",
        "reporting",
        "business",
        "intelligence",
        "insights",
        "dashboard",
        "dashboards",
        "excel",
        "tableau",
        "power",
        "bi",
        "sql",
        "data",
    },
    "data scientist": {
        "scientist",
        "ml",
        "machine",
        "learning",
        "ai",
        "llm",
        "deep",
        "tensorflow",
        "pytorch",
        "numpy",
        "modeling",
    },
    "machine learning engineer": {
        "ml",
        "machine",
        "learning",
        "ai",
        "llm",
        "engineer",
        "pytorch",
        "tensorflow",
        "inference",
        "serving",
    },
    "data engineer": {
        "etl",
        "pipeline",
        "pipelines",
        "warehouse",
        "warehousing",
        "dbt",
        "airflow",
        "spark",
        "databricks",
        "data",
        "engineer",
    },
    "software engineer": {
        "software",
        "backend",
        "developer",
        "engineer",
        "api",
        "microservice",
        "python",
        "java",
        "golang",
        "go",
        "dotnet",
        "django",
        "flask",
        "spring",
        "node",
        "nodejs",
    },
    "frontend developer": {
        "frontend",
        "front",
        "end",
        "react",
        "next",
        "nextjs",
        "angular",
        "vue",
        "ui",
        "ux",
        "web",
        "javascript",
        "typescript",
    },
    "full stack developer": {
        "fullstack",
        "full",
        "stack",
        "mern",
        "mean",
        "node",
        "nodejs",
        "express",
        "mongodb",
        "react",
        "end",
        "to",
    },
    "devops engineer": {
        "aws",
        "devops",
        "sre",
        "site",
        "reliability",
        "cloud",
        "platform",
        "kubernetes",
        "terraform",
        "docker",
        "linux",
        "jenkins",
        "ci",
        "cd",
        "infra",
        "infrastructure",
    },
    "cybersecurity engineer": {
        "cyber",
        "security",
        "soc",
        "siem",
        "splunk",
        "iam",
        "threat",
        "vulnerability",
        "pentest",
        "firewall",
        "incident",
        "response",
        "appsec",
    },
    "qa engineer": {
        "qa",
        "quality",
        "test",
        "testing",
        "automation",
        "selenium",
        "playwright",
        "cypress",
    },
    "product manager": {
        "product",
        "pm",
        "owner",
        "roadmap",
        "strategy",
        "discovery",
    },
    "ui/ux designer": {
        "design",
        "designer",
        "ui",
        "ux",
        "figma",
        "wireframe",
        "prototype",
        "visual",
    },
}


def _clean_role_text(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9+ ]+", " ", str(value).lower()).strip()
    return re.sub(r"\s+", " ", cleaned)


def _tokenize_role_text(value: str) -> set[str]:
    return {
        token
        for token in _clean_role_text(value).split()
        if token and token not in STOPWORDS and token not in GENERIC_ROLE_MATCH_TOKENS and len(token) > 2
    }


@lru_cache(maxsize=None)
def _role_alias_map() -> dict[str, str]:
    alias_map: dict[str, str] = {}
    for canonical in ROLE_FAMILY_CANONICALS:
        phrases = {
            canonical,
            *ROLE_SEARCH_VARIATIONS.get(canonical, []),
            *ROLE_PRODUCTION_VARIATIONS.get(canonical, []),
            *ROLE_TITLE_HINTS.get(canonical, set()),
            *ROLE_KEYWORD_FAMILIES.get(canonical, ()),
        }
        phrases.update({alias for alias, mapped in ROLE_SYNONYMS.items() if mapped == canonical})
        for phrase in phrases:
            cleaned = _clean_role_text(phrase)
            if cleaned:
                alias_map.setdefault(cleaned, canonical)
    return alias_map


def _infer_role_from_fuzzy_alias(cleaned: str) -> str | None:
    if not cleaned:
        return None
    if len(cleaned.split()) != 1:
        return None
    alias_map = _role_alias_map()
    matches = get_close_matches(cleaned, alias_map.keys(), n=1, cutoff=0.82)
    if not matches:
        return None
    return alias_map.get(matches[0])


@lru_cache(maxsize=None)
def _role_phrase_bank(role: str) -> set[str]:
    phrases = {
        role,
        *ROLE_SEARCH_VARIATIONS.get(role, []),
        *ROLE_PRODUCTION_VARIATIONS.get(role, []),
        *ROLE_TITLE_HINTS.get(role, set()),
    }
    phrases.update({alias for alias, canonical in ROLE_SYNONYMS.items() if canonical == role})
    return {_clean_role_text(phrase) for phrase in phrases if _clean_role_text(phrase)}


@lru_cache(maxsize=None)
def _role_signal_bank(role: str) -> set[str]:
    signals = set(_role_phrase_bank(role))
    signals.update(ROLE_INFERENCE_TOKEN_HINTS.get(role, set()))
    for phrase in _role_phrase_bank(role):
        signals.update(_tokenize_role_text(phrase))
    for skill in ROLE_PRIMARY_HINTS.get(role, set()):
        signals.update(_tokenize_role_text(skill))
    return {signal for signal in signals if signal}


def _infer_role_from_cleaned_query(cleaned: str) -> str | None:
    tokens = _tokenize_role_text(cleaned)
    words = set(cleaned.split())
    if not cleaned:
        return None
    mobile_only = bool({"ios", "android", "mobile", "swift", "kotlin", "flutter"} & words) and not bool(
        {"frontend", "react", "web", "javascript", "typescript", "ui"} & words
    )

    if {"mern", "mean"} & words or "fullstack" in words or ({"full", "stack"} <= words):
        if not mobile_only:
            return "full stack developer"
    if {"react", "next", "nextjs", "vue", "angular", "frontend"} & words:
        return "frontend developer"
    if {"aws", "cloud", "devops", "sre", "terraform", "kubernetes"} & words:
        return "devops engineer"
    if {"cyber", "security", "soc", "siem", "splunk", "iam"} & words:
        return "cybersecurity engineer"
    if {"reporting", "analytics", "bi"} & words:
        return "data analyst"
    if {"ml", "llm"} & words and "engineer" in words:
        return "machine learning engineer"
    if {"ml", "llm", "scientist"} & words:
        return "data scientist"
    if {"node", "nodejs", "golang", "go", "java", "python", "backend", "api"} & words and {"developer", "engineer"} & words:
        return "software engineer"
    fuzzy_match = _infer_role_from_fuzzy_alias(cleaned)
    if fuzzy_match:
        return fuzzy_match

    best_role: str | None = None
    best_score = 0.0
    second_best = 0.0
    for role in ROLE_FAMILY_CANONICALS:
        phrase_bank = _role_phrase_bank(role)
        signal_bank = _role_signal_bank(role)
        score = 0.0
        if cleaned in phrase_bank:
            score += 12.0
        for phrase in phrase_bank:
            if len(phrase.split()) > 1 and phrase in cleaned:
                score += 4.5
        token_hits = len(tokens & signal_bank)
        score += token_hits * 1.6
        if mobile_only and role in {"frontend developer", "full stack developer"}:
            score -= 4.0
        elif mobile_only and role == "software engineer":
            score -= 1.5
        if role_domain(role) == "software" and {"developer", "engineer", "frontend", "backend"} & set(cleaned.split()):
            score += 0.8
        if role_domain(role) == "data" and {"analyst", "analytics", "data", "reporting", "bi"} & set(cleaned.split()):
            score += 0.8
        if role_domain(role) == "security" and {"security", "cyber"} & set(cleaned.split()):
            score += 1.1
        if score > best_score:
            second_best = best_score
            best_role = role
            best_score = score
        elif score > second_best:
            second_best = score

    if best_role and best_score >= 2.4 and best_score >= second_best + 0.7:
        return best_role
    return None


def normalize_role(query: str) -> str:
    cleaned = _clean_role_text(query)
    if cleaned in ROLE_SYNONYMS:
        return ROLE_SYNONYMS[cleaned]
    for canonical, keywords in ROLE_KEYWORD_FAMILIES.items():
        if any(keyword in cleaned for keyword in keywords):
            return canonical
    inferred = _infer_role_from_cleaned_query(cleaned)
    if inferred:
        return inferred
    return cleaned


def _meaningful_raw_query(raw_cleaned: str, normalized: str) -> bool:
    raw_tokens = [token for token in raw_cleaned.split() if token and token not in STOPWORDS]
    if not raw_tokens or raw_cleaned == normalized:
        return False
    if all(token in GENERIC_ROLE_MATCH_TOKENS for token in raw_tokens):
        return False
    return True


def _dynamic_query_expansions(raw_cleaned: str, normalized: str) -> list[str]:
    if not _meaningful_raw_query(raw_cleaned, normalized):
        return []

    raw_tokens = [token for token in raw_cleaned.split() if token]
    expansions: list[str] = [raw_cleaned]
    if len(raw_tokens) == 1:
        root = raw_tokens[0]
        if "analyst" in normalized:
            expansions.append(f"{root} analyst")
        if "designer" in normalized:
            expansions.extend([f"{root} designer", f"{root} ux designer"])
        if "manager" in normalized:
            expansions.append(f"{root} manager")
        if "teacher" in normalized:
            expansions.append(f"{root} teacher")
        if "engineer" in normalized or "developer" in normalized:
            expansions.extend([f"{root} engineer", f"{root} developer"])
            if normalized == "frontend developer":
                expansions.extend([f"{root} frontend developer", f"{root} frontend engineer"])
            if normalized == "full stack developer":
                expansions.extend([f"{root} full stack developer", f"{root} fullstack developer"])
            if normalized == "cybersecurity engineer":
                expansions.append(f"{root} security engineer")
            if normalized == "devops engineer":
                expansions.append(f"{root} cloud engineer")
    cleaned_expansions = []
    for item in expansions:
        deduped = " ".join(
            word for index, word in enumerate(item.split()) if index == 0 or word != item.split()[index - 1]
        ).strip()
        if deduped:
            cleaned_expansions.append(deduped)
    return list(dict.fromkeys(cleaned_expansions))


def role_query_tokens(query: str) -> set[str]:
    raw_cleaned = _clean_role_text(query)
    normalized = normalize_role(query)
    return {
        token
        for token in [*raw_cleaned.split(), *normalized.split()]
        if token and token not in STOPWORDS and token not in GENERIC_ROLE_MATCH_TOKENS and len(token) > 2
    }


def query_variations(query: str) -> list[str]:
    normalized = normalize_role(query)
    raw_cleaned = _clean_role_text(query)
    variations = ROLE_SEARCH_VARIATIONS.get(normalized, [normalized])
    if normalized not in variations:
        variations = [normalized, *variations]
    dynamic_variations = _dynamic_query_expansions(raw_cleaned, normalized)
    if dynamic_variations:
        variations = [*dynamic_variations, *variations]
    elif raw_cleaned and raw_cleaned not in variations and normalized not in ROLE_FAMILY_CANONICALS:
        variations = [raw_cleaned, *variations]
    return list(dict.fromkeys(item for item in variations if item))


def production_query_variations(query: str) -> list[str]:
    normalized = normalize_role(query)
    raw_cleaned = _clean_role_text(query)
    variations = ROLE_PRODUCTION_VARIATIONS.get(normalized, [normalized])
    if normalized not in variations:
        variations = [normalized, *variations]
    dynamic_variations = _dynamic_query_expansions(raw_cleaned, normalized)
    if dynamic_variations:
        variations = [*dynamic_variations, *variations]
    elif raw_cleaned and raw_cleaned not in variations and normalized not in ROLE_FAMILY_CANONICALS:
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


def role_negative_title_hints(query: str) -> set[str]:
    domain = role_domain(query)
    if not domain:
        return set()
    return ROLE_NEGATIVE_TITLE_HINTS.get(domain, set())


def canonical_role_alignment(query: str, title: str) -> int:
    normalized_query = normalize_role(query)
    normalized_title = normalize_role(title)
    if not normalized_query or not normalized_title:
        return 0
    if normalized_query == normalized_title:
        return 3
    if normalized_title in ROLE_ADJACENT_CANONICALS.get(normalized_query, set()):
        return 1
    query_domain = role_domain(normalized_query)
    title_domain = role_domain(normalized_title)
    if normalized_title in ROLE_FAMILY_CANONICALS and query_domain and query_domain == title_domain:
        return -2
    return 0


def role_title_alignment_score(query: str, title: str, *, description: str = "", tags: list[str] | tuple[str, ...] = ()) -> float:
    normalized_query = normalize_role(query)
    normalized_title = normalize_role(title)
    if not normalized_query or not normalized_title:
        return 0.0

    raw_query = re.sub(r"[^a-z0-9+ ]+", " ", str(query).lower()).strip()
    raw_query = re.sub(r"\s+", " ", raw_query)
    raw_title = re.sub(r"[^a-z0-9+ ]+", " ", str(title).lower()).strip()
    raw_title = re.sub(r"\s+", " ", raw_title)
    raw_description = re.sub(r"[^a-z0-9+ ]+", " ", str(description).lower()).strip()
    raw_description = re.sub(r"\s+", " ", raw_description)
    raw_tags = " ".join(re.sub(r"[^a-z0-9+ ]+", " ", str(tag).lower()).strip() for tag in tags if str(tag).strip())

    def contains_phrase(haystack: str, needle: str) -> bool:
        cleaned = re.sub(r"[^a-z0-9+ ]+", " ", str(needle).lower()).strip()
        if not cleaned:
            return False
        if " " in cleaned:
            return cleaned in haystack
        return bool(re.search(rf"\b{re.escape(cleaned)}\b", haystack))

    query_tokens = role_query_tokens(query)
    title_hints = role_title_hints(query)
    negative_title_hints = role_negative_title_hints(query)
    query_domain = role_domain(query)
    title_domain = role_domain(title)

    title_hint_hits = sum(1 for hint in title_hints if contains_phrase(raw_title, hint))
    description_hint_hits = sum(1 for hint in title_hints if contains_phrase(raw_description, hint) or contains_phrase(raw_tags, hint))
    core_title_hits = sum(1 for token in query_tokens if contains_phrase(raw_title, token))
    tag_hits = sum(1 for token in query_tokens if contains_phrase(raw_tags, token))

    score = 0.0
    if normalized_query == normalized_title:
        score += 16.0
    elif normalized_query in normalized_title:
        score += 10.5

    if raw_query and contains_phrase(raw_title, raw_query):
        score += 8.0
    elif raw_query and contains_phrase(raw_tags, raw_query):
        score += 2.5
    elif raw_query and contains_phrase(raw_description, raw_query):
        score += 1.5

    score += min(10.0, title_hint_hits * 3.0)
    score += min(4.5, description_hint_hits * 1.0)
    score += core_title_hits * 2.5
    score += tag_hits * 1.0

    if query_domain and title_domain == query_domain:
        score += 3.5
    elif query_domain and title_domain and title_domain != query_domain:
        score -= 8.5

    if negative_title_hints and any(contains_phrase(raw_title, hint) for hint in negative_title_hints):
        if title_hint_hits == 0 and core_title_hits == 0 and normalized_query not in normalized_title:
            score -= 12.0
        else:
            score -= 4.5

    return round(score, 2)


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
    query_tokens = list(dict.fromkeys(role_query_tokens(query)))
    title = normalize_role(item.get("title", ""))
    raw_title = re.sub(r"[^a-z0-9+ ]+", " ", str(item.get("title", "")).lower())
    description = re.sub(r"[^a-z0-9+ ]+", " ", item.get("description", "").lower())
    tags = " ".join(str(tag).lower() for tag in item.get("tags", []))
    normalized_data = item.get("normalized_data", {}) or {}
    market_hints = role_market_hints(query)
    title_hints = role_title_hints(query)
    negative_title_hints = role_negative_title_hints(query)
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
    if negative_title_hints and any(hint in raw_title for hint in negative_title_hints):
        if not any(hint in raw_title for hint in title_hints):
            score *= 0.12
        else:
            score *= 0.45
    title_domain = role_domain(item.get("title", ""))
    query_domain = role_domain(query)
    if query_domain and title_domain and title_domain != query_domain:
        score *= 0.18
    if (
        normalized_query in ROLE_FAMILY_CANONICALS
        and title in ROLE_FAMILY_CANONICALS
        and title != normalized_query
    ):
        if title in ROLE_ADJACENT_CANONICALS.get(normalized_query, set()):
            score *= 0.75
        else:
            score *= 0.4
    return round(score, 2)
