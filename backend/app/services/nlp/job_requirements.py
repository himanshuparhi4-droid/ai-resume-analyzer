from __future__ import annotations

from collections import Counter, defaultdict
import re

from app.services.nlp.skill_extractor import KNOWN_SKILLS, extract_skill_matches
from app.utils.text import normalize_whitespace, truncate

JOB_REQUIREMENT_PROFILE_VERSION = 4

REQUIRED_HINTS = (
    "must have",
    "required",
    "requirements",
    "qualification",
    "qualifications",
    "proficient in",
    "experience with",
    "hands-on",
    "technical skills",
    "skills:",
    "strong knowledge of",
)
PREFERRED_HINTS = (
    "preferred",
    "nice to have",
    "good to have",
    "bonus",
    "plus",
    "desired",
)
RESPONSIBILITY_HINTS = (
    "responsibilities",
    "you will",
    "in this role",
    "day in the life",
    "duties",
)
SOFT_SKILLS = {"communication", "leadership", "problem solving"}
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
PROXY_SKILL_PATTERNS = {
    "pandas": [r"\bdataframes?\b", r"\bdata wrangling\b", r"\bdata manipulation\b", r"\btabular data\b"],
    "numpy": [r"\bnumerical computing\b", r"\bscientific computing\b", r"\barray programming\b", r"\bmatrix operations?\b"],
    "power bi": [r"\bpowerbi\b", r"\bdax\b", r"\bpower query\b", r"\bpower pivot\b", r"\bpowerpivot\b"],
    "tableau": [r"\btableau desktop\b", r"\btableau prep\b"],
    "excel": [r"\bpivot tables?\b", r"\bvlookups?\b", r"\bxlookups?\b", r"\bspreadsheet modeling\b", r"\badvanced excel\b"],
    "sql": [r"\bstructured query language\b", r"\brelational databases?\b", r"\bcomplex joins?\b", r"\bctes?\b", r"\bstored procedures?\b"],
    "data analysis": [r"\bexploratory analysis\b", r"\broot cause analysis\b", r"\bad hoc analysis\b", r"\bkpi analysis\b", r"\bbusiness insights?\b"],
    "machine learning": [r"\bpredictive modeling\b", r"\bsupervised learning\b", r"\bclassification\b", r"\bregression models?\b"],
    "statistics": [r"\bhypothesis testing\b", r"\ba/b testing\b", r"\bstatistical modeling\b"],
    "reporting": [r"\bbuild reports?\b", r"\bcreate reports?\b", r"\breporting\b", r"\bkpi reporting\b"],
    "dashboarding": [r"\bdashboards?\b", r"\bdashboarding\b"],
    "data visualization": [r"\bvisualizations?\b", r"\bdata visualization\b", r"\bdata visualisation\b"],
    "business intelligence": [r"\bbusiness intelligence\b", r"\bbi tools?\b"],
    "data modeling": [r"\bdata model(?:ing|ling)\b", r"\bdimensional model(?:ing|ling)\b", r"\bstar schema\b"],
    "data warehousing": [r"\bdata warehouse\b", r"\bdata warehousing\b"],
    "snowflake": [r"\bsnowflake\b"],
    "bigquery": [r"\bbigquery\b", r"\bgoogle bigquery\b"],
    "dbt": [r"\bdbt\b", r"\bdata build tool\b"],
    "airflow": [r"\bairflow\b", r"\bapache airflow\b"],
    "looker": [r"\blooker\b", r"\blooker studio\b", r"\bgoogle data studio\b"],
    "regression": [r"\blinear regression\b", r"\blogistic regression\b", r"\bregression models?\b"],
    "hypothesis testing": [r"\bhypothesis testing\b", r"\ba/?b testing\b", r"\bab tests?\b"],
    "forecasting": [r"\bforecasting\b", r"\btime series\b", r"\btime-series\b"],
    "feature engineering": [r"\bfeature engineering\b", r"\bfeature selection\b"],
    "model deployment": [
        r"\bmodel deployment\b",
        r"\bml deployment\b",
        r"\bdeploy(?:ed|ing)? (?:machine learning|ml|predictive)? ?models? (?:to|in) production\b",
        r"\bmodels? in production\b",
        r"\bproductioni[sz]e models?\b",
    ],
    "statsmodels": [r"\bstatsmodels\b"],
}
PROXY_CONTEXT_KEYWORDS = {
    "pandas": {"python", "analytics", "analysis", "machine learning", "modelling", "modeling"},
    "numpy": {"python", "machine learning", "statistics", "analytics", "analysis"},
    "sql": {"database", "warehouse", "analytics", "analysis", "reporting"},
    "excel": {"reporting", "analysis", "finance", "dashboard", "analytics"},
    "reporting": {"analysis", "analytics", "dashboard", "kpi", "business"},
    "dashboarding": {"analysis", "analytics", "reporting", "business", "kpi"},
    "data visualization": {"analysis", "analytics", "dashboard", "business"},
    "business intelligence": {"analysis", "analytics", "dashboard", "reporting", "data"},
    "data modeling": {"warehouse", "analytics", "data", "sql", "etl"},
    "data warehousing": {"warehouse", "analytics", "data", "sql", "etl"},
    "feature engineering": {"python", "machine learning", "model", "models", "prediction", "predictive", "data"},
    "model deployment": {"python", "machine learning", "mlops", "production", "deployment", "models"},
}
IMPLIED_SKILLS = {
    "postgresql": {"sql": 0.72},
    "mysql": {"sql": 0.72},
    "mongodb": {"sql": 0.28},
    "power bi": {"excel": 0.24},
    "tableau": {"data visualization": 0.72, "dashboarding": 0.66},
    "power bi": {"excel": 0.24, "data visualization": 0.74, "dashboarding": 0.68},
    "looker": {"data visualization": 0.72, "dashboarding": 0.66},
    "dbt": {"sql": 0.72, "data modeling": 0.58},
    "airflow": {"etl": 0.66},
    "snowflake": {"sql": 0.64, "data warehousing": 0.72},
    "bigquery": {"sql": 0.64, "data warehousing": 0.72},
}
TITLE_IMPLIED_SKILLS = {
    "data analyst": {"sql": 0.54, "excel": 0.48, "data analysis": 0.72, "reporting": 0.44},
    "business analyst": {"data analysis": 0.66, "reporting": 0.46, "excel": 0.42},
    "reporting analyst": {"reporting": 0.72, "excel": 0.52, "sql": 0.46},
    "bi analyst": {"business intelligence": 0.72, "dashboarding": 0.62, "sql": 0.46},
    "analytics analyst": {"data analysis": 0.68, "dashboarding": 0.52, "statistics": 0.44},
    "data engineer": {"sql": 0.62, "etl": 0.72, "data warehousing": 0.58},
    "data scientist": {
        "python": 0.56,
        "statistics": 0.56,
        "machine learning": 0.72,
        "pandas": 0.44,
        "numpy": 0.42,
        "scikit-learn": 0.46,
    },
    "machine learning engineer": {
        "python": 0.58,
        "machine learning": 0.74,
        "pytorch": 0.52,
        "tensorflow": 0.48,
        "scikit-learn": 0.44,
    },
    "computer vision": {
        "computer vision": 0.76,
        "python": 0.58,
        "machine learning": 0.72,
        "pytorch": 0.64,
        "tensorflow": 0.58,
        "opencv": 0.6,
    },
    "nlp": {
        "nlp": 0.78,
        "python": 0.58,
        "machine learning": 0.72,
        "pytorch": 0.62,
        "tensorflow": 0.56,
    },
    "deep learning": {
        "deep learning": 0.78,
        "python": 0.56,
        "machine learning": 0.74,
        "pytorch": 0.64,
        "tensorflow": 0.6,
    },
    "cloud security": {
        "cloud security": 0.78,
        "iam": 0.66,
        "splunk": 0.52,
        "siem": 0.6,
        "incident response": 0.58,
        "vulnerability management": 0.54,
        "firewall": 0.48,
    },
    "security engineer": {
        "cloud security": 0.6,
        "iam": 0.56,
        "splunk": 0.48,
        "siem": 0.54,
        "incident response": 0.52,
        "vulnerability management": 0.48,
    },
    "site reliability": {
        "monitoring": 0.72,
        "aws": 0.6,
        "kubernetes": 0.58,
        "terraform": 0.54,
        "linux": 0.48,
        "ci/cd": 0.46,
    },
    "devops": {
        "aws": 0.68,
        "kubernetes": 0.6,
        "terraform": 0.58,
        "docker": 0.56,
        "linux": 0.48,
        "ci/cd": 0.5,
        "monitoring": 0.46,
    },
    "salesforce": {
        "salesforce": 0.82,
        "apex": 0.62,
        "crm": 0.6,
        "api": 0.4,
    },
    "oracle": {"sql": 0.56, "erp": 0.5, "api": 0.34},
    "sap": {"erp": 0.62, "sql": 0.38},
    "microsoft dynamics": {"crm": 0.62, "erp": 0.54, "api": 0.34},
    "teacher": {"lesson planning": 0.64, "classroom management": 0.66, "curriculum development": 0.6},
    "lecturer": {"lesson planning": 0.6, "curriculum development": 0.64, "student assessment": 0.52, "pedagogy": 0.48},
    "professor": {"curriculum development": 0.62, "student assessment": 0.5, "pedagogy": 0.46},
    "faculty": {"lesson planning": 0.56, "curriculum development": 0.6, "student assessment": 0.48},
    "painter": {"painting": 0.68, "surface preparation": 0.64, "color matching": 0.6, "spray painting": 0.58},
}
GENERIC_REQUIREMENT_HEADS = (
    "experience with",
    "experience in",
    "proficient in",
    "knowledge of",
    "knowledge in",
    "skilled in",
    "skills:",
    "requirements:",
    "qualifications:",
    "must have",
    "nice to have",
    "good to have",
    "bonus:",
    "tools:",
    "tech stack:",
    "stack:",
    "exposure to",
    "familiarity with",
    "expertise in",
)
GENERIC_ROLE_TERMS = {
    "analyst",
    "engineer",
    "developer",
    "manager",
    "teacher",
    "educator",
    "instructor",
    "lecturer",
    "faculty",
    "professor",
    "painter",
    "designer",
    "specialist",
    "consultant",
    "coordinator",
    "representative",
    "customer success",
    "scientist",
    "administrator",
    "architect",
    "technician",
    "lead",
    "director",
    "head",
    "intern",
}
GENERIC_SKILL_NOISE = {
    "ability to work independently",
    "attention to detail",
    "data and analytics",
    "data",
    "communication skills",
    "customer success",
    "disability",
    "disabilities",
    "education",
    "higher education",
    "content",
    "benefits",
    "data annotation",
    "digital nomad",
    "entry level",
    "full-time",
    "full time",
    "growth",
    "historical business",
    "junior",
    "leader",
    "management",
    "marketing",
    "media",
    "mid level",
    "news",
    "operations",
    "other",
    "part time",
    "problem solving",
    "sales",
    "saas",
    "gender",
    "age",
    "religion",
    "religious belief",
    "national origin",
    "ethnicity",
    "race",
    "veteran status",
    "marital status",
    "equal opportunity",
    "equal employment opportunity",
    "compensation",
    "salary",
    "state",
    "senior level",
    "senior",
    "social media",
    "software engineering",
    "support",
    "technical",
    "team player",
    "test",
    "training",
    "voice",
    "audio tasks",
    "including maps",
    "cross functional collaboration",
    "strong communication skills",
    "strong analytical skills",
    "analytical skills",
    "leadership skills",
    "work independently",
    "stakeholder management",
    "passionate people",
    "policies",
    "procedures",
    "written communication skills in english",
    "actively communicate",
    "exchange costs",
    "retail customer support",
    "messaging platforms",
    "online chat systems",
    "artistic products",
    "as a senior travel agent",
    "we work with highly motivated",
}
GENERIC_PHRASE_SPLIT_RE = re.compile(r"\s*(?:,|;|\||/|\u2022|\u00b7|\band\b|\bor\b)\s*", re.IGNORECASE)
LEADING_REQUIREMENT_RE = re.compile(
    r"^(?:requirements?|qualifications?|skills?|preferred|must have|nice to have|good to have|bonus|experience with|experience in|proficiency in|knowledge of|knowledge in|skilled in|familiarity with|expertise in|tools?|tech stack|stack|exposure to)\s*[:\-]?\s*",
    re.IGNORECASE,
)
NON_SKILL_OPENERS = (
    "a ",
    "an ",
    "ability to",
    "advanced",
    "able to",
    "all ",
    "associated",
    "build ",
    "building ",
    "responsible for",
    "responsibilities include",
    "you will",
    "we are looking for",
    "can ",
    "current",
    "complete",
    "demonstrated",
    "collaborative",
    "must be",
    "should be",
    "strong",
    "excellent",
    "good",
    "you excel",
    "excel in",
    "passionate",
    "actively",
    "written",
    "as a",
    "we work",
)
GENERIC_SINGLETONS = {
    "policies",
    "procedures",
    "people",
    "benefits",
    "operations",
    "management",
    "support",
    "growth",
    "content",
    "communication",
    "english",
}
ALLOWED_UNKNOWN_SINGLETONS = {
    "siem",
    "iam",
    "soc",
    "etl",
    "seo",
    "crm",
    "erp",
    "aws",
    "api",
    "excel",
    "tableau",
    "splunk",
    "looker",
    "snowflake",
    "bigquery",
    "airflow",
    "dbt",
    "terraform",
    "kubernetes",
    "docker",
}
ALLOWED_SKILL_COMPONENTS = {
    "access",
    "cloud",
    "compute",
    "computer",
    "deep",
    "event",
    "identity",
    "incident",
    "language",
    "learning",
    "management",
    "network",
    "processing",
    "reliability",
    "response",
    "resource",
    "security",
    "site",
    "vision",
    "vulnerability",
}
SCIENTIFIC_TITLE_HINTS = (
    "data scientist",
    "machine learning",
    "ml engineer",
    "ml scientist",
    "applied scientist",
    "research scientist",
    "deep learning",
    "computer vision",
    "nlp",
)
SCIENTIFIC_CORE_SKILLS = {
    "python",
    "pandas",
    "numpy",
    "statistics",
    "machine learning",
    "scikit-learn",
    "feature engineering",
    "model deployment",
    "pytorch",
    "tensorflow",
    "nlp",
    "sql",
}
SCIENTIFIC_GENERIC_BUSINESS_SKILLS = {
    "reporting",
    "dashboarding",
    "business intelligence",
    "excel",
}
SPECIALIST_TITLE_PROFILES = {
    "scientific": {
        "patterns": SCIENTIFIC_TITLE_HINTS,
        "core_skills": SCIENTIFIC_CORE_SKILLS | {"computer vision", "deep learning"},
        "generic_skills": SCIENTIFIC_GENERIC_BUSINESS_SKILLS,
    },
    "security": {
        "patterns": ("security", "cybersecurity", "security engineer", "security analyst", "soc", "iam", "cloud security"),
        "core_skills": {"cloud security", "network security", "siem", "splunk", "iam", "incident response", "vulnerability management", "security operations", "penetration testing", "firewall"},
        "generic_skills": {"reporting", "dashboarding", "business intelligence", "excel"},
    },
    "devops": {
        "patterns": ("devops", "site reliability", "sre", "platform engineer", "cloud engineer", "cloud architect"),
        "core_skills": {"aws", "ec2", "lambda", "cloudformation", "docker", "kubernetes", "terraform", "linux", "ci/cd", "monitoring"},
        "generic_skills": {"reporting", "dashboarding", "business intelligence", "excel"},
    },
    "enterprise": {
        "patterns": ("salesforce", "oracle", "sap", "microsoft dynamics", "crm", "erp"),
        "core_skills": {"salesforce", "apex", "crm", "erp", "sql", "api"},
        "generic_skills": {"reporting", "dashboarding", "business intelligence"},
    },
}
LOW_SIGNAL_SENTENCE_HINTS = (
    "equal opportunity",
    "equal employment opportunity",
    "all qualified applicants",
    "regard to race",
    "without regard to",
    "compensation and benefits",
    "employee benefits",
    "benefits package",
    "salary range",
    "country by country reporting",
    "transfer pricing",
)
CANONICAL_SKILL_ALIASES = {
    "analytics": "data analysis",
    "amazon web services": "aws",
    "bi": "business intelligence",
    "customer relationship management": "crm",
    "elastic compute cloud": "ec2",
    "enterprise resource planning": "erp",
    "identity and access management": "iam",
    "natural language processing": "nlp",
    "powerbi": "power bi",
    "power query": "power bi",
    "power pivot": "power bi",
    "google data studio": "looker",
    "security information and event management": "siem",
    "looker studio": "looker",
    "dashboards": "dashboarding",
    "dashboard": "dashboarding",
    "reports": "reporting",
    "report": "reporting",
    "data visualisation": "data visualization",
    "visualisation": "data visualization",
    "visualization": "data visualization",
}


def _matched_specialist_title_profiles(title_lower: str) -> set[str]:
    matched = set()
    for profile_name, config in SPECIALIST_TITLE_PROFILES.items():
        if any(pattern in title_lower for pattern in config["patterns"]):
            matched.add(profile_name)
    return matched


def _normalize_candidate_skill(label: str) -> str:
    cleaned = normalize_whitespace(label).lower().strip(" ,;:.()[]{}")
    cleaned = LEADING_REQUIREMENT_RE.sub("", cleaned)
    cleaned = re.sub(r"^[^a-z0-9+#]+", "", cleaned)
    cleaned = re.sub(r"[^a-z0-9+#/& .-]+$", "", cleaned)
    cleaned = normalize_whitespace(cleaned)
    cleaned = CANONICAL_SKILL_ALIASES.get(cleaned, cleaned)
    return cleaned


def _is_valid_candidate_skill(skill: str) -> bool:
    if not skill or len(skill) < 2:
        return False
    if skill in SOFT_SKILLS or skill in GENERIC_SKILL_NOISE:
        return False
    if skill in GENERIC_ROLE_TERMS:
        return False
    tokens = skill.split()
    if len(tokens) == 1 and skill not in KNOWN_SKILLS and skill not in ALLOWED_UNKNOWN_SINGLETONS:
        return False
    if any(token in GENERIC_SINGLETONS for token in tokens) and skill not in KNOWN_SKILLS:
        return False
    if tokens and tokens[-1] in GENERIC_ROLE_TERMS:
        return False
    if any(skill.startswith(prefix) for prefix in NON_SKILL_OPENERS):
        return False
    if any(token in {"degree", "discipline", "required", "preferred", "others", "current", "experience", "knowledge"} for token in tokens):
        return False
    if any(token in {"age", "gender", "religion", "disability", "benefits", "salary", "compensation", "state", "origin"} for token in tokens):
        return False
    if skill.endswith(" level"):
        return False
    if len(skill.split()) > 5:
        return False
    if re.fullmatch(r"\d+(?:\.\d+)?", skill):
        return False
    if "\u00e2" in skill or "\u0080" in skill or "\u0094" in skill:
        return False
    if not re.search(r"[a-z]", skill):
        return False
    if len(tokens) >= 2 and skill not in KNOWN_SKILLS:
        content_tokens = [token for token in tokens if token not in {"and", "or", "with", "for", "the", "a", "an", "to", "of", "in"}]
        if len(content_tokens) > 3:
            return False
        if not any(token in KNOWN_SKILLS for token in content_tokens):
            if not any(token in {"sql", "aws", "api", "etl", "seo", "crm", "erp", "siem", "iam", "soc"} for token in content_tokens):
                return False
        if any(
            token not in KNOWN_SKILLS
            and token not in ALLOWED_UNKNOWN_SINGLETONS
            and token not in ALLOWED_SKILL_COMPONENTS
            for token in content_tokens
        ):
            return False
    stopword_ratio = sum(1 for token in skill.split() if token in {"and", "or", "with", "for", "the", "a", "an", "to", "of"}) / max(len(skill.split()), 1)
    if stopword_ratio > 0.5:
        return False
    return True


def _split_requirement_candidates(chunk: str) -> list[str]:
    cleaned_chunk = normalize_whitespace(chunk)
    if not cleaned_chunk:
        return []
    parts = GENERIC_PHRASE_SPLIT_RE.split(cleaned_chunk)
    candidates: list[str] = []
    for part in parts:
        normalized = _normalize_candidate_skill(part)
        if _is_valid_candidate_skill(normalized):
            candidates.append(normalized)
    return list(dict.fromkeys(candidates))


def _collect_dynamic_skill_candidates(*, title_text: str, description_text: str, tags: list[str]) -> list[dict]:
    collected: list[dict] = []
    sentences = [segment.strip() for segment in SENTENCE_SPLIT_RE.split(description_text) if segment.strip()]

    for tag in tags:
        normalized = _normalize_candidate_skill(tag)
        if not _is_valid_candidate_skill(normalized):
            continue
        if normalized not in KNOWN_SKILLS and len(normalized.split()) == 1:
            continue
        collected.append(
            {
                "skill": normalized,
                "snippet": tag,
                "mode": "tag",
                "weight": 0.48,
            }
        )

    for sentence in sentences:
        sentence_lower = sentence.lower()
        if any(hint in sentence_lower for hint in LOW_SIGNAL_SENTENCE_HINTS):
            continue
        relevant = any(head in sentence_lower for head in GENERIC_REQUIREMENT_HEADS)
        if not relevant and ":" not in sentence_lower:
            continue
        segments = [sentence]
        for head in GENERIC_REQUIREMENT_HEADS:
            if head in sentence_lower:
                segments.append(sentence[sentence_lower.index(head) + len(head) :])
        if ":" in sentence:
            segments.append(sentence.split(":", 1)[1])

        for segment in segments:
            candidates = _split_requirement_candidates(segment)
            for candidate in candidates:
                weight = 0.3
                if any(marker in sentence_lower for marker in REQUIRED_HINTS):
                    weight += 0.16
                elif any(marker in sentence_lower for marker in PREFERRED_HINTS):
                    weight += 0.08
                if candidate in title_text.lower():
                    weight += 0.08
                snippet = truncate(segment if candidate in segment.lower() else sentence, 200)
                collected.append(
                    {
                        "skill": candidate,
                        "snippet": snippet,
                        "mode": "phrase",
                        "weight": min(weight, 0.62),
                    }
                )

    deduped: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for item in collected:
        key = (item["skill"], item["snippet"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def extract_job_requirement_profile(*, title: str, description: str, tags: list[str] | None = None, source: str = "job") -> dict:
    title_text = normalize_whitespace(title)
    description_text = normalize_whitespace(description)
    full_text = normalize_whitespace(f"{title_text} {description_text}")
    title_lower = title_text.lower()
    normalized_tags = [normalize_whitespace(str(tag)) for tag in (tags or []) if normalize_whitespace(str(tag))]

    weighted_scores: dict[str, float] = {}
    grouped_evidence: defaultdict[str, list[dict]] = defaultdict(list)
    evidence_counts = Counter()

    explicit_matches = extract_skill_matches(full_text, source=source)
    extracted_skills = set()
    scientific_title = any(hint in title_lower for hint in SCIENTIFIC_TITLE_HINTS)
    specialist_profiles = _matched_specialist_title_profiles(title_lower)
    specialist_core_skills = {
        skill
        for profile_name in specialist_profiles
        for skill in SPECIALIST_TITLE_PROFILES[profile_name]["core_skills"]
    }
    specialist_generic_skills = {
        skill
        for profile_name in specialist_profiles
        for skill in SPECIALIST_TITLE_PROFILES[profile_name]["generic_skills"]
    }

    for title_key, implied_skills in TITLE_IMPLIED_SKILLS.items():
        if title_key not in title_lower:
            continue
        for implied_skill, implied_weight in implied_skills.items():
            evidence_counts[implied_skill] += 1
            current = weighted_scores.get(implied_skill, 0.0)
            weighted_scores[implied_skill] = max(current, implied_weight)
            grouped_evidence[implied_skill].append(
                {
                    "skill": implied_skill,
                    "matched_text": title_text,
                    "snippet": title_text,
                    "source": source,
                    "mode": "title-implied",
                }
            )

    for item in _collect_dynamic_skill_candidates(title_text=title_text, description_text=description_text, tags=normalized_tags):
        skill = item["skill"]
        evidence_counts[skill] += 1
        current = weighted_scores.get(skill, 0.0)
        weighted_scores[skill] = max(current, float(item["weight"]))
        if item["snippet"] not in {entry["snippet"] for entry in grouped_evidence[skill]}:
            grouped_evidence[skill].append(
                {
                    "skill": skill,
                    "matched_text": item["snippet"],
                    "snippet": item["snippet"],
                    "source": source,
                    "mode": item["mode"],
                }
            )

    for item in explicit_matches:
        skill = item["skill"]
        if skill in SOFT_SKILLS:
            continue
        if skill in {"reporting", "customer success"} and not any(title_key in title_lower for title_key in {"analyst", "business", "customer", "success", "operations", "manager"}):
            continue

        snippet_lower = item["snippet"].lower()
        if skill == "excel" and any(phrase in snippet_lower for phrase in {"you excel", "excel in ", "must excel", "will excel"}):
            if "microsoft excel" not in snippet_lower and "advanced excel" not in snippet_lower:
                continue
        if any(hint in snippet_lower for hint in LOW_SIGNAL_SENTENCE_HINTS):
            continue
        weight = 0.36
        if skill in title_lower:
            weight += 0.18
        if any(marker in snippet_lower for marker in REQUIRED_HINTS):
            weight += 0.34
        elif any(marker in snippet_lower for marker in PREFERRED_HINTS):
            weight += 0.16
        elif any(marker in snippet_lower for marker in RESPONSIBILITY_HINTS):
            weight += 0.08
        else:
            weight += 0.04

        evidence_counts[skill] += 1
        current = weighted_scores.get(skill, 0.0)
        weighted_scores[skill] = max(current, weight)
        extracted_skills.add(skill)
        if item["snippet"] not in {entry["snippet"] for entry in grouped_evidence[skill]}:
            grouped_evidence[skill].append(item)

    for skill in list(extracted_skills):
        for implied_skill, implied_weight in IMPLIED_SKILLS.get(skill, {}).items():
            evidence_counts[implied_skill] += 1
            current = weighted_scores.get(implied_skill, 0.0)
            weighted_scores[implied_skill] = max(current, implied_weight)
            snippet = grouped_evidence[skill][0]["snippet"] if grouped_evidence.get(skill) else f"Implied by {skill} in the role description."
            if snippet not in {entry["snippet"] for entry in grouped_evidence[implied_skill]}:
                grouped_evidence[implied_skill].append(
                    {
                        "skill": implied_skill,
                        "matched_text": skill,
                        "snippet": snippet,
                        "source": source,
                        "mode": "implied",
                    }
                )

    for sentence in [segment.strip() for segment in SENTENCE_SPLIT_RE.split(full_text) if segment.strip()]:
        sentence_lower = sentence.lower()
        if any(hint in sentence_lower for hint in LOW_SIGNAL_SENTENCE_HINTS):
            continue
        sentence_explicit = {item["skill"] for item in extract_skill_matches(sentence, source=source)}
        for skill, patterns in PROXY_SKILL_PATTERNS.items():
            if skill in SOFT_SKILLS:
                continue
            if not any(re.search(pattern, sentence_lower, re.IGNORECASE) for pattern in patterns):
                continue
            context_keywords = PROXY_CONTEXT_KEYWORDS.get(skill, set())
            if context_keywords and not any(token in sentence_lower for token in context_keywords | sentence_explicit | {title_lower}):
                continue

            weight = 0.24
            if skill in title_lower:
                weight += 0.12
            if any(marker in sentence_lower for marker in REQUIRED_HINTS):
                weight += 0.22
            elif any(marker in sentence_lower for marker in PREFERRED_HINTS):
                weight += 0.12
            elif any(marker in sentence_lower for marker in RESPONSIBILITY_HINTS):
                weight += 0.06
            if context_keywords and any(token in sentence_lower for token in context_keywords):
                weight += 0.08

            evidence_counts[skill] += 1
            current = weighted_scores.get(skill, 0.0)
            weighted_scores[skill] = max(current, weight)
            if sentence not in {entry["snippet"] for entry in grouped_evidence[skill]}:
                grouped_evidence[skill].append(
                    {
                        "skill": skill,
                        "matched_text": sentence,
                        "snippet": sentence,
                        "source": source,
                        "mode": "proxy",
                    }
                )

    normalized_weights: dict[str, float] = {}
    for skill, base_weight in weighted_scores.items():
        bonus = min(0.12, max(0, evidence_counts[skill] - 1) * 0.06)
        adjusted_weight = min(1.0, base_weight + bonus)
        if scientific_title and skill in SCIENTIFIC_CORE_SKILLS:
            adjusted_weight = min(1.0, adjusted_weight + 0.05)
        if (
            scientific_title
            and skill in SCIENTIFIC_GENERIC_BUSINESS_SKILLS
            and evidence_counts[skill] < 2
            and skill not in extracted_skills
        ):
            adjusted_weight = min(adjusted_weight, 0.34)
        if skill in specialist_core_skills:
            adjusted_weight = min(1.0, adjusted_weight + 0.06)
        if (
            specialist_profiles
            and skill in specialist_generic_skills
            and evidence_counts[skill] < 3
            and skill not in title_lower
        ):
            adjusted_weight = min(adjusted_weight, 0.34)
        normalized_weights[skill] = round(adjusted_weight, 2)

    filtered_skills = [
        skill
        for skill, weight in sorted(normalized_weights.items(), key=lambda entry: (-entry[1], entry[0]))
        if weight >= 0.36
    ]

    requirement_quality = round(
        (sum(normalized_weights[skill] for skill in filtered_skills[:6]) / max(len(filtered_skills[:6]), 1)) * 100,
        1,
    ) if filtered_skills else 0.0

    return {
        "skills": filtered_skills,
        "skill_weights": {skill: normalized_weights[skill] for skill in filtered_skills},
        "skill_evidence": [item for skill in filtered_skills for item in grouped_evidence.get(skill, [])[:2]],
        "skill_extraction_mode": "weighted-pattern",
        "requirement_quality": requirement_quality,
        "normalization_version": JOB_REQUIREMENT_PROFILE_VERSION,
    }
