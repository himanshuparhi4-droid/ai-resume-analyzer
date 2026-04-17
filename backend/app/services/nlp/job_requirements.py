from __future__ import annotations

from collections import Counter, defaultdict
import re

from app.services.nlp.skill_extractor import extract_skill_matches
from app.utils.text import normalize_whitespace

JOB_REQUIREMENT_PROFILE_VERSION = 3

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
    "data scientist": {"python": 0.56, "statistics": 0.56, "machine learning": 0.72},
}


def extract_job_requirement_profile(*, title: str, description: str, source: str = "job") -> dict:
    title_text = normalize_whitespace(title)
    description_text = normalize_whitespace(description)
    full_text = normalize_whitespace(f"{title_text} {description_text}")
    title_lower = title_text.lower()

    weighted_scores: dict[str, float] = {}
    grouped_evidence: defaultdict[str, list[dict]] = defaultdict(list)
    evidence_counts = Counter()

    explicit_matches = extract_skill_matches(full_text, source=source)
    extracted_skills = set()

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

    for item in explicit_matches:
        skill = item["skill"]
        if skill in SOFT_SKILLS:
            continue

        snippet_lower = item["snippet"].lower()
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
        normalized_weights[skill] = round(min(1.0, base_weight + bonus), 2)

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
