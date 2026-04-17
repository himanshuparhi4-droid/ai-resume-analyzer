import re
from collections import Counter, defaultdict

from app.utils.text import normalize_whitespace

URL_RE = re.compile(r"https?://\S+")

SKILL_PATTERNS = {
    "python": [r"\bpython\b"],
    "java": [r"\bjava\b"],
    "javascript": [r"\bjavascript\b"],
    "typescript": [r"\btypescript\b"],
    "react": [r"\breact(?:\.js)?\b"],
    "next.js": [r"\bnext(?:\.js)?\b"],
    "node.js": [r"\bnode(?:\.js)?\b"],
    "fastapi": [r"\bfastapi\b"],
    "django": [r"\bdjango\b"],
    "flask": [r"\bflask\b"],
    "sql": [r"\bsql\b", r"\bstructured query language\b", r"\bt-?sql\b", r"\bpl/sql\b"],
    "postgresql": [r"\bpostgres(?:ql)?\b"],
    "mysql": [r"\bmysql\b"],
    "mongodb": [r"\bmongodb\b"],
    "redis": [r"\bredis\b"],
    "docker": [r"\bdocker\b"],
    "kubernetes": [r"\bkubernetes\b"],
    "aws": [r"\baws\b", r"\bamazon web services\b"],
    "azure": [r"\bazure\b"],
    "gcp": [r"\bgcp\b", r"\bgoogle cloud\b"],
    "linux": [r"\blinux\b"],
    "git": [r"\bgit\b", r"\bgithub\b", r"\bgitlab\b"],
    "tailwind": [r"\btailwind\b", r"\btailwindcss\b"],
    "html": [r"\bhtml5?\b"],
    "css": [r"\bcss3?\b"],
    "rest": [r"\brest(?:ful)? api(?:s)?\b", r"\brest\b"],
    "graphql": [r"\bgraphql\b"],
    "pandas": [r"\bpandas\b"],
    "numpy": [r"\bnumpy\b"],
    "scikit-learn": [r"\bscikit[- ]learn\b", r"\bsklearn\b", r"\bscifiit[- ]learn\b"],
    "matplotlib": [r"\bmatplotlib\b"],
    "seaborn": [r"\bseaborn\b"],
    "statistics": [r"\bstatistics\b", r"\bstatistical analysis\b"],
    "tensorflow": [r"\btensorflow\b"],
    "pytorch": [r"\bpytorch\b"],
    "nlp": [r"\bnatural language processing\b", r"\bnlp\b"],
    "machine learning": [r"\bmachine learning\b", r"\bml\b"],
    "data analysis": [r"\bdata analysis\b", r"\bdata analytics\b", r"\bdata analyst\b", r"\bexploratory data analysis\b", r"\beda\b"],
    "power bi": [r"\bpower\s*bi\b", r"\bpowerbi\b", r"\bdax\b", r"\bpower query\b", r"\bpower pivot\b", r"\bpowerpivot\b"],
    "tableau": [r"\btableau\b", r"\btableau desktop\b", r"\btableau prep\b"],
    "excel": [r"\bmicrosoft excel\b", r"\bexcel\b", r"\bpivot tables?\b", r"\bvlookups?\b", r"\bxlookups?\b"],
    "c++": [r"\bc\+\+\b"],
    "c": [r"\bc language\b", r"\bc programming\b", r"\bprogramming in c\b"],
    "spring boot": [r"\bspring boot\b"],
    "microservices": [r"\bmicroservices?\b"],
    "system design": [r"\bsystem design\b"],
    "communication": [r"\bcommunication\b"],
    "leadership": [r"\bleadership\b"],
    "problem solving": [r"\bproblem solving\b", r"\bproblem-solving\b"],
    "figma": [r"\bfigma\b"],
    "ui design": [r"\bui design\b", r"\buser interface\b", r"\binterface design\b"],
    "ux design": [r"\bux design\b", r"\buser experience\b", r"\bux research\b"],
    "seo": [r"\bseo\b", r"\bsearch engine optimization\b"],
    "testing": [r"\btesting\b", r"\btest automation\b"],
    "pytest": [r"\bpytest\b"],
    "ci/cd": [r"\bci/cd\b", r"\bci cd\b", r"\bcontinuous integration\b", r"\bcontinuous delivery\b"],
    "jenkins": [r"\bjenkins\b"],
    "terraform": [r"\bterraform\b"],
    "spark": [r"\bspark\b", r"\bapache spark\b"],
    "hadoop": [r"\bhadoop\b"],
    "etl": [r"\betl\b", r"\bextract transform load\b", r"\bdata pipelines?\b", r"\bpipeline automation\b"],
    "go": [r"\bgolang\b", r"\bgo language\b", r"\bgo programming\b"],
    "rust": [r"\brust\b"],
}

COMPILED_SKILL_PATTERNS = {
    skill: [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
    for skill, patterns in SKILL_PATTERNS.items()
}
KNOWN_SKILLS = set(SKILL_PATTERNS.keys())
SNIPPET_WINDOW = 84


def _build_snippet(text: str, start: int, end: int) -> str:
    snippet = text[max(0, start - SNIPPET_WINDOW): min(len(text), end + SNIPPET_WINDOW)]
    return normalize_whitespace(snippet)


def _exact_skill_pattern(skill: str) -> re.Pattern[str]:
    return re.compile(rf"(?<![A-Za-z0-9]){re.escape(skill)}(?![A-Za-z0-9])", re.IGNORECASE)


def extract_skill_matches(text: str, *, source: str = "document") -> list[dict]:
    normalized = normalize_whitespace(text)
    url_spans = [match.span() for match in URL_RE.finditer(normalized)]
    collected: list[dict] = []

    for skill, patterns in COMPILED_SKILL_PATTERNS.items():
        seen_snippets: set[str] = set()
        for pattern in patterns:
            for match in pattern.finditer(normalized):
                if any(start <= match.start() < end for start, end in url_spans):
                    continue
                snippet = _build_snippet(normalized, match.start(), match.end())
                if snippet in seen_snippets:
                    continue
                seen_snippets.add(snippet)
                collected.append(
                    {
                        "skill": skill,
                        "matched_text": match.group(0),
                        "snippet": snippet,
                        "source": source,
                        "mode": "pattern",
                    }
                )
                if len(seen_snippets) >= 2:
                    break
            if len(seen_snippets) >= 2:
                break

    return sorted(collected, key=lambda item: (item["skill"], item["snippet"]))


def extract_skills(text: str) -> list[str]:
    return sorted({item["skill"] for item in extract_skill_matches(text)})


def extract_skill_evidence(text: str, skills: list[str], *, source: str = "document") -> list[dict]:
    normalized = normalize_whitespace(text)
    requested = {normalize_whitespace(skill).lower() for skill in skills if normalize_whitespace(skill).strip()}
    evidence = [item for item in extract_skill_matches(normalized, source=source) if item["skill"] in requested]
    existing = {(item["skill"], item["snippet"]) for item in evidence}
    missing = requested - {item["skill"] for item in evidence}

    for skill in sorted(missing):
        pattern = _exact_skill_pattern(skill)
        match = pattern.search(normalized)
        if not match:
            continue
        snippet = _build_snippet(normalized, match.start(), match.end())
        key = (skill, snippet)
        if key in existing:
            continue
        evidence.append(
            {
                "skill": skill,
                "matched_text": match.group(0),
                "snippet": snippet,
                "source": source,
                "mode": "literal",
            }
        )

    return sorted(evidence, key=lambda item: (item["skill"], item["snippet"]))


def _job_signature(job_item: dict) -> str:
    title = normalize_whitespace(str(job_item.get("title", ""))).lower()
    company = normalize_whitespace(str(job_item.get("company", ""))).lower()
    description = normalize_whitespace(str(job_item.get("description", ""))).lower()
    return f"{job_item.get('source', 'unknown')}|{company}|{title}|{description[:240]}"


def infer_skill_frequency(job_items: list[dict]) -> list[dict]:
    frequency: defaultdict[str, float] = defaultdict(float)
    mention_count = Counter()
    signatures = Counter(_job_signature(item) for item in job_items) or Counter()
    denominator = 0.0

    for item in job_items:
        normalized_data = item.get("normalized_data", {}) or {}
        skills = list(dict.fromkeys(normalized_data.get("skills", [])))
        if not skills:
            continue

        signature = _job_signature(item)
        duplicate_divisor = max(1, signatures.get(signature, 1))
        source_weight = 1.0 if item.get("source") != "role-baseline" else 0.65
        job_weight = source_weight / duplicate_divisor
        denominator += job_weight

        title_text = normalize_whitespace(str(item.get("title", ""))).lower()
        skill_weights = normalized_data.get("skill_weights", {}) or {}
        evidence_count = Counter(
            evidence.get("skill")
            for evidence in normalized_data.get("skill_evidence", [])
            if evidence.get("skill")
        )

        for skill in skills:
            default_weight = 0.82 if item.get("source") != "role-baseline" else 0.7
            base_weight = float(skill_weights.get(skill, default_weight))
            if skill in title_text:
                base_weight = min(1.0, base_weight + 0.12)
            if evidence_count.get(skill, 0) > 1:
                base_weight = min(1.0, base_weight + 0.06)
            frequency[skill] += job_weight * base_weight
            mention_count[skill] += 1

    result = []
    if denominator <= 0:
        return result

    for skill, weighted_score in sorted(frequency.items(), key=lambda item: item[1], reverse=True)[:15]:
        result.append(
            {
                "skill": skill,
                "count": mention_count[skill],
                "share": round((weighted_score / denominator) * 100, 1),
            }
        )
    return result
