import re
from collections import Counter, defaultdict

from app.services.jobs.taxonomy import role_market_hints, role_primary_hints
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
    "r": [r"\br programming\b", r"\br language\b", r"\br studio\b", r"\brstudio\b"],
    "sas": [r"\bsas\b"],
    "scikit-learn": [r"\bscikit[- ]learn\b", r"\bsklearn\b", r"\bscifiit[- ]learn\b"],
    "matplotlib": [r"\bmatplotlib\b"],
    "seaborn": [r"\bseaborn\b"],
    "statistics": [r"\bstatistics\b", r"\bstatistical analysis\b"],
    "hypothesis testing": [r"\bhypothesis testing\b", r"\ba/?b testing\b", r"\bab tests?\b"],
    "regression": [r"\bregression\b", r"\blinear regression\b", r"\blogistic regression\b"],
    "forecasting": [r"\bforecasting\b", r"\btime series\b", r"\btime-series\b"],
    "data visualization": [r"\bdata visualization\b", r"\bdata visualisation\b", r"\bvisualization\b", r"\bdashboarding\b"],
    "dashboarding": [r"\bdashboarding\b", r"\bdashboards?\b", r"\breport building\b"],
    "reporting": [r"\breporting\b", r"\breports?\b", r"\bkpi reporting\b"],
    "lesson planning": [r"\blesson planning\b", r"\blesson plans?\b"],
    "classroom management": [r"\bclassroom management\b"],
    "curriculum development": [r"\bcurriculum development\b", r"\bcurriculum design\b"],
    "student assessment": [r"\bstudent assessment\b", r"\bassessment design\b", r"\bgrading\b"],
    "differentiated instruction": [r"\bdifferentiated instruction\b", r"\bdifferentiated learning\b"],
    "pedagogy": [r"\bpedagogy\b", r"\bteaching methodology\b"],
    "painting": [r"\bpainting\b", r"\bpainter\b"],
    "surface preparation": [r"\bsurface preparation\b", r"\bsurface prep\b", r"\bsanding\b"],
    "color matching": [r"\bcolor matching\b", r"\bcolour matching\b"],
    "spray painting": [r"\bspray painting\b", r"\bspray painter\b", r"\bpaint spraying\b"],
    "safety compliance": [r"\bsafety compliance\b", r"\bworkplace safety\b", r"\bsafety protocols?\b"],
    "coating": [r"\bcoatings?\b", r"\bprotective coatings?\b"],
    "tensorflow": [r"\btensorflow\b"],
    "pytorch": [r"\bpytorch\b"],
    "nlp": [r"\bnatural language processing\b", r"\bnlp\b"],
    "machine learning": [r"\bmachine learning\b", r"\bml\b"],
    "data analysis": [r"\bdata analysis\b", r"\bdata analytics\b", r"\bdata analyst\b", r"\bexploratory data analysis\b", r"\beda\b"],
    "power bi": [r"\bpower\s*bi\b", r"\bpowerbi\b", r"\bdax\b", r"\bpower query\b", r"\bpower pivot\b", r"\bpowerpivot\b"],
    "tableau": [r"\btableau\b", r"\btableau desktop\b", r"\btableau prep\b"],
    "looker": [r"\blooker\b", r"\blooker studio\b", r"\bgoogle data studio\b"],
    "excel": [r"\bmicrosoft excel\b", r"\bexcel\b", r"\bpivot tables?\b", r"\bvlookups?\b", r"\bxlookups?\b"],
    "snowflake": [r"\bsnowflake\b"],
    "bigquery": [r"\bbigquery\b", r"\bgoogle bigquery\b"],
    "redshift": [r"\bredshift\b", r"\bamazon redshift\b"],
    "dbt": [r"\bdbt\b", r"\bdata build tool\b"],
    "airflow": [r"\bairflow\b", r"\bapache airflow\b"],
    "pyspark": [r"\bpyspark\b"],
    "data modeling": [r"\bdata model(?:ing|ling)\b", r"\bdimensional model(?:ing|ling)\b", r"\bstar schema\b"],
    "data warehousing": [r"\bdata warehouse\b", r"\bdata warehousing\b"],
    "business intelligence": [r"\bbusiness intelligence\b", r"\bbi\b"],
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
    "kafka": [r"\bkafka\b", r"\bapache kafka\b"],
    "go": [r"\bgolang\b", r"\bgo language\b", r"\bgo programming\b"],
    "rust": [r"\brust\b"],
}

COMPILED_SKILL_PATTERNS = {
    skill: [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
    for skill, patterns in SKILL_PATTERNS.items()
}
KNOWN_SKILLS = set(SKILL_PATTERNS.keys())
SOURCE_TRUST_WEIGHTS = {
    "jobicy": 1.0,
    "remotive": 0.94,
    "themuse": 0.92,
    "adzuna": 0.97,
    "usajobs": 0.88,
    "remoteok": 0.72,
    "arbeitnow": 0.62,
    "role-baseline": 0.58,
}
SNIPPET_WINDOW = 84
SENTENCE_PUNCTUATION = ".!?;\n"


def _clean_snippet_edges(snippet: str) -> str:
    cleaned = normalize_whitespace(snippet).strip(" ,;:-")
    cleaned = re.sub(r"^[^\w+(]+", "", cleaned)
    cleaned = cleaned.strip(" ,;:-")
    if not cleaned:
        return ""

    words = cleaned.split()
    # OCR-heavy PDFs sometimes hand us a fragment like "pple" or "ing" as the
    # first token. If the opener looks like a broken lowercase fragment and the
    # next token is also lowercase, drop that fragment so the proof starts on a
    # more natural word boundary.
    if len(words) >= 2:
        first = words[0]
        second = words[1]
        if (
            len(first) <= 4
            and first.isalpha()
            and first.islower()
            and second.isalpha()
            and second.islower()
            and first not in KNOWN_SKILLS
        ):
            cleaned = " ".join(words[1:]).strip(" ,;:-")

    first_token_match = re.search(r"[A-Za-z0-9(+]", cleaned)
    if first_token_match and first_token_match.start() > 0:
        cleaned = cleaned[first_token_match.start():]
    return cleaned.strip(" ,;:-")


def _build_snippet(text: str, start: int, end: int) -> str:
    sentence_left = start
    sentence_right = end

    while sentence_left > 0 and text[sentence_left - 1] not in SENTENCE_PUNCTUATION:
        sentence_left -= 1
    while sentence_right < len(text) and text[sentence_right] not in SENTENCE_PUNCTUATION:
        sentence_right += 1
    if sentence_right < len(text):
        sentence_right += 1

    sentence = _clean_snippet_edges(text[sentence_left:sentence_right])
    if sentence and len(sentence) <= 220:
        return sentence

    left = max(0, start - SNIPPET_WINDOW)
    right = min(len(text), end + SNIPPET_WINDOW)
    while left > 0 and text[left - 1].isalnum():
        left -= 1
    while right < len(text) and text[right].isalnum():
        right += 1

    return _clean_snippet_edges(text[left:right])


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
    description = URL_RE.sub("", description)
    description = re.sub(r"[^a-z0-9\s]", " ", description)
    description = normalize_whitespace(description)
    description_head = " ".join(description.split()[:36])
    return f"{job_item.get('source', 'unknown')}|{company}|{title}|{description_head}"


def infer_skill_frequency(job_items: list[dict], *, role_query: str | None = None) -> list[dict]:
    frequency: defaultdict[str, float] = defaultdict(float)
    mention_count = Counter()
    live_mention_count = Counter()
    company_mentions: defaultdict[str, set[str]] = defaultdict(set)
    source_mentions: defaultdict[str, set[str]] = defaultdict(set)
    signatures = Counter(_job_signature(item) for item in job_items) or Counter()
    denominator = 0.0
    market_hints = role_market_hints(role_query or "") if role_query else set()
    primary_hints = role_primary_hints(role_query or "") if role_query else set()
    live_jobs_present = any(item.get("source") != "role-baseline" for item in job_items)

    for item in job_items:
        normalized_data = item.get("normalized_data", {}) or {}
        skills = list(dict.fromkeys(normalized_data.get("skills", [])))
        if not skills:
            continue

        signature = _job_signature(item)
        duplicate_divisor = max(1, signatures.get(signature, 1))
        source_name = str(item.get("source", "unknown")).lower()
        source_weight = SOURCE_TRUST_WEIGHTS.get(source_name, 0.84)
        if live_jobs_present and source_name == "role-baseline":
            source_weight *= 0.62
        role_fit = float(normalized_data.get("role_fit_score", 0.0))
        listing_quality = float(normalized_data.get("listing_quality_score", 10.0))
        relevance_weight = 0.45 + min(role_fit, 8.0) / 8.0
        quality_weight = 0.65 + min(listing_quality, 20.0) / 28.0
        job_weight = (source_weight * relevance_weight * quality_weight) / duplicate_divisor
        denominator += job_weight

        title_text = normalize_whitespace(str(item.get("title", ""))).lower()
        skill_weights = normalized_data.get("skill_weights", {}) or {}
        evidence_count = Counter(
            evidence.get("skill")
            for evidence in normalized_data.get("skill_evidence", [])
            if evidence.get("skill")
        )
        company = normalize_whitespace(str(item.get("company", ""))).lower() or f"{source_name}:{title_text[:48]}"

        for skill in skills:
            default_weight = 0.82 if item.get("source") != "role-baseline" else 0.7
            base_weight = float(skill_weights.get(skill, default_weight))
            if skill in title_text:
                base_weight = min(1.0, base_weight + 0.12)
            if evidence_count.get(skill, 0) > 1:
                base_weight = min(1.0, base_weight + 0.06)
            frequency[skill] += job_weight * base_weight
            mention_count[skill] += 1
            if source_name != "role-baseline":
                live_mention_count[skill] += 1
            company_mentions[skill].add(company)
            source_mentions[skill].add(source_name)

    result = []
    if denominator <= 0:
        return result

    for skill, weighted_score in sorted(frequency.items(), key=lambda item: item[1], reverse=True):
        share = round((weighted_score / denominator) * 100, 1)
        company_count = len(company_mentions[skill])
        live_count = live_mention_count[skill]
        source_count = len(source_mentions[skill])
        primary_skill = skill in primary_hints
        hinted_skill = skill in market_hints or primary_skill

        if live_jobs_present and live_count == 0 and source_mentions[skill] == {"role-baseline"}:
            if not primary_skill or share < 18.0:
                continue

        if role_query:
            if primary_skill:
                if live_jobs_present and live_count == 0 and share < 18.0:
                    continue
            elif hinted_skill:
                if live_jobs_present and live_count < 2 and company_count < 2 and share < 11.0:
                    continue
            else:
                if skill in KNOWN_SKILLS:
                    if live_count < 2 and company_count < 2 and share < 13.0:
                        continue
                    if mention_count[skill] < 2 and company_count < 2:
                        continue
                else:
                    if mention_count[skill] < 3 or company_count < 2:
                        continue
                    if share < 10.0:
                        continue
        result.append(
            {
                "skill": skill,
                "count": mention_count[skill],
                "live_count": live_count,
                "company_count": company_count,
                "source_count": source_count,
                "share": share,
            }
        )
        if len(result) >= 20:
            break
    return result
