import re
from collections import Counter, defaultdict

from app.services.jobs.taxonomy import role_domain, role_family, role_market_hints, role_primary_hints, role_recommendation_skills
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
    "ec2": [r"\bec2\b", r"\belastic compute cloud\b"],
    "lambda": [r"\blambda\b", r"\baws lambda\b"],
    "cloudformation": [r"\bcloudformation\b", r"\baws cloudformation\b"],
    "azure": [r"\bazure\b"],
    "gcp": [r"\bgcp\b", r"\bgoogle cloud\b"],
    "linux": [r"\blinux\b"],
    "git": [r"\bgit\b", r"\bgithub\b", r"\bgitlab\b"],
    "tailwind": [r"\btailwind\b", r"\btailwindcss\b"],
    "html": [r"\bhtml5?\b"],
    "css": [r"\bcss3?\b"],
    "api": [r"\bapi\b", r"\bapis\b", r"\bapi integrations?\b", r"\bapi development\b"],
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
    "feature engineering": [r"\bfeature engineering\b", r"\bfeature selection\b"],
    "model deployment": [
        r"\bmodel deployment\b",
        r"\bml deployment\b",
        r"\bdeploy(?:ed|ing)? (?:machine learning|ml|predictive)? ?models? (?:to|in) production\b",
        r"\bmodels? in production\b",
        r"\bproductioni[sz]e models?\b",
    ],
    "statsmodels": [r"\bstatsmodels\b"],
    "data visualization": [
        r"\bdata visualization\b",
        r"\bdata visualisation\b",
        r"\bdata\s*visuali[sz]ations?\b",
        r"\bvisuali[sz]ations?\b",
        r"\bdata\s*viz\b",
        r"\bdataviz\b",
        r"\bdashboarding\b",
    ],
    "dashboarding": [r"\bdashboarding\b", r"\bdash\s*boards?\b", r"\bdashboards?\b", r"\breport building\b"],
    "reporting": [r"\breporting\b", r"\breports?\b", r"\bkpi reporting\b"],
    "computer vision": [r"\bcomputer vision\b"],
    "deep learning": [r"\bdeep learning\b"],
    "opencv": [r"\bopen ?cv\b", r"\bopencv\b"],
    "mlops": [r"\bmlops\b", r"\bmachine learning operations\b"],
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
    "tensorflow": [r"\btensorflow\b", r"\bkeras\b"],
    "pytorch": [r"\bpytorch\b", r"\btorch\b"],
    "nlp": [r"\bnatural language processing\b", r"\bnlp\b"],
    "machine learning": [r"\bmachine learning\b", r"\bml\b"],
    "data analysis": [r"\bdata analysis\b", r"\bdata analytics\b", r"\bdata analyst\b", r"\bexploratory data analysis\b", r"\beda\b"],
    "power bi": [
        r"\b(?:microsoft|ms)\s+power\s*bi\b",
        r"\bpower\s*bi(?:\s+(?:desktop|service|reports?))?\b",
        r"\bpower\s*b\s*i\b",
        r"\bpowerbi\b",
        r"\bpbix\b",
        r"\bdax\b",
        r"\bpower query\b",
        r"\bpower pivot\b",
        r"\bpowerpivot\b",
    ],
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
    "android": [r"\bandroid\b"],
    "ios": [r"\bios\b"],
    "swift": [r"\bswift\b"],
    "kotlin": [r"\bkotlin\b"],
    "react native": [r"\breact native\b"],
    "flutter": [r"\bflutter\b"],
    "xcode": [r"\bxcode\b"],
    "android studio": [r"\bandroid studio\b"],
    "embedded": [r"\bembedded\b", r"\bembedded systems?\b"],
    "firmware": [r"\bfirmware\b"],
    "microcontroller": [r"\bmicrocontrollers?\b"],
    "rtos": [r"\brtos\b", r"\breal[- ]time operating system\b", r"\breal[- ]time operating systems\b"],
    "device drivers": [r"\bdevice drivers?\b"],
    "serial communication": [r"\bserial communication\b", r"\buart\b", r"\bi2c\b", r"\bspi\b"],
    "iot": [r"\biot\b", r"\binternet of things\b"],
    "spring boot": [r"\bspring boot\b"],
    "microservices": [r"\bmicroservices?\b"],
    "system design": [r"\bsystem design\b"],
    "solution architecture": [r"\bsolution architecture\b", r"\bsolutions architecture\b"],
    "integration": [r"\bintegrations?\b", r"\bsystems integration\b"],
    "technical consulting": [r"\btechnical consulting\b", r"\bsolution consulting\b"],
    "pre sales": [r"\bpre[- ]sales\b", r"\bpresales\b"],
    "salesforce": [r"\bsalesforce\b", r"\bsalesforce admin(?:istrator)?\b", r"\bsalesforce developer\b"],
    "apex": [r"\bapex\b", r"\bapex classes?\b", r"\bapex triggers?\b"],
    "crm": [r"\bcrm\b", r"\bcustomer relationship management\b"],
    "erp": [r"\berp\b", r"\benterprise resource planning\b"],
    "sap": [r"\bsap\b"],
    "oracle": [r"\boracle\b"],
    "microsoft dynamics": [r"\bmicrosoft dynamics\b", r"\bdynamics 365\b"],
    "workflow automation": [r"\bworkflow automation\b", r"\bprocess automation\b"],
    "technical writing": [r"\btechnical writing\b", r"\btechnical writer\b"],
    "documentation": [r"\bdocumentation\b", r"\bdocs\b"],
    "api documentation": [r"\bapi documentation\b", r"\bdeveloper documentation\b"],
    "openapi": [r"\bopenapi\b", r"\bswagger\b"],
    "markdown": [r"\bmarkdown\b", r"\bmdx\b"],
    "developer relations": [r"\bdeveloper relations\b", r"\bdevrel\b"],
    "cloud security": [r"\bcloud security\b"],
    "network security": [r"\bnetwork security\b"],
    "siem": [r"\bsiem\b", r"\bsecurity information and event management\b"],
    "splunk": [r"\bsplunk\b"],
    "iam": [r"\biam\b", r"\bidentity and access management\b"],
    "incident response": [r"\bincident response\b", r"\bincident handling\b"],
    "vulnerability management": [r"\bvulnerability management\b", r"\bvulnerability assessment\b"],
    "threat hunting": [r"\bthreat hunting\b", r"\bthreat detection\b"],
    "security operations": [r"\bsecurity operations\b", r"\bsoc operations\b"],
    "penetration testing": [r"\bpenetration testing\b", r"\bpen testing\b", r"\bpentest(?:ing)?\b"],
    "firewall": [r"\bfirewalls?\b"],
    "monitoring": [r"\bmonitoring\b", r"\bobservability\b", r"\balerting\b"],
    "technical support": [r"\btechnical support\b", r"\bsupport engineering\b"],
    "troubleshooting": [r"\btroubleshooting\b", r"\btroubleshoot\b"],
    "incident management": [r"\bincident management\b", r"\bincident coordination\b"],
    "ticketing": [r"\bticketing\b", r"\bservice desk\b", r"\bhelp desk\b"],
    "root cause analysis": [r"\broot cause analysis\b", r"\brca\b"],
    "networking": [r"\bnetworking\b", r"\btcp/?ip\b", r"\bdns\b"],
    "sla": [r"\bsla\b", r"\bservice level agreements?\b"],
    "communication": [r"\bcommunication\b"],
    "leadership": [r"\bleadership\b"],
    "engineering management": [r"\bengineering management\b", r"\bmanaging engineering teams?\b"],
    "mentoring": [r"\bmentoring\b", r"\bmentor(?:ing)? engineers?\b"],
    "scalability": [r"\bscalability\b", r"\bscalable systems?\b"],
    "cross functional collaboration": [r"\bcross[- ]functional collaboration\b", r"\bcross functional teams?\b"],
    "problem solving": [r"\bproblem solving\b", r"\bproblem-solving\b"],
    "figma": [r"\bfigma\b"],
    "ui design": [r"\bui design\b", r"\buser interface\b", r"\binterface design\b"],
    "ux design": [r"\bux design\b", r"\buser experience\b", r"\bux research\b"],
    "prototyping": [r"\bprototyping\b", r"\bprototypes?\b"],
    "wireframing": [r"\bwireframing\b", r"\bwireframes?\b"],
    "user research": [r"\buser research\b", r"\bux research\b"],
    "design systems": [r"\bdesign systems?\b"],
    "interaction design": [r"\binteraction design\b"],
    "seo": [r"\bseo\b", r"\bsearch engine optimization\b"],
    "testing": [r"\btesting\b", r"\btest automation\b"],
    "test automation": [r"\btest automation\b", r"\bautomation testing\b"],
    "manual testing": [r"\bmanual testing\b", r"\bmanual tester\b"],
    "performance testing": [r"\bperformance testing\b", r"\bload testing\b", r"\bstress testing\b"],
    "pytest": [r"\bpytest\b"],
    "selenium": [r"\bselenium\b"],
    "playwright": [r"\bplaywright\b"],
    "cypress": [r"\bcypress\b"],
    "ci/cd": [r"\bci/cd\b", r"\bci cd\b", r"\bcontinuous integration\b", r"\bcontinuous delivery\b"],
    "jenkins": [r"\bjenkins\b"],
    "terraform": [r"\bterraform\b"],
    "spark": [r"\bspark\b", r"\bapache spark\b"],
    "hadoop": [r"\bhadoop\b"],
    "etl": [r"\betl\b", r"\bextract transform load\b", r"\bdata pipelines?\b", r"\bpipeline automation\b"],
    "kafka": [r"\bkafka\b", r"\bapache kafka\b"],
    "go": [r"\bgolang\b", r"\bgo language\b", r"\bgo programming\b"],
    "rust": [r"\brust\b"],
    "database design": [r"\bdatabase design\b", r"\bschema design\b"],
    "database administration": [r"\bdatabase administration\b", r"\bdatabase administrator\b", r"\bdba\b"],
    "performance tuning": [r"\bperformance tuning\b", r"\bquery optimization\b"],
    "backup": [r"\bbackups?\b", r"\bdisaster recovery\b"],
}

COMPILED_SKILL_PATTERNS = {
    skill: [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
    for skill, patterns in SKILL_PATTERNS.items()
}
KNOWN_SKILLS = set(SKILL_PATTERNS.keys())
SKILL_LABEL_ALIASES = {
    "advanced excel": "excel",
    "amazon web services": "aws",
    "analytics": "data analysis",
    "business analytics": "data analysis",
    "business intelligence tools": "business intelligence",
    "ci cd": "ci/cd",
    "cicd": "ci/cd",
    "customer relationship management": "crm",
    "dash board": "dashboarding",
    "dash boards": "dashboarding",
    "dashboard": "dashboarding",
    "dashboards": "dashboarding",
    "data analytics": "data analysis",
    "data visualisation": "data visualization",
    "data visualisations": "data visualization",
    "data visualization": "data visualization",
    "data visualizations": "data visualization",
    "data viz": "data visualization",
    "dataviz": "data visualization",
    "elastic compute cloud": "ec2",
    "enterprise resource planning": "erp",
    "google data studio": "looker",
    "github": "git",
    "gitlab": "git",
    "identity and access management": "iam",
    "kpi dashboard": "dashboarding",
    "kpi dashboards": "dashboarding",
    "kpi report": "reporting",
    "kpi reporting": "reporting",
    "kpi reports": "reporting",
    "looker studio": "looker",
    "machine learning operations": "mlops",
    "microsoft excel": "excel",
    "natural language processing": "nlp",
    "pl sql": "sql",
    "plsql": "sql",
    "power b i": "power bi",
    "power b.i": "power bi",
    "power bi": "power bi",
    "power bi desktop": "power bi",
    "power bi report": "power bi",
    "power bi reports": "power bi",
    "power bi service": "power bi",
    "powerbi": "power bi",
    "microsoft power bi": "power bi",
    "ms power bi": "power bi",
    "pbix": "power bi",
    "power pivot": "power bi",
    "power query": "power bi",
    "powerpivot": "power bi",
    "report": "reporting",
    "reports": "reporting",
    "report building": "dashboarding",
    "security information and event management": "siem",
    "structured query language": "sql",
    "t sql": "sql",
    "tableau desktop": "tableau",
    "tableau prep": "tableau",
    "visualisation": "data visualization",
    "visualisations": "data visualization",
    "visualization": "data visualization",
    "visualizations": "data visualization",
}


def _skill_text_fold(value: str) -> str:
    text = normalize_whitespace(str(value or ""))
    if not text:
        return ""
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", text)
    text = re.sub(r"(?<![A-Za-z])(?:[A-Za-z]\.){2,}(?![A-Za-z])", lambda match: match.group(0).replace(".", ""), text)
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[_/|\u2022\u00b7-]+", " ", text)
    text = re.sub(r"[^a-z0-9+#.]+", " ", text)
    text = text.replace(".", " ")
    return normalize_whitespace(text)


def _compact_skill_key(value: str) -> str:
    return re.sub(r"[^a-z0-9+#]+", "", _skill_text_fold(value))


KNOWN_SKILL_COMPACT_MAP = {_compact_skill_key(skill): skill for skill in KNOWN_SKILLS}
SKILL_ALIAS_COMPACT_MAP = {_compact_skill_key(alias): canonical for alias, canonical in SKILL_LABEL_ALIASES.items()}


def canonical_skill_label(value: str) -> str:
    folded = _skill_text_fold(value)
    if not folded:
        return ""
    aliased = SKILL_LABEL_ALIASES.get(folded)
    if aliased:
        return aliased
    if folded in KNOWN_SKILLS:
        return folded
    compact = _compact_skill_key(folded)
    if compact in SKILL_ALIAS_COMPACT_MAP:
        return SKILL_ALIAS_COMPACT_MAP[compact]
    if compact in KNOWN_SKILL_COMPACT_MAP:
        return KNOWN_SKILL_COMPACT_MAP[compact]
    singular = folded[:-1] if folded.endswith("s") else folded
    return SKILL_LABEL_ALIASES.get(singular, singular)


ROLE_GENERIC_SECONDARY_SKILLS = {
    "reporting",
    "dashboarding",
    "data visualization",
    "business intelligence",
    "excel",
}
DENSE_ROLE_GAP_FAMILIES = {
    "software engineer",
    "frontend developer",
    "full stack developer",
    "mobile developer",
    "embedded engineer",
    "data analyst",
    "data scientist",
    "machine learning engineer",
    "data engineer",
    "database engineer",
    "devops engineer",
    "cybersecurity engineer",
    "qa engineer",
    "solutions architect",
    "enterprise applications engineer",
}
RESUME_HIGH_PROOF_SECTIONS = {"experience", "projects", "research", "teaching"}
RESUME_MEDIUM_PROOF_SECTIONS = {"summary", "certifications"}
RESUME_SECTION_SUPPORT_WEIGHTS = {
    "experience": 1.0,
    "projects": 1.0,
    "research": 0.95,
    "teaching": 0.9,
    "summary": 0.45,
    "certifications": 0.5,
    "skills": 0.32,
}
RESUME_PROOF_LEVEL_WEIGHTS = {
    "strong": 1.0,
    "medium": 0.72,
    "weak": 0.38,
    "none": 0.0,
}
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
    tokens = [token for token in _skill_text_fold(skill).split() if token]
    if not tokens:
        return re.compile(r"$^")
    pattern = r"[\s._/\-]*".join(re.escape(token) for token in tokens)
    return re.compile(rf"(?<![A-Za-z0-9]){pattern}(?![A-Za-z0-9])", re.IGNORECASE)


def _skill_matching_text_variants(text: str) -> list[tuple[str, str]]:
    normalized = normalize_whitespace(text)
    normalized_without_urls = URL_RE.sub(" ", normalized)
    variants = [("pattern", normalize_whitespace(normalized_without_urls))]
    folded = _skill_text_fold(normalized_without_urls)
    if folded and folded != variants[0][1]:
        variants.append(("pattern-normalized", folded))
    return [(mode, value) for mode, value in variants if value]


def extract_skill_matches(text: str, *, source: str = "document") -> list[dict]:
    collected: list[dict] = []

    for skill, patterns in COMPILED_SKILL_PATTERNS.items():
        seen_snippets: set[str] = set()
        for mode, searchable_text in _skill_matching_text_variants(text):
            for pattern in patterns:
                for match in pattern.finditer(searchable_text):
                    snippet = _build_snippet(searchable_text, match.start(), match.end())
                    if snippet in seen_snippets:
                        continue
                    seen_snippets.add(snippet)
                    collected.append(
                        {
                            "skill": canonical_skill_label(skill),
                            "matched_text": match.group(0),
                            "snippet": snippet,
                            "source": source,
                            "mode": mode,
                        }
                    )
                    if len(seen_snippets) >= 2:
                        break
                if len(seen_snippets) >= 2:
                    break
            if len(seen_snippets) >= 2:
                break

    return sorted(collected, key=lambda item: (item["skill"], item["snippet"]))


def extract_skills(text: str) -> list[str]:
    return sorted({item["skill"] for item in extract_skill_matches(text)})


def extract_skill_evidence(text: str, skills: list[str], *, source: str = "document") -> list[dict]:
    requested = {canonical_skill_label(skill) for skill in skills if canonical_skill_label(skill)}
    evidence = [item for item in extract_skill_matches(text, source=source) if item["skill"] in requested]
    existing = {(item["skill"], item["snippet"]) for item in evidence}
    missing = requested - {item["skill"] for item in evidence}

    for skill in sorted(missing):
        pattern = _exact_skill_pattern(skill)
        match = None
        searchable_text = ""
        mode = "literal"
        for candidate_mode, candidate_text in _skill_matching_text_variants(text):
            match = pattern.search(candidate_text)
            if match:
                searchable_text = candidate_text
                mode = f"{candidate_mode}-literal"
                break
        if not match:
            continue
        snippet = _build_snippet(searchable_text, match.start(), match.end())
        key = (skill, snippet)
        if key in existing:
            continue
        evidence.append(
            {
                "skill": skill,
                "matched_text": match.group(0),
                "snippet": snippet,
                "source": source,
                "mode": mode,
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
    live_job_count = sum(1 for item in job_items if item.get("source") != "role-baseline")

    for item in job_items:
        normalized_data = item.get("normalized_data", {}) or {}
        skills = list(
            dict.fromkeys(
                canonical_skill_label(skill)
                for skill in normalized_data.get("skills", [])
                if canonical_skill_label(skill)
            )
        )
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
        skill_weights = {
            canonical_skill_label(skill): weight
            for skill, weight in (normalized_data.get("skill_weights", {}) or {}).items()
            if canonical_skill_label(skill)
        }
        evidence_count = Counter(
            canonical_skill_label(evidence.get("skill", ""))
            for evidence in normalized_data.get("skill_evidence", [])
            if canonical_skill_label(evidence.get("skill", ""))
        )
        company = normalize_whitespace(str(item.get("company", ""))).lower() or f"{source_name}:{title_text[:48]}"

        for skill in skills:
            default_weight = 0.82 if item.get("source") != "role-baseline" else 0.7
            base_weight = float(skill_weights.get(skill, default_weight))
            if skill in title_text:
                base_weight = min(1.0, base_weight + 0.12)
            if evidence_count.get(skill, 0) > 1:
                base_weight = min(1.0, base_weight + 0.06)
            primary_skill = skill in primary_hints
            hinted_skill = skill in market_hints or primary_skill
            if role_query:
                if primary_skill:
                    base_weight = min(1.0, base_weight + 0.08)
                elif hinted_skill:
                    base_weight = min(1.0, base_weight + 0.03)
                else:
                    base_weight *= 0.86
                if skill in ROLE_GENERIC_SECONDARY_SKILLS and skill not in primary_hints:
                    base_weight *= 0.72
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
                hinted_dense_keep = live_jobs_present and live_job_count >= 6 and live_count >= 1 and share >= 7.0
                if not hinted_dense_keep:
                    if live_jobs_present and live_count < 2 and company_count < 2 and share < 11.0:
                        continue
                    if skill in ROLE_GENERIC_SECONDARY_SKILLS and live_count < 2 and share < 13.0:
                        continue
            else:
                if skill in KNOWN_SKILLS:
                    broadly_supported = live_jobs_present and live_job_count >= 8 and live_count >= 1 and share >= 8.0
                    if not broadly_supported:
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


def _missing_gap_target(role_query: str | None) -> int:
    if not role_query:
        return 4
    family = role_family(role_query)
    domain = role_domain(role_query)
    if family in {"data scientist", "machine learning engineer", "data engineer", "data analyst"}:
        return 6
    if family in DENSE_ROLE_GAP_FAMILIES or domain in {"data", "software", "security"}:
        return 5
    return 4


def _live_gap_evidence(job_items: list[dict], skill: str) -> list[dict[str, str]]:
    evidence_items: list[dict[str, str]] = []
    seen_company_skill: set[str] = set()
    normalized_skill = canonical_skill_label(skill)
    for item in job_items:
        if item.get("source") == "role-baseline":
            continue
        company = normalize_whitespace(str(item.get("company", ""))).lower() or "unknown"
        key = f"{company}::{normalized_skill}"
        if key in seen_company_skill:
            continue
        normalized_data = item.get("normalized_data", {}) or {}
        stored_evidence = [
            evidence
            for evidence in normalized_data.get("skill_evidence", []) or []
            if canonical_skill_label(str(evidence.get("skill", ""))) == normalized_skill
        ]
        evidence = stored_evidence
        if not evidence and normalized_skill in {
            canonical_skill_label(str(item_skill))
            for item_skill in normalized_data.get("skills", []) or []
            if canonical_skill_label(str(item_skill))
        }:
            evidence = [
                {
                    "snippet": normalize_whitespace(str(item.get("description") or item.get("title") or ""))[:220],
                    "source": str(item.get("source", "unknown")),
                }
            ]
        if not evidence:
            job_text = normalize_whitespace(f"{item.get('title', '')} {item.get('description', '')}")
            job_text_lower = job_text.lower()
            skill_tokens = [token for token in re.split(r"[^a-z0-9]+", normalized_skill.lower()) if token]
            if not skill_tokens or not all(token in job_text_lower for token in skill_tokens):
                continue
            pattern = _exact_skill_pattern(normalized_skill)
            match = pattern.search(job_text)
            evidence = (
                [
                    {
                        "snippet": _build_snippet(job_text, match.start(), match.end()),
                        "source": "job",
                    }
                ]
                if match
                else []
            )
        if not evidence:
            continue
        seen_company_skill.add(key)
        evidence_items.append(
            {
                "title": str(item.get("title", "Unknown Role")),
                "company": str(item.get("company", "Unknown Company")),
                "snippet": str(evidence[0].get("snippet", "")),
                "source": str(item.get("source", "unknown")),
            }
        )
    return evidence_items


def resume_skill_support_levels(
    *,
    resume_sections: dict[str, str] | None,
    skills: list[str],
) -> dict[str, str]:
    if not resume_sections or not skills:
        return {}

    support_levels: dict[str, str] = {}
    normalized_sections = {
        normalize_whitespace(str(name)).lower(): normalize_whitespace(str(text))
        for name, text in (resume_sections or {}).items()
        if normalize_whitespace(str(text))
    }
    section_skill_matches: dict[str, set[str]] = {}
    section_text_variants: dict[str, list[tuple[str, str]]] = {}
    for section_name, section_text in normalized_sections.items():
        section_skill_matches[section_name] = {
            item["skill"]
            for item in extract_skill_matches(section_text, source=f"resume:{section_name}")
        }
        section_text_variants[section_name] = _skill_matching_text_variants(section_text)

    for skill in skills:
        normalized_skill = canonical_skill_label(skill)
        if not normalized_skill:
            continue
        matched_sections: list[str] = []
        for section_name, section_text in normalized_sections.items():
            matched = normalized_skill in section_skill_matches.get(section_name, set())
            if not matched:
                pattern = _exact_skill_pattern(normalized_skill)
                matched = any(pattern.search(text_variant) for _, text_variant in section_text_variants.get(section_name, []))
            if matched:
                matched_sections.append(section_name)

        section_score = sum(RESUME_SECTION_SUPPORT_WEIGHTS.get(section, 0.4) for section in matched_sections)
        if any(section in RESUME_HIGH_PROOF_SECTIONS for section in matched_sections):
            support_levels[normalized_skill] = "strong"
        elif section_score >= 0.85 or (
            len(matched_sections) >= 2
            and any(section not in {"summary", "skills"} for section in matched_sections)
        ):
            support_levels[normalized_skill] = "medium"
        elif matched_sections:
            support_levels[normalized_skill] = "weak"
    return support_levels


def resume_skill_proof_weight(
    *,
    skill: str,
    resume_skills: set[str],
    support_levels: dict[str, str],
) -> float:
    normalized_skill = canonical_skill_label(skill)
    canonical_resume_skills = {canonical_skill_label(item) for item in resume_skills if canonical_skill_label(item)}
    if normalized_skill not in canonical_resume_skills:
        return RESUME_PROOF_LEVEL_WEIGHTS["none"]
    support_level = support_levels.get(normalized_skill, "weak")
    return RESUME_PROOF_LEVEL_WEIGHTS.get(support_level, RESUME_PROOF_LEVEL_WEIGHTS["weak"])


def required_resume_proof_weight(
    *,
    skill: str,
    role_query: str | None,
    market_stats: dict | None = None,
    experience_years: float | None = None,
    live_job_count: int = 0,
) -> float:
    threshold = RESUME_PROOF_LEVEL_WEIGHTS["medium"]
    if not role_query:
        return threshold

    normalized_skill = canonical_skill_label(skill)
    stats = market_stats or {}
    share = float(stats.get("share", 0.0) or 0.0)
    live_count = int(stats.get("live_count", 0) or 0)
    company_count = int(stats.get("company_count", 0) or 0)
    family = role_family(role_query)
    domain = role_domain(role_query)
    dense_role = family in DENSE_ROLE_GAP_FAMILIES or domain in {"data", "software", "security"}
    early_career = float(experience_years or 0.0) < 1.5
    primary_skill = normalized_skill in role_primary_hints(role_query or "")
    hinted_skill = normalized_skill in role_market_hints(role_query or "") or primary_skill
    repeated_live_signal = live_count >= 2 or company_count >= 2

    if primary_skill and share >= 8.0 and (dense_role or early_career):
        return 0.86
    if hinted_skill and early_career and share >= 10.0 and (repeated_live_signal or live_job_count >= 8):
        return 0.84
    if hinted_skill and dense_role and share >= 12.0 and repeated_live_signal:
        return 0.82
    return threshold


def split_missing_and_weak_skill_proofs(
    gaps: list[dict],
    *,
    resume_skills: set[str],
) -> tuple[list[dict], list[dict]]:
    canonical_resume_skills = {canonical_skill_label(skill) for skill in resume_skills if canonical_skill_label(skill)}
    missing: list[dict] = []
    weak: list[dict] = []

    for item in gaps:
        skill = canonical_skill_label(str(item.get("skill", "")))
        if not skill:
            continue
        normalized_item = {**item, "skill": skill}
        if skill in canonical_resume_skills:
            normalized_item["signal_source"] = "weak-resume-proof"
            weak.append(normalized_item)
        else:
            if normalized_item.get("signal_source") == "weak-resume-proof":
                normalized_item["signal_source"] = "live"
            missing.append(normalized_item)

    def dedupe(items: list[dict]) -> list[dict]:
        deduped: list[dict] = []
        seen: set[str] = set()
        for item in items:
            skill = canonical_skill_label(str(item.get("skill", "")))
            if not skill or skill in seen:
                continue
            seen.add(skill)
            deduped.append({**item, "skill": skill})
        return deduped

    return dedupe(missing), dedupe(weak)


def augment_missing_skills(
    *,
    role_query: str | None,
    resume_skills: set[str],
    resume_sections: dict[str, str] | None = None,
    job_items: list[dict],
    existing_missing_skills: list[dict],
    market_skill_frequency: list[dict] | None = None,
    experience_years: float | None = None,
) -> list[dict]:
    if not role_query:
        return existing_missing_skills[:10]

    normalized_resume_skills = {canonical_skill_label(skill) for skill in resume_skills if canonical_skill_label(skill)}
    augmented: list[dict] = [
        {
            **item,
            "skill": canonical_skill_label(str(item.get("skill", ""))) or str(item.get("skill", "")).lower(),
            "signal_source": str(item.get("signal_source", "live")),
        }
        for item in existing_missing_skills
    ]
    target_count = _missing_gap_target(role_query)
    live_jobs = [item for item in job_items if item.get("source") != "role-baseline"]
    live_job_count = max(len(live_jobs), 1)
    recommendation_skills = [
        canonical_skill_label(skill)
        for skill in role_recommendation_skills(role_query, limit=14)
        if canonical_skill_label(skill)
    ]
    live_market_frequency = market_skill_frequency or infer_skill_frequency(job_items, role_query=role_query)
    live_market_stats = {
        canonical_skill_label(str(item.get("skill", ""))): {**item, "skill": canonical_skill_label(str(item.get("skill", "")))}
        for item in live_market_frequency
        if canonical_skill_label(str(item.get("skill", "")))
    }
    resume_support = resume_skill_support_levels(
        resume_sections=resume_sections,
        skills=sorted({*recommendation_skills, *normalized_resume_skills, *live_market_stats.keys()}),
    )

    def proof_weight_for(skill_name: str) -> float:
        return resume_skill_proof_weight(
            skill=skill_name,
            resume_skills=normalized_resume_skills,
            support_levels=resume_support,
        )

    def required_weight_for(skill_name: str) -> float:
        return required_resume_proof_weight(
            skill=skill_name,
            role_query=role_query,
            market_stats=live_market_stats.get(skill_name, {}),
            experience_years=experience_years,
            live_job_count=live_job_count,
        )

    augmented = [
        item
        for item in augmented
        if not (
            canonical_skill_label(str(item.get("skill", ""))) in normalized_resume_skills
            and proof_weight_for(str(item.get("skill", ""))) >= required_weight_for(str(item.get("skill", "")))
        )
    ]
    existing_names = {canonical_skill_label(str(item.get("skill", ""))) for item in augmented if canonical_skill_label(str(item.get("skill", "")))}

    evidence_backfill: list[dict] = []
    weak_market_backfill: list[dict] = []
    calibrated_backfill: list[dict] = []

    for skill, skill_stats in sorted(
        live_market_stats.items(),
        key=lambda item: float(item[1].get("share", 0.0) or 0.0),
        reverse=True,
    ):
        if skill in existing_names or skill not in normalized_resume_skills:
            continue
        if proof_weight_for(skill) >= required_weight_for(skill):
            continue
        share = float(skill_stats.get("share", 0.0) or 0.0)
        live_count = int(skill_stats.get("live_count", 0) or 0)
        company_count = int(skill_stats.get("company_count", 0) or 0)
        if share < 4.0 and live_count < 2 and company_count < 2:
            continue
        live_evidence = _live_gap_evidence(live_jobs, skill)
        weak_market_backfill.append(
            {
                "skill": skill,
                "share": share,
                "signal_source": "weak-resume-proof",
                "primary_source": live_evidence[0]["source"] if live_evidence else "live-market",
                "job_evidence": live_evidence[:2],
            }
        )
        existing_names.add(skill)

    for rank, skill in enumerate(recommendation_skills, start=1):
        normalized_skill = canonical_skill_label(skill)
        if not normalized_skill or normalized_skill in existing_names:
            continue
        current_proof_weight = proof_weight_for(normalized_skill)
        required_proof_weight = required_weight_for(normalized_skill)
        strongly_covered = current_proof_weight >= required_proof_weight
        weakly_covered = normalized_skill in normalized_resume_skills and current_proof_weight < required_proof_weight
        if strongly_covered:
            continue

        live_evidence = _live_gap_evidence(live_jobs, normalized_skill)
        if live_evidence:
            market_share = float((live_market_stats.get(normalized_skill, {}) or {}).get("share", 0.0) or 0.0)
            live_company_count = len({normalize_whitespace(item["company"]).lower() for item in live_evidence})
            share = round(
                market_share or max(4.5, min(18.0, (live_company_count / live_job_count) * 100)),
                1,
            )
            evidence_backfill.append(
                {
                    "skill": normalized_skill,
                    "share": share,
                    "signal_source": "weak-resume-proof" if weakly_covered else "live-support",
                    "primary_source": live_evidence[0]["source"],
                    "job_evidence": live_evidence[:2],
                }
            )
            existing_names.add(normalized_skill)
            continue

        if weakly_covered:
            continue

        calibrated_share = round(max(3.5, 9.5 - ((rank - 1) * 0.8)), 1)
        calibrated_backfill.append(
            {
                "skill": normalized_skill,
                "share": calibrated_share,
                "signal_source": "role-family",
                "primary_source": "role-baseline",
                "job_evidence": [],
            }
        )
        existing_names.add(normalized_skill)

    augmented.extend(weak_market_backfill)
    augmented.extend(evidence_backfill)
    if len(augmented) < target_count:
        augmented.extend(calibrated_backfill[: max(0, target_count - len(augmented))])

    priority = {"live": 4, "weak-resume-proof": 3, "live-support": 2, "role-family": 1}
    deduped: list[dict] = []
    seen: set[str] = set()
    for item in sorted(
        augmented,
        key=lambda candidate: (
            priority.get(str(candidate.get("signal_source", "live")), 0),
            float(candidate.get("share", 0.0) or 0.0),
        ),
        reverse=True,
    ):
        skill = canonical_skill_label(str(item.get("skill", "")))
        if not skill or skill in seen:
            continue
        seen.add(skill)
        deduped.append(item)
    return deduped[:10]
