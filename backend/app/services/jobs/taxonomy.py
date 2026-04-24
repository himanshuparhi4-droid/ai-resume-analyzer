from __future__ import annotations

from dataclasses import dataclass
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
    "admin",
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
        "insights analyst",
        "data operations analyst",
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
    "data analyst": [
        "data analyst",
        "business intelligence analyst",
        "reporting analyst",
        "bi analyst",
        "analytics analyst",
        "insights analyst",
        "data operations analyst",
        "data analytics",
    ],
    "data scientist": ["data scientist", "machine learning scientist", "applied scientist"],
    "machine learning engineer": ["machine learning engineer", "machine learning", "ai"],
    "data engineer": ["data engineer", "etl engineer", "analytics engineer"],
    "software engineer": ["software engineer", "backend engineer", "python developer"],
    "frontend developer": ["frontend developer", "web developer", "react developer", "frontend engineer"],
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
    "data scientist": {
        "python",
        "pandas",
        "numpy",
        "machine learning",
        "scikit-learn",
        "sql",
        "statistics",
        "forecasting",
        "hypothesis testing",
        "feature engineering",
        "model deployment",
        "deep learning",
        "tensorflow",
        "pytorch",
        "nlp",
        "data visualization",
        "matplotlib",
        "seaborn",
        "spark",
        "mlops",
        "azure",
        "gcp",
    },
    "machine learning engineer": {
        "python",
        "machine learning",
        "scikit-learn",
        "tensorflow",
        "pytorch",
        "sql",
        "feature engineering",
        "model deployment",
        "mlops",
        "spark",
    },
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
    "data analyst": {
        "data analyst",
        "reporting analyst",
        "business intelligence",
        "business intelligence analyst",
        "bi analyst",
        "analytics analyst",
        "insights analyst",
        "data operations analyst",
        "data analytics",
    },
    "data scientist": {"data scientist", "applied scientist", "machine learning scientist", "ml scientist", "research scientist"},
    "machine learning engineer": {"machine learning engineer", "machine learning", "ml engineer", "ai engineer", "ai ml engineer"},
    "data engineer": {"data engineer", "etl", "analytics engineer"},
    "software engineer": {"software engineer", "software developer", "backend engineer", "backend developer", "application engineer", "python developer"},
    "frontend developer": {"frontend", "front end", "frontend developer", "frontend engineer", "react", "react developer", "ui engineer", "web developer", "web engineer"},
    "full stack developer": {"full stack", "fullstack", "mern", "mern stack", "full stack developer", "full stack engineer", "web developer"},
    "devops engineer": {"devops", "devops engineer", "site reliability", "site reliability engineer", "sre", "platform", "platform engineer", "cloud", "aws", "cloud engineer", "cloud architect", "infrastructure engineer"},
    "cybersecurity engineer": {"cybersecurity engineer", "cyber security engineer", "security engineer", "cloud security", "application security", "information security", "security operations", "soc", "security architect", "security analyst"},
    "qa engineer": {"qa", "qa engineer", "quality assurance", "quality assurance engineer", "test", "test engineer", "automation", "automation test engineer"},
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

DOMAIN_SKILL_MODEL_DEFAULTS = {
    "software": {
        "baseline": ["api", "sql", "docker", "git"],
        "recommendations": ["api", "sql", "docker", "git", "system design"],
    },
    "data": {
        "baseline": ["sql", "python", "data analysis", "statistics"],
        "recommendations": ["sql", "python", "data analysis", "statistics", "data visualization"],
    },
    "security": {
        "baseline": ["cloud security", "iam", "siem", "incident response"],
        "recommendations": ["cloud security", "iam", "siem", "incident response", "vulnerability management"],
    },
    "product": {
        "baseline": ["data analysis", "communication", "leadership"],
        "recommendations": ["data analysis", "communication", "leadership", "sql"],
    },
    "design": {
        "baseline": ["figma", "ui design", "ux design"],
        "recommendations": ["figma", "ui design", "ux design", "prototyping"],
    },
    "education": {
        "baseline": ["lesson planning", "classroom management", "curriculum development"],
        "recommendations": ["lesson planning", "classroom management", "curriculum development", "student assessment"],
    },
    "trades": {
        "baseline": ["safety compliance"],
        "recommendations": ["safety compliance"],
    },
}
ROLE_FAMILY_SKILL_MODELS = {
    "software engineer": {
        "baseline": ["python", "java", "javascript", "sql", "api", "docker", "git", "microservices", "system design"],
        "recommendations": ["python", "java", "javascript", "sql", "api", "docker", "git", "microservices", "system design", "go", "rest"],
    },
    "frontend developer": {
        "baseline": ["javascript", "typescript", "react", "next.js", "html", "css", "figma", "graphql", "design systems"],
        "recommendations": ["javascript", "typescript", "react", "next.js", "html", "css", "figma", "graphql", "design systems", "prototyping"],
    },
    "full stack developer": {
        "baseline": ["javascript", "typescript", "react", "node.js", "sql", "api", "docker", "mongodb", "graphql"],
        "recommendations": ["javascript", "typescript", "react", "node.js", "sql", "api", "docker", "mongodb", "graphql", "microservices"],
    },
    "mobile developer": {
        "baseline": ["android", "ios", "swift", "kotlin", "react native", "flutter", "api", "xcode", "android studio"],
        "recommendations": ["android", "ios", "swift", "kotlin", "react native", "flutter", "api", "xcode", "android studio", "graphql"],
    },
    "embedded engineer": {
        "baseline": ["embedded", "firmware", "c", "c++", "microcontroller", "rtos", "device drivers", "serial communication", "iot"],
        "recommendations": ["embedded", "firmware", "c", "c++", "microcontroller", "rtos", "device drivers", "serial communication", "iot", "linux"],
    },
    "data analyst": {
        "baseline": ["sql", "excel", "power bi", "tableau", "data analysis", "statistics", "python", "dashboarding", "reporting", "business intelligence"],
        "recommendations": ["sql", "excel", "power bi", "tableau", "data analysis", "statistics", "python", "dashboarding", "reporting", "business intelligence", "data visualization", "looker"],
    },
    "data scientist": {
        "baseline": ["python", "sql", "pandas", "numpy", "machine learning", "scikit-learn", "statistics", "feature engineering", "model deployment", "forecasting", "pytorch", "tensorflow", "deep learning", "nlp", "data visualization"],
        "recommendations": ["python", "sql", "pandas", "numpy", "machine learning", "scikit-learn", "statistics", "feature engineering", "model deployment", "forecasting", "pytorch", "tensorflow", "deep learning", "nlp", "data visualization", "spark", "mlops", "statsmodels"],
    },
    "machine learning engineer": {
        "baseline": ["python", "machine learning", "pytorch", "tensorflow", "scikit-learn", "feature engineering", "model deployment", "mlops", "docker", "kubernetes"],
        "recommendations": ["python", "machine learning", "pytorch", "tensorflow", "scikit-learn", "feature engineering", "model deployment", "mlops", "docker", "kubernetes", "spark", "api"],
    },
    "data engineer": {
        "baseline": ["python", "sql", "spark", "etl", "airflow", "dbt", "data modeling", "data warehousing", "aws", "postgresql"],
        "recommendations": ["python", "sql", "spark", "etl", "airflow", "dbt", "data modeling", "data warehousing", "aws", "postgresql", "bigquery", "snowflake", "kafka"],
    },
    "database engineer": {
        "baseline": ["sql", "postgresql", "mysql", "oracle", "database design", "database administration", "data warehousing", "performance tuning", "backup"],
        "recommendations": ["sql", "postgresql", "mysql", "oracle", "database design", "database administration", "data warehousing", "performance tuning", "backup", "etl"],
    },
    "devops engineer": {
        "baseline": ["aws", "docker", "kubernetes", "terraform", "linux", "ci/cd", "monitoring", "cloudformation", "ec2", "lambda"],
        "recommendations": ["aws", "docker", "kubernetes", "terraform", "linux", "ci/cd", "monitoring", "cloudformation", "ec2", "lambda", "git"],
    },
    "cybersecurity engineer": {
        "baseline": ["cloud security", "network security", "siem", "splunk", "iam", "incident response", "vulnerability management", "security operations", "penetration testing", "firewall"],
        "recommendations": ["cloud security", "network security", "siem", "splunk", "iam", "incident response", "vulnerability management", "security operations", "penetration testing", "firewall", "linux", "python"],
    },
    "qa engineer": {
        "baseline": ["testing", "test automation", "api", "pytest", "selenium", "playwright", "cypress", "ci/cd", "performance testing"],
        "recommendations": ["testing", "test automation", "api", "pytest", "selenium", "playwright", "cypress", "ci/cd", "performance testing", "javascript"],
    },
    "product manager": {
        "baseline": ["data analysis", "sql", "communication", "leadership", "roadmap planning", "stakeholder management", "experimentation"],
        "recommendations": ["data analysis", "sql", "communication", "leadership", "roadmap planning", "stakeholder management", "experimentation", "dashboarding"],
    },
    "ui/ux designer": {
        "baseline": ["figma", "ui design", "ux design", "prototyping", "wireframing", "user research", "design systems", "interaction design"],
        "recommendations": ["figma", "ui design", "ux design", "prototyping", "wireframing", "user research", "design systems", "interaction design", "communication"],
    },
    "support engineer": {
        "baseline": ["technical support", "troubleshooting", "incident management", "ticketing", "root cause analysis", "monitoring", "linux", "networking", "sla", "api"],
        "recommendations": ["technical support", "troubleshooting", "incident management", "ticketing", "root cause analysis", "monitoring", "linux", "networking", "sla", "api", "sql"],
    },
    "solutions architect": {
        "baseline": ["solution architecture", "system design", "integration", "cloud", "aws", "api", "technical consulting", "pre sales"],
        "recommendations": ["solution architecture", "system design", "integration", "cloud", "aws", "api", "technical consulting", "pre sales", "docker", "kubernetes"],
    },
    "enterprise applications engineer": {
        "baseline": ["salesforce", "apex", "crm", "erp", "sap", "oracle", "microsoft dynamics", "workflow automation", "sql", "api"],
        "recommendations": ["salesforce", "apex", "crm", "erp", "sap", "oracle", "microsoft dynamics", "workflow automation", "sql", "api", "configuration"],
    },
    "technical writer": {
        "baseline": ["technical writing", "documentation", "api documentation", "openapi", "markdown", "developer relations", "information architecture"],
        "recommendations": ["technical writing", "documentation", "api documentation", "openapi", "markdown", "developer relations", "information architecture", "communication"],
    },
    "engineering leadership": {
        "baseline": ["leadership", "engineering management", "system design", "architecture", "mentoring", "scalability", "cross functional collaboration"],
        "recommendations": ["leadership", "engineering management", "system design", "architecture", "mentoring", "scalability", "cross functional collaboration", "delivery management"],
    },
    "teacher": {
        "baseline": ["lesson planning", "classroom management", "curriculum development", "student assessment", "differentiated instruction", "pedagogy"],
        "recommendations": ["lesson planning", "classroom management", "curriculum development", "student assessment", "differentiated instruction", "pedagogy"],
    },
    "carpenter": {
        "baseline": ["woodworking", "framing", "blueprint reading", "finish carpentry", "power tools", "measuring", "safety compliance"],
        "recommendations": ["woodworking", "framing", "blueprint reading", "finish carpentry", "power tools", "measuring", "safety compliance"],
    },
    "painter": {
        "baseline": ["painting", "surface preparation", "color matching", "spray painting", "safety compliance", "coating"],
        "recommendations": ["painting", "surface preparation", "color matching", "spray painting", "safety compliance", "coating"],
    },
}

ROLE_HEAD_TOKENS = {
    "engineer",
    "developer",
    "analyst",
    "scientist",
    "architect",
    "designer",
    "manager",
    "administrator",
    "admin",
    "writer",
    "consultant",
    "specialist",
    "tester",
    "researcher",
    "owner",
    "support",
}
ROLE_SENIORITY_TOKENS = {
    "intern",
    "junior",
    "senior",
    "staff",
    "principal",
    "lead",
    "associate",
    "entry",
    "mid",
    "head",
    "director",
    "chief",
    "cto",
    "vp",
}
ROLE_HEAD_VARIANTS = {
    "engineer": ("engineer", "developer"),
    "developer": ("developer", "engineer"),
    "analyst": ("analyst", "specialist"),
    "scientist": ("scientist", "engineer", "analyst"),
    "architect": ("architect", "engineer", "consultant"),
    "designer": ("designer", "researcher"),
    "manager": ("manager", "owner", "lead"),
    "administrator": ("administrator", "developer", "engineer"),
    "admin": ("admin", "administrator", "developer"),
    "writer": ("writer", "documentation engineer"),
    "consultant": ("consultant", "engineer"),
    "support": ("support engineer", "support specialist"),
}
DOMAIN_QUERY_TEMPLATES = {
    "software": ("{specialty} engineer", "{specialty} developer", "software engineer"),
    "data": ("{specialty} analyst", "{specialty} engineer", "{specialty} scientist", "data analyst"),
    "security": ("{specialty} engineer", "{specialty} analyst", "security engineer"),
    "product": ("{specialty} manager", "{specialty} analyst", "product manager"),
    "design": ("{specialty} designer", "{specialty} researcher", "ux designer"),
    "education": ("{specialty} teacher", "{specialty} instructor", "teacher"),
    "trades": ("{specialty} technician", "{specialty} contractor"),
}
PROVIDER_QUERY_BUDGETS = {
    "jobicy": {"base": 2, "narrow": 1},
    "remotive": {"base": 3, "narrow": 2},
    "themuse": {"base": 2, "narrow": 1},
    "jooble": {"base": 2, "narrow": 1},
    "adzuna": {"base": 2, "narrow": 1},
    "remoteok": {"base": 1, "narrow": 1},
}
ABSTRACT_CANONICAL_QUERY_FAMILIES = {
    "enterprise applications engineer",
    "engineering leadership",
    "technical writer",
}


@dataclass(frozen=True)
class RoleProfile:
    raw_query: str
    cleaned_query: str
    normalized_role: str
    family_role: str | None
    domain: str | None
    head_terms: tuple[str, ...]
    seniority_terms: tuple[str, ...]
    specialty_tokens: tuple[str, ...]
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
DOMAIN_DEFAULT_MARKET_HINTS = {
    "software": {"api", "sql", "docker"},
    "data": {"sql", "python", "data analysis", "statistics"},
    "security": {"cloud security", "iam", "siem", "incident response", "splunk"},
    "product": {"data analysis", "sql", "leadership"},
    "design": {"figma", "ui design", "ux design"},
    "education": {"lesson planning", "classroom management", "curriculum development"},
    "trades": {"safety compliance"},
}
DOMAIN_DEFAULT_PRIMARY_HINTS = {
    "software": {"api", "sql"},
    "data": {"sql", "python"},
    "security": {"cloud security", "iam", "incident response"},
    "product": {"data analysis", "sql"},
    "design": {"figma", "ux design"},
    "education": {"lesson planning", "classroom management"},
    "trades": {"safety compliance"},
}
ROLE_PHRASE_MARKET_HINTS = {
    "computer vision": {"computer vision", "python", "machine learning", "pytorch", "tensorflow", "opencv"},
    "natural language processing": {"nlp", "python", "machine learning", "pytorch", "tensorflow"},
    "deep learning": {"deep learning", "python", "machine learning", "pytorch", "tensorflow"},
    "cloud security": {"cloud security", "iam", "siem", "splunk", "incident response", "vulnerability management", "firewall"},
    "site reliability": {"monitoring", "aws", "kubernetes", "terraform", "linux", "ci/cd"},
    "salesforce": {"salesforce", "apex", "crm", "api"},
    "microsoft dynamics": {"crm", "erp", "api"},
}
ROLE_PHRASE_PRIMARY_HINTS = {
    "computer vision": {"computer vision", "python", "machine learning", "pytorch", "tensorflow"},
    "natural language processing": {"nlp", "python", "machine learning", "pytorch", "tensorflow"},
    "deep learning": {"deep learning", "python", "machine learning", "pytorch", "tensorflow"},
    "cloud security": {"cloud security", "iam", "incident response", "splunk"},
    "site reliability": {"monitoring", "aws", "kubernetes", "terraform"},
    "salesforce": {"salesforce", "apex", "crm"},
    "microsoft dynamics": {"crm", "erp"},
}
ROLE_PHRASE_TITLE_HINTS = {
    "computer vision": {"computer vision", "vision engineer"},
    "natural language processing": {"natural language processing", "nlp engineer", "nlp scientist"},
    "deep learning": {"deep learning", "applied scientist"},
    "cloud security": {"cloud security", "security architect", "security engineer"},
    "site reliability": {"site reliability", "sre", "platform engineer"},
    "salesforce": {"salesforce", "salesforce administrator", "salesforce developer"},
    "microsoft dynamics": {"microsoft dynamics", "dynamics consultant"},
}
ROLE_TOKEN_MARKET_HINTS = {
    "aws": {"aws", "ec2", "lambda", "cloudformation", "terraform", "kubernetes", "linux", "ci/cd"},
    "cloud": {"aws", "cloud security", "kubernetes", "terraform", "linux"},
    "computer": {"computer vision", "python", "machine learning", "opencv"},
    "data": {"sql", "python", "data analysis", "statistics"},
    "devops": {"aws", "docker", "kubernetes", "terraform", "linux", "ci/cd", "monitoring"},
    "dynamics": {"crm", "erp", "api"},
    "nlp": {"nlp", "python", "machine learning", "pytorch", "tensorflow"},
    "oracle": {"sql", "erp", "api"},
    "robotics": {"python", "c++", "system design", "computer vision"},
    "salesforce": {"salesforce", "apex", "crm"},
    "sap": {"erp", "sql"},
    "security": {"cloud security", "network security", "siem", "iam", "incident response", "firewall", "splunk"},
    "sre": {"monitoring", "aws", "kubernetes", "terraform", "linux"},
    "vision": {"computer vision", "pytorch", "tensorflow", "opencv"},
}
ROLE_TOKEN_PRIMARY_HINTS = {
    "aws": {"aws", "terraform", "kubernetes", "linux"},
    "cloud": {"aws", "cloud security", "terraform"},
    "computer": {"computer vision", "python", "machine learning"},
    "devops": {"aws", "docker", "kubernetes", "terraform", "monitoring"},
    "dynamics": {"crm", "erp"},
    "nlp": {"nlp", "python", "machine learning", "pytorch"},
    "oracle": {"sql"},
    "robotics": {"python", "c++", "computer vision"},
    "salesforce": {"salesforce", "apex", "crm"},
    "sap": {"erp"},
    "security": {"cloud security", "siem", "iam", "incident response", "splunk"},
    "sre": {"monitoring", "aws", "kubernetes"},
    "vision": {"computer vision", "pytorch", "tensorflow"},
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

ROLE_FAMILY_EXTENSIONS = {
    "mobile developer": {
        "search_variations": ["mobile developer", "mobile app developer", "android developer", "ios developer", "react native developer"],
        "production_variations": ["mobile developer", "android developer", "ios developer", "mobile app developer", "react native developer"],
        "market_hints": {"android", "ios", "swift", "kotlin", "react native", "flutter", "mobile", "app development", "xcode", "android studio"},
        "primary_hints": {"android", "ios", "swift", "kotlin", "react native", "flutter", "mobile"},
        "title_hints": {"mobile developer", "mobile engineer", "android developer", "ios developer", "react native developer"},
        "keyword_families": ("mobile developer", "mobile app developer", "android developer", "ios developer", "react native developer"),
        "inference_tokens": {"android", "ios", "mobile", "swift", "kotlin", "flutter", "react", "native"},
        "domain": "software",
        "adjacent": {"software engineer", "frontend developer", "full stack developer"},
    },
    "embedded engineer": {
        "search_variations": [
            "embedded engineer",
            "embedded software engineer",
            "embedded systems engineer",
            "embedded software developer",
            "firmware engineer",
            "firmware developer",
            "iot engineer",
            "embedded developer",
        ],
        "production_variations": [
            "embedded engineer",
            "embedded software engineer",
            "embedded systems engineer",
            "firmware engineer",
            "firmware developer",
            "embedded software developer",
        ],
        "market_hints": {"embedded", "firmware", "c", "c++", "microcontroller", "rtos", "hardware", "device drivers", "serial communication", "iot"},
        "primary_hints": {"embedded", "firmware", "c", "c++", "microcontroller", "rtos", "iot"},
        "title_hints": {
            "embedded engineer",
            "embedded software engineer",
            "embedded systems engineer",
            "embedded software developer",
            "firmware engineer",
            "firmware developer",
            "iot engineer",
        },
        "keyword_families": (
            "embedded engineer",
            "embedded software engineer",
            "embedded systems engineer",
            "embedded software developer",
            "firmware engineer",
            "firmware developer",
            "iot engineer",
        ),
        "inference_tokens": {"embedded", "firmware", "microcontroller", "rtos", "iot", "hardware"},
        "domain": "software",
        "adjacent": {"software engineer"},
    },
    "database engineer": {
        "search_variations": ["database engineer", "database administrator", "database developer", "sql developer", "data warehouse engineer", "database architect"],
        "production_variations": ["database engineer", "database administrator", "sql developer", "database developer", "data warehouse engineer"],
        "market_hints": {"sql", "postgresql", "mysql", "oracle", "database design", "database administration", "data warehouse", "etl", "performance tuning", "backup"},
        "primary_hints": {"sql", "postgresql", "mysql", "oracle", "database", "data warehouse"},
        "title_hints": {"database engineer", "database administrator", "database developer", "sql developer", "database architect", "dba"},
        "keyword_families": ("database engineer", "database administrator", "database developer", "sql developer", "data warehouse engineer", "database architect", "dba"),
        "inference_tokens": {"database", "sql", "dba", "postgresql", "mysql", "oracle", "warehouse"},
        "domain": "data",
        "adjacent": {"data engineer", "software engineer"},
    },
    "support engineer": {
        "search_variations": ["technical support engineer", "application support engineer", "it support specialist", "help desk engineer", "noc engineer", "production support engineer"],
        "production_variations": ["technical support engineer", "application support engineer", "it support specialist", "help desk engineer", "noc engineer", "production support engineer"],
        "market_hints": {"technical support", "troubleshooting", "incident management", "ticketing", "customer support", "sla", "root cause analysis", "monitoring", "linux", "networking"},
        "primary_hints": {"technical support", "troubleshooting", "incident management", "ticketing", "application support", "monitoring"},
        "title_hints": {"technical support engineer", "application support engineer", "it support specialist", "help desk engineer", "noc engineer", "production support engineer"},
        "keyword_families": ("technical support engineer", "application support engineer", "it support specialist", "help desk engineer", "noc engineer", "production support engineer", "support engineer"),
        "inference_tokens": {"support", "helpdesk", "noc", "troubleshooting", "ticketing", "incident"},
        "domain": "software",
        "adjacent": {"software engineer", "devops engineer"},
    },
    "solutions architect": {
        "search_variations": ["solutions architect", "software architect", "enterprise architect", "technical architect", "solutions consultant", "solutions engineer", "sales engineer"],
        "production_variations": ["solutions architect", "software architect", "technical architect", "enterprise architect", "solutions consultant", "solutions engineer"],
        "market_hints": {"solution architecture", "systems design", "cloud", "integration", "stakeholder management", "pre sales", "technical consulting", "aws", "architecture"},
        "primary_hints": {"solution architecture", "systems design", "cloud", "integration", "architecture"},
        "title_hints": {"solutions architect", "software architect", "enterprise architect", "technical architect", "solutions consultant", "solutions engineer", "sales engineer"},
        "keyword_families": ("solutions architect", "software architect", "enterprise architect", "technical architect", "solutions consultant", "solutions engineer", "sales engineer"),
        "inference_tokens": {"architect", "architecture", "solutions", "solution", "integration", "presales"},
        "domain": "software",
        "adjacent": {"software engineer", "devops engineer", "enterprise applications engineer"},
    },
    "enterprise applications engineer": {
        "search_variations": [
            "salesforce developer",
            "salesforce admin",
            "salesforce administrator",
            "sap consultant",
            "sap developer",
            "erp consultant",
            "crm developer",
            "oracle developer",
            "microsoft dynamics consultant",
        ],
        "production_variations": [
            "salesforce developer",
            "salesforce admin",
            "salesforce administrator",
            "sap consultant",
            "sap developer",
            "erp consultant",
            "crm developer",
            "oracle developer",
        ],
        "market_hints": {"salesforce", "sap", "crm", "erp", "oracle", "microsoft dynamics", "configuration", "workflow automation", "apex", "sql"},
        "primary_hints": {"salesforce", "sap", "crm", "erp", "oracle", "microsoft dynamics"},
        "title_hints": {
            "salesforce developer",
            "salesforce admin",
            "salesforce administrator",
            "sap consultant",
            "sap developer",
            "erp consultant",
            "crm developer",
            "oracle developer",
            "microsoft dynamics consultant",
        },
        "keyword_families": (
            "salesforce developer",
            "salesforce admin",
            "salesforce administrator",
            "sap consultant",
            "sap developer",
            "erp consultant",
            "crm developer",
            "oracle developer",
            "microsoft dynamics consultant",
        ),
        "inference_tokens": {"salesforce", "sap", "erp", "crm", "oracle", "dynamics"},
        "domain": "software",
        "adjacent": {"solutions architect", "software engineer", "database engineer"},
    },
    "technical writer": {
        "search_variations": ["technical writer", "documentation engineer", "developer advocate", "api documentation writer"],
        "production_variations": ["technical writer", "documentation engineer", "developer advocate", "api documentation writer"],
        "market_hints": {"technical writing", "documentation", "developer documentation", "api documentation", "openapi", "markdown", "communication", "developer relations"},
        "primary_hints": {"technical writing", "documentation", "api documentation", "markdown", "developer relations"},
        "title_hints": {"technical writer", "documentation engineer", "developer advocate", "documentation writer"},
        "keyword_families": ("technical writer", "documentation engineer", "developer advocate", "api documentation"),
        "inference_tokens": {"writer", "documentation", "docs", "doc", "advocate"},
        "domain": "product",
        "adjacent": {"product manager"},
    },
    "engineering leadership": {
        "search_variations": ["engineering manager", "principal engineer", "staff engineer", "head of engineering", "cto"],
        "production_variations": ["engineering manager", "principal engineer", "staff engineer", "head of engineering", "cto"],
        "market_hints": {"leadership", "system design", "architecture", "mentoring", "cross functional collaboration", "engineering management", "scalability"},
        "primary_hints": {"leadership", "system design", "architecture", "engineering management"},
        "title_hints": {"engineering manager", "principal engineer", "staff engineer", "head of engineering", "cto"},
        "keyword_families": ("engineering manager", "principal engineer", "staff engineer", "head of engineering", "cto"),
        "inference_tokens": {"manager", "principal", "staff", "cto", "leadership", "engineering"},
        "domain": "software",
        "adjacent": {"software engineer", "solutions architect"},
    },
}

ROLE_ALIAS_EXTENSIONS = {
    "software developer": "software engineer",
    "application developer": "software engineer",
    "desktop application developer": "software engineer",
    "game developer": "software engineer",
    "systems developer": "software engineer",
    "api developer": "software engineer",
    "blockchain developer": "software engineer",
    "ar vr developer": "software engineer",
    "arvr developer": "software engineer",
    "robotics software engineer": "software engineer",
    "gis developer": "software engineer",
    "simulation engineer": "software engineer",
    "search engineer": "software engineer",
    "backend developer": "software engineer",
    "backend engineer": "software engineer",
    "mobile app developer": "mobile developer",
    "mobile developer": "mobile developer",
    "android developer": "mobile developer",
    "ios developer": "mobile developer",
    "react native developer": "mobile developer",
    "embedded software engineer": "embedded engineer",
    "embedded engineer": "embedded engineer",
    "embedded systems engineer": "embedded engineer",
    "embedded software developer": "embedded engineer",
    "firmware engineer": "embedded engineer",
    "firmware developer": "embedded engineer",
    "iot engineer": "embedded engineer",
    "business analyst": "data analyst",
    "technical business analyst": "data analyst",
    "product analyst": "data analyst",
    "operations analyst": "data analyst",
    "statistician": "data scientist",
    "quantitative analyst": "data scientist",
    "analytics engineer": "data engineer",
    "bi developer": "data engineer",
    "data architect": "data engineer",
    "deep learning engineer": "machine learning engineer",
    "nlp engineer": "machine learning engineer",
    "computer vision engineer": "machine learning engineer",
    "mlops engineer": "machine learning engineer",
    "cloud engineer": "devops engineer",
    "cloud architect": "devops engineer",
    "platform engineer": "devops engineer",
    "infrastructure engineer": "devops engineer",
    "build and release engineer": "devops engineer",
    "systems engineer": "devops engineer",
    "linux engineer": "devops engineer",
    "network engineer": "devops engineer",
    "virtualization engineer": "devops engineer",
    "storage engineer": "devops engineer",
    "site reliability engineer": "devops engineer",
    "sre": "devops engineer",
    "cybersecurity analyst": "cybersecurity engineer",
    "security analyst": "cybersecurity engineer",
    "soc analyst": "cybersecurity engineer",
    "security engineer": "cybersecurity engineer",
    "information security analyst": "cybersecurity engineer",
    "penetration tester": "cybersecurity engineer",
    "ethical hacker": "cybersecurity engineer",
    "vulnerability analyst": "cybersecurity engineer",
    "incident responder": "cybersecurity engineer",
    "digital forensics analyst": "cybersecurity engineer",
    "malware analyst": "cybersecurity engineer",
    "threat intelligence analyst": "cybersecurity engineer",
    "security architect": "cybersecurity engineer",
    "iam engineer": "cybersecurity engineer",
    "grc analyst": "cybersecurity engineer",
    "application security engineer": "cybersecurity engineer",
    "cloud security engineer": "cybersecurity engineer",
    "software test analyst": "qa engineer",
    "automation test engineer": "qa engineer",
    "manual tester": "qa engineer",
    "performance test engineer": "qa engineer",
    "sdet": "qa engineer",
    "quality assurance analyst": "qa engineer",
    "database administrator": "database engineer",
    "dba": "database engineer",
    "database developer": "database engineer",
    "sql developer": "database engineer",
    "data warehouse engineer": "database engineer",
    "database architect": "database engineer",
    "associate product manager": "product manager",
    "project manager": "product manager",
    "program manager": "product manager",
    "scrum master": "product manager",
    "ux designer": "ui/ux designer",
    "ui designer": "ui/ux designer",
    "ux researcher": "ui/ux designer",
    "interaction designer": "ui/ux designer",
    "solutions architect": "solutions architect",
    "software architect": "solutions architect",
    "enterprise architect": "solutions architect",
    "technical architect": "solutions architect",
    "solutions consultant": "solutions architect",
    "solutions engineer": "solutions architect",
    "sales engineer": "solutions architect",
    "technical support engineer": "support engineer",
    "application support engineer": "support engineer",
    "it support specialist": "support engineer",
    "help desk engineer": "support engineer",
    "noc engineer": "support engineer",
    "production support engineer": "support engineer",
    "sap consultant": "enterprise applications engineer",
    "sap developer": "enterprise applications engineer",
    "salesforce developer": "enterprise applications engineer",
    "salesforce admin": "enterprise applications engineer",
    "salesforce administrator": "enterprise applications engineer",
    "erp consultant": "enterprise applications engineer",
    "crm developer": "enterprise applications engineer",
    "oracle developer": "enterprise applications engineer",
    "microsoft dynamics consultant": "enterprise applications engineer",
    "technical writer": "technical writer",
    "documentation engineer": "technical writer",
    "developer advocate": "technical writer",
    "principal engineer": "engineering leadership",
    "staff engineer": "engineering leadership",
    "engineering manager": "engineering leadership",
    "cto": "engineering leadership",
}

for canonical, config in ROLE_FAMILY_EXTENSIONS.items():
    ROLE_SEARCH_VARIATIONS[canonical] = list(config["search_variations"])
    ROLE_PRODUCTION_VARIATIONS[canonical] = list(config["production_variations"])
    ROLE_MARKET_HINTS[canonical] = set(config["market_hints"])
    ROLE_PRIMARY_HINTS[canonical] = set(config["primary_hints"])
    ROLE_TITLE_HINTS[canonical] = set(config["title_hints"])
    ROLE_KEYWORD_FAMILIES[canonical] = tuple(config["keyword_families"])
    ROLE_INFERENCE_TOKEN_HINTS[canonical] = set(config["inference_tokens"])
    ROLE_DOMAIN_MAP[canonical] = str(config["domain"])
    ROLE_ADJACENT_CANONICALS.setdefault(canonical, set()).update(config.get("adjacent", set()))
    for adjacent in config.get("adjacent", set()):
        ROLE_ADJACENT_CANONICALS.setdefault(adjacent, set()).add(canonical)

for alias, canonical in ROLE_ALIAS_EXTENSIONS.items():
    ROLE_SYNONYMS[alias] = canonical

for canonical in ROLE_SEARCH_VARIATIONS.keys():
    ROLE_SYNONYMS.setdefault(canonical, canonical)

ROLE_FAMILY_CANONICALS = set(ROLE_SEARCH_VARIATIONS.keys())


def _clean_role_text(value: str) -> str:
    raw_text = str(value or "").strip()
    raw_text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", raw_text)
    raw_text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", raw_text)
    cleaned = re.sub(r"[^a-z0-9+ ]+", " ", raw_text.lower()).strip()
    return re.sub(r"\s+", " ", cleaned)


def _compact_role_text(value: str) -> str:
    return re.sub(r"[^a-z0-9+]+", "", str(value).lower()).strip()


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


@lru_cache(maxsize=None)
def _compact_role_alias_phrase_map() -> dict[str, str]:
    phrase_map: dict[str, str] = {}
    alias_map = _role_alias_map()
    grouped: dict[str, list[str]] = {}
    for phrase in alias_map.keys():
        compact = _compact_role_text(phrase)
        if compact:
            grouped.setdefault(compact, []).append(phrase)

    for compact, phrases in grouped.items():
        ranked = sorted(
            set(phrases),
            key=lambda phrase: (
                0 if phrase in ROLE_FAMILY_CANONICALS else 1,
                len(phrase.split()),
                len(phrase),
            ),
        )
        if ranked:
            phrase_map[compact] = ranked[0]
    return phrase_map


def _expand_compact_role_alias(cleaned: str) -> str | None:
    if not cleaned:
        return None
    expanded = _compact_role_alias_phrase_map().get(_compact_role_text(cleaned))
    if not expanded or expanded == cleaned:
        return None
    return expanded


def _infer_role_from_fuzzy_alias(cleaned: str) -> str | None:
    if not cleaned:
        return None
    alias_map = _role_alias_map()
    cutoff = 0.9 if len(cleaned.split()) >= 2 else 0.82
    matches = get_close_matches(cleaned, alias_map.keys(), n=1, cutoff=cutoff)
    if not matches:
        compact_cleaned = _compact_role_text(cleaned)
        compact_matches = get_close_matches(compact_cleaned, _compact_role_alias_phrase_map().keys(), n=1, cutoff=0.9)
        if not compact_matches:
            return None
        expanded = _compact_role_alias_phrase_map().get(compact_matches[0], "")
        return alias_map.get(expanded) or ROLE_SYNONYMS.get(expanded)
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


def _domain_for_normalized_role(role: str) -> str | None:
    return ROLE_DOMAIN_MAP.get(role)


@lru_cache(maxsize=None)
def _domain_signal_bank(domain: str) -> set[str]:
    signals: set[str] = set()
    for role, role_domain_value in ROLE_DOMAIN_MAP.items():
        if role_domain_value != domain:
            continue
        signals.update(_role_signal_bank(role))
    return signals


def _infer_domain_from_cleaned_query(cleaned: str, normalized: str) -> str | None:
    known_domain = _domain_for_normalized_role(normalized)
    if known_domain:
        return known_domain
    if not cleaned:
        return None

    tokens = _tokenize_role_text(cleaned)
    words = set(cleaned.split())
    domain_scores = {domain: 0.0 for domain in DOMAIN_QUERY_TEMPLATES.keys()}
    for domain in domain_scores:
        domain_scores[domain] += len(tokens & _domain_signal_bank(domain)) * 1.25

    if {"engineer", "developer", "architect"} & words:
        domain_scores["software"] += 1.0
    if {"analyst", "scientist"} & words:
        domain_scores["data"] += 0.8
    if "data" in words and {"engineer", "developer", "platform", "pipeline", "warehouse", "etl", "architect"} & words:
        domain_scores["data"] += 1.4
    if {"ai", "ml", "llm", "prompt", "model", "models", "genai", "generative"} & words:
        domain_scores["data"] += 1.1
    if {"security", "cyber", "soc", "iam"} & words:
        domain_scores["security"] += 1.25
    if {"designer", "ux", "ui"} & words:
        domain_scores["design"] += 1.25
    if {"product", "roadmap", "owner"} & words:
        domain_scores["product"] += 1.25
    if {"teacher", "lecturer", "faculty", "professor"} & words:
        domain_scores["education"] += 1.25
    if {"carpenter", "painter", "plumber", "electrician"} & words:
        domain_scores["trades"] += 1.25

    best_domain = max(domain_scores.items(), key=lambda item: item[1])
    return best_domain[0] if best_domain[1] >= 1.2 else None


def _extract_head_terms(cleaned: str, normalized: str) -> tuple[str, ...]:
    words = cleaned.split()
    heads = [word for word in words if word in ROLE_HEAD_TOKENS]
    if heads:
        return tuple(dict.fromkeys(heads))
    normalized_heads = [word for word in normalized.split() if word in ROLE_HEAD_TOKENS]
    if normalized_heads:
        return tuple(dict.fromkeys(normalized_heads))
    return ()


def _extract_seniority_terms(cleaned: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(word for word in cleaned.split() if word in ROLE_SENIORITY_TOKENS))


def _extract_specialty_tokens(cleaned: str, normalized: str) -> tuple[str, ...]:
    raw_tokens = [
        token
        for token in cleaned.split()
        if token
        and token not in STOPWORDS
        and token not in ROLE_HEAD_TOKENS
        and token not in ROLE_SENIORITY_TOKENS
        and token not in GENERIC_ROLE_MATCH_TOKENS
    ]
    if raw_tokens:
        return tuple(dict.fromkeys(raw_tokens))
    normalized_tokens = [
        token
        for token in normalized.split()
        if token
        and token not in STOPWORDS
        and token not in ROLE_HEAD_TOKENS
        and token not in ROLE_SENIORITY_TOKENS
        and token not in GENERIC_ROLE_MATCH_TOKENS
    ]
    return tuple(dict.fromkeys(normalized_tokens))


def _expanded_head_terms(head_terms: tuple[str, ...]) -> set[str]:
    expanded: set[str] = set()
    for head in head_terms:
        expanded.add(head)
        expanded.update(ROLE_HEAD_VARIANTS.get(head, (head,)))
    return expanded


@lru_cache(maxsize=None)
def _role_head_terms(role: str) -> tuple[str, ...]:
    cleaned = _clean_role_text(role)
    return tuple(word for word in cleaned.split() if word in ROLE_HEAD_TOKENS)


def _infer_family_role_from_values(
    *,
    cleaned: str,
    normalized: str,
    domain: str | None,
    head_terms: tuple[str, ...],
    specialty_tokens: tuple[str, ...],
) -> str | None:
    if normalized in ROLE_FAMILY_CANONICALS:
        return normalized
    if not cleaned:
        return None

    tokens = _tokenize_role_text(cleaned)
    words = set(cleaned.split())
    expanded_heads = _expanded_head_terms(head_terms)
    best_role: str | None = None
    best_score = 0.0
    second_best = 0.0

    for role in ROLE_FAMILY_CANONICALS:
        score = 0.0
        role_domain_value = _domain_for_normalized_role(role)
        role_heads = set(_role_head_terms(role))
        phrase_bank = _role_phrase_bank(role)
        signal_bank = _role_signal_bank(role)

        if cleaned in phrase_bank:
            score += 12.0
        for phrase in phrase_bank:
            if len(phrase.split()) > 1 and phrase in cleaned:
                score += 4.0

        if domain and role_domain_value == domain:
            score += 3.0
        elif domain and role_domain_value and role_domain_value != domain:
            score -= 1.5

        head_overlap = len(expanded_heads & role_heads)
        if head_overlap:
            score += head_overlap * 3.5
        elif expanded_heads and role_heads:
            score -= 1.0

        signal_hits = len(tokens & signal_bank)
        specialty_hits = len(set(specialty_tokens) & signal_bank)
        score += signal_hits * 1.35
        score += specialty_hits * 2.1

        if specialty_tokens and any(token in words for token in specialty_tokens):
            score += min(2.0, specialty_hits * 0.5)

        if {"security", "cyber"} & words and role == "cybersecurity engineer":
            score += 1.5
        if {"architect", "architecture"} & words and role == "solutions architect":
            score += 1.5
        if {"support", "helpdesk", "noc"} & words and role == "support engineer":
            score += 1.5
        if {"salesforce", "sap", "erp", "crm", "oracle", "dynamics"} & words and role == "enterprise applications engineer":
            score += 1.75
        if {"writer", "documentation", "docs", "advocate"} & words and role == "technical writer":
            score += 1.5
        if {"mobile", "android", "ios", "swift", "kotlin", "flutter"} & words and role == "mobile developer":
            score += 1.75
        if {"embedded", "firmware", "microcontroller", "rtos", "iot"} & words and role == "embedded engineer":
            score += 1.75
        if {"database", "dba", "sql", "warehouse"} & words and role == "database engineer":
            score += 1.75
        if {"principal", "staff", "cto"} & words and role == "engineering leadership":
            score += 1.5

        if score > best_score:
            second_best = best_score
            best_score = score
            best_role = role
        elif score > second_best:
            second_best = score

    if best_role and best_score >= 3.0 and best_score >= second_best + 0.6:
        return best_role
    return None


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
    if {"android", "ios", "swift", "kotlin", "flutter"} & words or ("mobile" in words and {"developer", "engineer"} & words):
        return "mobile developer"
    if {"embedded", "firmware", "microcontroller", "rtos"} & words or ("iot" in words and {"engineer", "developer"} & words):
        return "embedded engineer"
    if {"cyber", "security", "soc", "siem", "splunk", "iam"} & words:
        return "cybersecurity engineer"
    if {"aws", "cloud", "devops", "sre", "terraform", "kubernetes"} & words:
        return "devops engineer"
    if {"database", "dba", "postgresql", "mysql"} & words or (
        "sql" in words and ({"developer", "engineer", "administrator", "architect"} & words or len(words) <= 2)
    ):
        return "database engineer"
    if {"reporting", "analytics", "bi"} & words:
        return "data analyst"
    if "data" in words and {"platform", "pipeline", "warehouse", "etl", "architect"} & words and {"engineer", "developer", "architect"} & words:
        return "data engineer"
    if {"ml", "llm"} & words and "engineer" in words:
        return "machine learning engineer"
    if {"ai", "llm", "prompt", "genai", "generative"} & words and {"engineer", "developer"} & words:
        return "machine learning engineer"
    if {"ml", "llm", "scientist"} & words:
        return "data scientist"
    if {"salesforce", "sap", "erp", "crm", "dynamics"} & words:
        return "enterprise applications engineer"
    if {"support", "helpdesk", "noc"} & words:
        return "support engineer"
    if {"writer", "documentation", "docs", "doc", "advocate"} & words:
        return "technical writer"
    if "cto" in words or {"principal", "staff"} & words or ({"engineering", "manager"} <= words):
        return "engineering leadership"
    if {"architect", "architecture"} & words and ({"solutions", "solution", "technical", "enterprise", "software"} & words or len(words) <= 2):
        return "solutions architect"
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
        if _domain_for_normalized_role(role) == "software" and {"developer", "engineer", "frontend", "backend"} & set(cleaned.split()):
            score += 0.8
        if _domain_for_normalized_role(role) == "data" and {"analyst", "analytics", "data", "reporting", "bi"} & set(cleaned.split()):
            score += 0.8
        if _domain_for_normalized_role(role) == "security" and {"security", "cyber"} & set(cleaned.split()):
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


def _match_keyword_family(cleaned: str, *, exact_only: bool = False) -> str | None:
    if not cleaned:
        return None
    padded_cleaned = f" {cleaned} "
    best_role: str | None = None
    best_length = -1
    for canonical, keywords in ROLE_KEYWORD_FAMILIES.items():
        for keyword in keywords:
            normalized_keyword = _clean_role_text(keyword)
            if not normalized_keyword:
                continue
            if exact_only:
                matched = cleaned == normalized_keyword
            else:
                padded_keyword = f" {normalized_keyword} "
                matched = padded_keyword in padded_cleaned or cleaned == normalized_keyword
            if matched and len(normalized_keyword) > best_length:
                best_role = canonical
                best_length = len(normalized_keyword)
    return best_role


def normalize_role(query: str) -> str:
    cleaned = _clean_role_text(query)
    if cleaned in ROLE_SYNONYMS:
        return ROLE_SYNONYMS[cleaned]
    expanded_compact_alias = _expand_compact_role_alias(cleaned)
    if expanded_compact_alias and expanded_compact_alias in ROLE_SYNONYMS:
        return ROLE_SYNONYMS[expanded_compact_alias]
    exact_keyword_match = _match_keyword_family(cleaned, exact_only=True)
    if exact_keyword_match:
        return exact_keyword_match
    inferred = _infer_role_from_cleaned_query(cleaned)
    if inferred:
        return inferred
    loose_keyword_match = _match_keyword_family(cleaned)
    if loose_keyword_match:
        return loose_keyword_match
    return cleaned


@lru_cache(maxsize=None)
def role_profile(query: str) -> RoleProfile:
    cleaned = _clean_role_text(query)
    normalized = normalize_role(query)
    expanded_cleaned = _expand_compact_role_alias(cleaned) or cleaned
    domain = _infer_domain_from_cleaned_query(expanded_cleaned, normalized)
    head_terms = _extract_head_terms(expanded_cleaned, normalized)
    seniority_terms = _extract_seniority_terms(expanded_cleaned)
    specialty_tokens = _extract_specialty_tokens(expanded_cleaned, normalized)
    return RoleProfile(
        raw_query=str(query or ""),
        cleaned_query=expanded_cleaned,
        normalized_role=normalized,
        family_role=_infer_family_role_from_values(
            cleaned=expanded_cleaned,
            normalized=normalized,
            domain=domain,
            head_terms=head_terms,
            specialty_tokens=specialty_tokens,
        ),
        domain=domain,
        head_terms=head_terms,
        seniority_terms=seniority_terms,
        specialty_tokens=specialty_tokens,
    )


def _meaningful_raw_query(raw_cleaned: str, normalized: str) -> bool:
    raw_tokens = [token for token in raw_cleaned.split() if token and token not in STOPWORDS]
    if not raw_tokens or raw_cleaned == normalized:
        return False
    if all(token in GENERIC_ROLE_MATCH_TOKENS for token in raw_tokens):
        return False
    return True


def _preserve_exact_canonical_query(profile: RoleProfile) -> bool:
    if profile.normalized_role not in ROLE_FAMILY_CANONICALS:
        return False
    if profile.cleaned_query != profile.normalized_role:
        return False
    specialty_tokens = {token for token in profile.specialty_tokens if token}
    if not specialty_tokens:
        return True
    normalized_tokens = set(profile.normalized_role.split())
    return specialty_tokens <= normalized_tokens


def _head_query_variants(profile: RoleProfile) -> list[str]:
    if profile.head_terms:
        variants: list[str] = []
        for head in profile.head_terms:
            variants.extend(ROLE_HEAD_VARIANTS.get(head, (head,)))
        return list(dict.fromkeys(item for item in variants if item))
    if profile.domain == "data":
        return ["analyst", "engineer", "scientist"]
    if profile.domain == "security":
        return ["engineer", "analyst", "architect"]
    if profile.domain == "product":
        return ["manager", "analyst"]
    if profile.domain == "design":
        return ["designer", "researcher"]
    if profile.domain == "education":
        return ["teacher", "instructor"]
    if profile.domain == "trades":
        return ["technician", "contractor"]
    return ["engineer", "developer"]


def _generic_query_expansions(profile: RoleProfile) -> list[str]:
    expansions: list[str] = []
    specialty = " ".join(profile.specialty_tokens).strip()
    allowed_head_variants = set(_head_query_variants(profile)) if profile.head_terms else set()

    if profile.cleaned_query and _meaningful_raw_query(profile.cleaned_query, profile.normalized_role):
        expansions.append(profile.cleaned_query)
    if profile.normalized_role:
        expansions.append(profile.normalized_role)

    if specialty:
        for head_variant in _head_query_variants(profile):
            if head_variant in specialty:
                expansions.append(specialty)
            else:
                expansions.append(f"{specialty} {head_variant}".strip())
        for template in DOMAIN_QUERY_TEMPLATES.get(profile.domain or "", ()):
            if "{specialty}" in template:
                candidate = template.format(specialty=specialty).strip()
            else:
                candidate = template
            if allowed_head_variants:
                candidate_tokens = set(candidate.split())
                if not (candidate_tokens & allowed_head_variants):
                    continue
            expansions.append(candidate)

    canonical_role = profile.family_role or profile.normalized_role
    if canonical_role:
        family_safe_candidates = {
            *ROLE_SEARCH_VARIATIONS.get(canonical_role, []),
            *ROLE_PRODUCTION_VARIATIONS.get(canonical_role, []),
            *ROLE_TITLE_HINTS.get(canonical_role, set()),
        }
        for candidate in family_safe_candidates:
            cleaned_candidate = _clean_role_text(candidate)
            if not cleaned_candidate or cleaned_candidate in {profile.cleaned_query, profile.normalized_role}:
                continue
            candidate_profile = role_profile(cleaned_candidate)
            candidate_family = candidate_profile.family_role or candidate_profile.normalized_role
            if candidate_family != canonical_role:
                continue
            candidate_tokens = set(cleaned_candidate.split())
            if allowed_head_variants and cleaned_candidate != profile.normalized_role:
                if not (candidate_tokens & allowed_head_variants):
                    continue
            expansions.append(cleaned_candidate)

    cleaned_expansions: list[str] = []
    for item in expansions:
        words = item.split()
        deduped = " ".join(word for index, word in enumerate(words) if index == 0 or word != words[index - 1]).strip()
        if deduped:
            cleaned_expansions.append(deduped)
    return list(dict.fromkeys(cleaned_expansions))


def _dynamic_query_expansions(raw_cleaned: str, normalized: str) -> list[str]:
    if not _meaningful_raw_query(raw_cleaned, normalized):
        return []

    profile = role_profile(raw_cleaned or normalized)
    generic_expansions = _generic_query_expansions(profile)
    if generic_expansions:
        return generic_expansions

    raw_tokens = [token for token in raw_cleaned.split() if token]
    normalized_tokens = set(normalized.split())
    specialized_families = {
        "mobile developer",
        "embedded engineer",
        "database engineer",
        "support engineer",
        "solutions architect",
        "enterprise applications engineer",
        "technical writer",
        "engineering leadership",
    }
    expansions: list[str] = [raw_cleaned]
    if len(raw_tokens) == 1:
        root = raw_tokens[0]
        if normalized == "mobile developer":
            expansions.extend([f"{root} developer", f"{root} mobile developer"])
        if normalized == "embedded engineer":
            expansions.extend([f"{root} engineer", f"{root} embedded engineer"])
        if normalized == "database engineer":
            if root == "dba":
                expansions.extend(["database administrator", "database engineer", "sql developer"])
            else:
                expansions.extend([f"{root} administrator", f"{root} developer", f"{root} architect"])
        if normalized == "support engineer":
            expansions.extend([f"{root} support engineer", f"{root} support specialist"])
        if normalized == "solutions architect":
            expansions.extend([f"{root} architect", f"{root} consultant"])
        if normalized == "enterprise applications engineer":
            if root == "sap":
                expansions.extend(["sap consultant", "sap developer"])
            elif root == "salesforce":
                expansions.extend(["salesforce developer", "salesforce admin"])
            else:
                expansions.extend([f"{root} consultant", f"{root} developer"])
        if normalized == "technical writer":
            expansions.extend([f"{root} writer", f"{root} documentation engineer"])
        if normalized == "engineering leadership":
            if root == "cto":
                expansions.extend(["head of engineering", "engineering manager"])
            else:
                expansions.extend([f"{root} engineer", f"{root} manager"])
        if "analyst" in normalized_tokens:
            expansions.append(f"{root} analyst")
        if "designer" in normalized_tokens:
            expansions.extend([f"{root} designer", f"{root} ux designer"])
        if "manager" in normalized_tokens:
            expansions.append(f"{root} manager")
        if "teacher" in normalized_tokens:
            expansions.append(f"{root} teacher")
        if normalized not in specialized_families and ("engineer" in normalized_tokens or "developer" in normalized_tokens):
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
    profile = role_profile(query)
    return {
        token
        for token in [*profile.cleaned_query.split(), *profile.normalized_role.split()]
        if token and token not in STOPWORDS and token not in GENERIC_ROLE_MATCH_TOKENS and len(token) > 2
    }


def query_variations(query: str) -> list[str]:
    profile = role_profile(query)
    canonical_role = profile.family_role or profile.normalized_role
    variations = [*ROLE_SEARCH_VARIATIONS.get(canonical_role, [canonical_role])]
    if not _preserve_exact_canonical_query(profile):
        variations = [*_generic_query_expansions(profile), *variations]
    if profile.cleaned_query and profile.cleaned_query not in variations:
        variations = [profile.cleaned_query, *variations]
    return list(dict.fromkeys(item for item in variations if item))


def _is_generic_security_query(profile: RoleProfile) -> bool:
    cleaned_tokens = set(profile.cleaned_query.split())
    explicit_head_tokens = cleaned_tokens & ROLE_HEAD_TOKENS
    return (
        profile.domain == "security"
        and not explicit_head_tokens
        and bool(cleaned_tokens & {"cyber", "cybersecurity", "security", "infosec"})
    )


def production_query_variations(query: str) -> list[str]:
    profile = role_profile(query)
    canonical_role = profile.family_role or profile.normalized_role
    variations = [*ROLE_PRODUCTION_VARIATIONS.get(canonical_role, [canonical_role])]
    if not _preserve_exact_canonical_query(profile):
        variations = [*_generic_query_expansions(profile), *variations]
    if _is_generic_security_query(profile):
        variations = [
            "cybersecurity",
            "cybersecurity engineer",
            "security engineer",
            "security analyst",
            "soc analyst",
            "cybersecurity analyst",
            *variations,
        ]
    if profile.cleaned_query and profile.cleaned_query not in variations:
        variations = [profile.cleaned_query, *variations]
    return list(dict.fromkeys(item for item in variations if item))[:6]


def _query_priority_score(candidate: str, profile: RoleProfile, source_name: str) -> float:
    cleaned_candidate = _clean_role_text(candidate)
    candidate_tokens = set(cleaned_candidate.split())
    candidate_profile = role_profile(cleaned_candidate)
    candidate_heads = set(candidate_profile.head_terms)
    score = 0.0
    if cleaned_candidate == profile.cleaned_query:
        score += 18.0
    if cleaned_candidate == profile.normalized_role:
        score += 3.0 if profile.normalized_role in ABSTRACT_CANONICAL_QUERY_FAMILIES and profile.specialty_tokens else 10.0
    if profile.specialty_tokens:
        score += len(candidate_tokens & set(profile.specialty_tokens)) * 3.0
    generic_security_query = _is_generic_security_query(profile)
    if generic_security_query:
        if candidate_tokens & {"security", "cybersecurity", "cyber", "soc"}:
            score += 4.0
        if candidate_tokens & {"analyst", "soc"}:
            score += 7.0
        if cleaned_candidate in {"security analyst", "soc analyst"}:
            score += 10.0
    if profile.head_terms and not generic_security_query:
        head_overlap = len(candidate_tokens & set(profile.head_terms))
        score += head_overlap * 4.0
        exact_head_overlap = len(candidate_heads & set(profile.head_terms))
        if exact_head_overlap:
            score += exact_head_overlap * 2.5
        if head_overlap == 0 and candidate_heads:
            score -= 6.5
        elif head_overlap == 0:
            score -= 5.0
    if source_name in {"jobicy", "themuse", "remoteok"}:
        score -= max(0, len(candidate_tokens) - 3) * 0.6
    elif source_name in {"remotive", "jooble", "adzuna"}:
        score -= max(0, len(candidate_tokens) - 4) * 0.35
    return score


def _provider_query_budget(profile: RoleProfile, source_name: str, *, production: bool, query_is_narrow: bool) -> int:
    budget_config = PROVIDER_QUERY_BUDGETS.get(source_name, {"base": 2, "narrow": 1})
    budget = int(budget_config["narrow"] if query_is_narrow else budget_config["base"])
    family_alias_query = bool(profile.family_role and profile.family_role != profile.normalized_role)
    abstract_family = profile.normalized_role in ABSTRACT_CANONICAL_QUERY_FAMILIES
    broad_dense_query = profile.domain in {"software", "data", "security"} and not query_is_narrow
    security_analyst_style = profile.domain == "security" and any(
        head in {"analyst", "specialist"} for head in profile.head_terms
    )
    weak_software_family = profile.normalized_role in {"frontend developer", "mobile developer", "embedded engineer"}

    if not production:
        if family_alias_query:
            budget = max(budget, 2)
        return budget

    if source_name in {"remotive", "jooble", "adzuna"}:
        if broad_dense_query:
            budget = max(budget, 3)
        if source_name == "remotive" and (broad_dense_query or family_alias_query or abstract_family):
            budget = max(budget, 4)
        if source_name in {"jooble", "adzuna"} and security_analyst_style:
            budget = max(budget, 3)
        if source_name == "jooble" and weak_software_family:
            budget = max(budget, 3)
    if source_name == "jobicy":
        if profile.domain == "security":
            budget = max(budget, 2) if security_analyst_style else 1
        elif profile.domain == "data":
            budget = 1
        elif profile.domain == "software":
            budget = max(budget, 2 if (broad_dense_query or family_alias_query or abstract_family) else 1)
            if weak_software_family and not query_is_narrow:
                budget = max(budget, 3)
            if query_is_narrow and not family_alias_query and not abstract_family:
                budget = 1
    if source_name == "themuse" and profile.domain in {"data", "security"}:
        budget = min(budget, 1)
    if family_alias_query:
        budget = max(budget, 2)
    return budget


def _provider_query_anchors(profile: RoleProfile) -> list[str]:
    anchors: list[str] = []
    if profile.cleaned_query:
        anchors.append(profile.cleaned_query)
    if profile.normalized_role:
        anchors.append(profile.normalized_role)
    if profile.family_role and profile.family_role != profile.normalized_role:
        anchors.append(profile.family_role)
    return list(dict.fromkeys(item for item in anchors if item))


def provider_query_variations(query: str, source_name: str, *, production: bool = False) -> list[str]:
    profile = role_profile(query)
    variations = production_query_variations(query) if production else query_variations(query)
    candidate_scores = {
        item: _query_priority_score(item, profile, source_name)
        for item in variations
    }
    ranked = sorted(
        variations,
        key=lambda item: (candidate_scores.get(item, 0.0), -len(item.split())),
        reverse=True,
    )
    query_is_narrow = len(profile.specialty_tokens) >= 2 or len(profile.cleaned_query.split()) >= 3
    budget = _provider_query_budget(profile, source_name, production=production, query_is_narrow=query_is_narrow)
    selected = ranked[: max(1, budget)]

    def ensure_selected(candidate: str) -> None:
        if candidate not in ranked or candidate in selected:
            return
        if len(selected) < max(1, budget):
            selected.append(candidate)
            return
        if not selected:
            return

        weakest_selected = min(
            selected,
            key=lambda item: (candidate_scores.get(item, 0.0), -len(item.split())),
        )
        candidate_rank = (candidate_scores.get(candidate, 0.0), -len(candidate.split()))
        weakest_rank = (candidate_scores.get(weakest_selected, 0.0), -len(weakest_selected.split()))
        if candidate_rank > weakest_rank:
            selected[selected.index(weakest_selected)] = candidate

    for anchor in _provider_query_anchors(profile):
        ensure_selected(anchor)
    return list(dict.fromkeys(selected))


def _derived_role_skill_hints(profile: RoleProfile, *, primary_only: bool) -> set[str]:
    hints = set(
        DOMAIN_DEFAULT_PRIMARY_HINTS.get(profile.domain or "", set())
        if primary_only
        else DOMAIN_DEFAULT_MARKET_HINTS.get(profile.domain or "", set())
    )
    phrase_map = ROLE_PHRASE_PRIMARY_HINTS if primary_only else ROLE_PHRASE_MARKET_HINTS
    token_map = ROLE_TOKEN_PRIMARY_HINTS if primary_only else ROLE_TOKEN_MARKET_HINTS
    cleaned_query = profile.cleaned_query
    for phrase, phrase_hints in phrase_map.items():
        if phrase in cleaned_query:
            hints.update(phrase_hints)
    for token in set(cleaned_query.split()) | set(profile.specialty_tokens):
        hints.update(token_map.get(token, set()))
    return hints


def _derived_role_title_hints(profile: RoleProfile) -> set[str]:
    hints = set()
    if _meaningful_raw_query(profile.cleaned_query, profile.normalized_role):
        hints.add(profile.cleaned_query)
    for phrase, phrase_hints in ROLE_PHRASE_TITLE_HINTS.items():
        if phrase in profile.cleaned_query:
            hints.update(phrase_hints)
    if "salesforce" in profile.cleaned_query:
        hints.update({"salesforce", "salesforce administrator", "salesforce developer"})
    if "aws" in profile.cleaned_query:
        hints.update({"aws", "cloud engineer", "devops engineer"})
    if "robotics" in profile.cleaned_query:
        hints.update({"robotics", "robotics engineer"})
    return {hint for hint in hints if hint}


def role_market_hints(query: str) -> set[str]:
    profile = role_profile(query)
    canonical_role = profile.family_role or profile.normalized_role
    return set(ROLE_MARKET_HINTS.get(canonical_role, set())) | _derived_role_skill_hints(profile, primary_only=False)


def role_primary_hints(query: str) -> set[str]:
    profile = role_profile(query)
    canonical_role = profile.family_role or profile.normalized_role
    return set(ROLE_PRIMARY_HINTS.get(canonical_role, set())) | _derived_role_skill_hints(profile, primary_only=True)


def role_title_hints(query: str) -> set[str]:
    profile = role_profile(query)
    canonical_role = profile.family_role or profile.normalized_role
    return set(ROLE_TITLE_HINTS.get(canonical_role, set())) | _derived_role_title_hints(profile)


def role_family(query: str) -> str:
    profile = role_profile(query)
    return profile.family_role or profile.normalized_role


def role_domain(query: str) -> str | None:
    profile = role_profile(query)
    return _domain_for_normalized_role(profile.family_role or profile.normalized_role) or profile.domain


def _ordered_unique_skills(*skill_groups: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for group in skill_groups:
        for skill in group:
            cleaned = _clean_role_text(skill)
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            ordered.append(cleaned)
    return ordered


def role_skill_model(query: str) -> dict[str, list[str]]:
    profile = role_profile(query)
    canonical_role = profile.family_role or profile.normalized_role
    explicit = ROLE_FAMILY_SKILL_MODELS.get(canonical_role)
    if explicit:
        baseline = _ordered_unique_skills(
            list(explicit.get("baseline", [])),
            sorted(ROLE_PRIMARY_HINTS.get(canonical_role, set())),
            sorted(ROLE_MARKET_HINTS.get(canonical_role, set())),
        )
        recommendations = _ordered_unique_skills(
            list(explicit.get("recommendations", [])),
            baseline,
            sorted(ROLE_MARKET_HINTS.get(canonical_role, set())),
        )
        return {
            "baseline": baseline,
            "recommendations": recommendations,
        }

    domain = role_domain(query)
    domain_defaults = DOMAIN_SKILL_MODEL_DEFAULTS.get(domain or "", {"baseline": [], "recommendations": []})
    baseline = _ordered_unique_skills(
        list(domain_defaults.get("baseline", [])),
        sorted(role_primary_hints(query)),
        sorted(role_market_hints(query)),
    )
    recommendations = _ordered_unique_skills(
        list(domain_defaults.get("recommendations", [])),
        baseline,
        sorted(role_market_hints(query)),
    )
    return {
        "baseline": baseline,
        "recommendations": recommendations,
    }


def role_baseline_skills(query: str, *, limit: int | None = None) -> list[str]:
    baseline = list(role_skill_model(query).get("baseline", []))
    return baseline[:limit] if limit else baseline


def role_recommendation_skills(query: str, *, limit: int | None = None) -> list[str]:
    recommendations = list(role_skill_model(query).get("recommendations", []))
    return recommendations[:limit] if limit else recommendations


def is_sparse_live_market_role(query: str) -> bool:
    return normalize_role(query) in SPARSE_LIVE_MARKET_ROLES


def role_negative_title_hints(query: str) -> set[str]:
    domain = role_domain(query)
    if not domain:
        return set()
    return ROLE_NEGATIVE_TITLE_HINTS.get(domain, set())


def canonical_role_alignment(query: str, title: str) -> int:
    normalized_query = role_family(query)
    normalized_title = role_family(title)
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
    normalized_query = role_family(query)
    normalized_title = role_family(title)
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
    normalized_query = role_family(query)
    query_tokens = list(dict.fromkeys(role_query_tokens(query)))
    title = role_family(item.get("title", ""))
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
