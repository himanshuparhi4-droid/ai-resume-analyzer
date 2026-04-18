from __future__ import annotations

import io
import re
from datetime import date
from typing import Any

import pdfplumber
from docx import Document
from PyPDF2 import PdfReader

from app.services.nlp.skill_extractor import extract_skills
from app.services.parsers.ocr import OCRService
from app.utils.text import normalize_whitespace

SECTION_HEADERS = {
    "summary": ["summary", "professional summary", "profile", "objective", "about me", "professional profile"],
    "experience": ["experience", "work experience", "professional experience", "employment", "career history", "employment history", "work history", "work experience"],
    "projects": ["projects", "project experience", "selected projects", "academic projects"],
    "education": ["education", "academics", "education and training", "academic background"],
    "skills": ["skills", "technical skills", "core skills", "personal skills", "digital skills", "technical proficiencies", "core competencies"],
    "certifications": ["certifications", "licenses", "courses", "licenses & certifications"],
    "research": ["research", "research experience"],
    "publications": ["publications", "papers", "selected publications"],
    "teaching": ["teaching", "teaching experience"],
    "awards": ["awards", "honors", "achievements"],
    "languages": ["languages", "language skills"],
}
HEADER_ALIASES = {
    alias: key
    for key, headers in SECTION_HEADERS.items()
    for alias in sorted(headers, key=len, reverse=True)
}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+91|\+1)?[ -]?(?:\d[ -]?){10,12}")
YEAR_RE = re.compile(r"((?:19|20)\d{2})")
YEARS_EXPERIENCE_RE = re.compile(r"(\d{1,2})\+?\s+years")
MONTHS_EXPERIENCE_RE = re.compile(r"(\d{1,2})\+?\s+months?")
MONTH_NAME_RE = r"(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)"
DATE_RANGE_RE = re.compile(
    rf"((?:{MONTH_NAME_RE})\s+\d{{4}}|\d{{1,2}}/\d{{4}}|\d{{4}})\s*(?:-|to)\s*(present|current|now|(?:{MONTH_NAME_RE})\s+\d{{4}}|\d{{1,2}}/\d{{4}}|\d{{4}})",
    re.IGNORECASE,
)
WORK_CONTEXT_RE = re.compile(
    r"\b(intern|internship|apprentice|trainee|analyst|engineer|developer|consultant|associate|scientist|specialist|coordinator|representative|manager|assistant|freelance|contract|systems|techworld|solutions|ltd|pvt|company|corp)\b",
    re.IGNORECASE,
)
PROJECT_CONTEXT_RE = re.compile(
    r"\b(project|capstone|case study|prototype|portfolio|thesis|implementation|dashboard|analysis|model|build)\b",
    re.IGNORECASE,
)
EDUCATION_CONTEXT_RE = re.compile(
    r"\b(education|university|college|school|b\.?tech|bachelor|master|12th|10th|class|cgpa|gpa)\b",
    re.IGNORECASE,
)
URL_RE = re.compile(r"https?://\S+")
OCR_NOISE_MARKERS = ("linfiedin", "scifiit", "sefihar")
BULLET_LINE_RE = re.compile(r"^(?:[-*•]|(?:\d+[\).]))\s+")
QUANTIFIED_LINE_RE = re.compile(r"\b\d[\d,]*\b|%")
ACTION_LINE_RE = re.compile(
    r"\b(built|developed|designed|implemented|led|managed|delivered|improved|optimized|launched|created|reduced|increased|analyzed|automated|deployed|taught|published|researched|mentored|presented)\b",
    re.IGNORECASE,
)
ACADEMIC_MARKER_RE = re.compile(r"\b(publication|publications|journal|conference|research|thesis|dissertation|teaching assistant|faculty)\b", re.IGNORECASE)
TEACHING_MARKER_RE = re.compile(r"\b(curriculum|lesson planning|classroom|pedagogy|lecturer|faculty|teacher|student engagement)\b", re.IGNORECASE)
EXECUTIVE_MARKER_RE = re.compile(r"\b(director|vice president|vp|head of|strategy|p&l|stakeholder management|organizational)\b", re.IGNORECASE)
CREATIVE_MARKER_RE = re.compile(r"\b(behance|dribbble|portfolio|branding|visual design|ux|ui|art direction|creative)\b", re.IGNORECASE)
TECHNICAL_PORTFOLIO_MARKER_RE = re.compile(r"\b(github|portfolio|deployed|api|microservice|full stack|machine learning|dashboard|cloud)\b", re.IGNORECASE)
CERTIFICATION_MARKER_RE = re.compile(r"\b(certification|certified|coursework|license|credential|training)\b", re.IGNORECASE)
OBJECTIVE_MARKER_RE = re.compile(r"\bobjective\b", re.IGNORECASE)


class ResumeParser:
    def __init__(self) -> None:
        self.ocr_service = OCRService()

    def parse(self, filename: str, content_type: str, file_bytes: bytes) -> dict[str, Any]:
        text, extract_meta = self._extract_text(filename, content_type, file_bytes)
        normalized = normalize_whitespace(text)
        if not normalized.strip():
            raise ValueError(
                "Could not extract readable text from this resume. "
                "Try a text-based PDF or DOCX file, or verify OCR/Poppler for scanned PDFs."
            )
        skills = extract_skills(normalized)
        grouped_sections = self._group_section_lines(text)
        sections = {key: normalize_whitespace(" ".join(value)) for key, value in grouped_sections.items() if value}
        if "skills" not in sections and re.search(r"\bskills\b", normalized, re.IGNORECASE) and skills:
            sections["skills"] = ", ".join(skills[:12])
        experience_years = self._estimate_experience_years(normalized, sections)
        parse_signals = self._analyze_parse_signals(
            text=text,
            normalized=normalized,
            sections=sections,
            grouped_sections=grouped_sections,
            skills=skills,
            extract_meta=extract_meta,
        )
        resume_archetype = self._detect_resume_archetype(
            normalized=normalized,
            sections=sections,
            parse_signals=parse_signals,
            experience_years=experience_years,
        )
        return {
            "filename": filename,
            "content_type": content_type,
            "raw_text": normalized,
            "sections": sections,
            "contact": self._extract_contact(normalized),
            "skills": skills,
            "experience_years": experience_years,
            "education_years": sorted({int(year) for year in YEAR_RE.findall(normalized)}),
            "parse_signals": parse_signals,
            "resume_archetype": resume_archetype,
        }

    def _extract_text(self, filename: str, content_type: str, file_bytes: bytes) -> tuple[str, dict[str, Any]]:
        lower_name = filename.lower()
        if content_type == "application/msword" or (lower_name.endswith(".doc") and not lower_name.endswith(".docx")):
            raise ValueError("Legacy .doc files are not supported yet. Convert the resume to .docx or a text-based PDF and try again.")
        if content_type == "application/pdf" or lower_name.endswith(".pdf"):
            text, layout_meta = self._extract_pdf_layout_aware(file_bytes)
            if len(text.split()) < 20:
                text = f"{text}\n{self._extract_pdf(file_bytes)}".strip()
            if len(text.split()) < 60:
                text = f"{text}\n{self.ocr_service.extract_text_from_pdf(file_bytes)}"
            return text, layout_meta
        if content_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document" or lower_name.endswith(".docx"):
            return self._extract_docx(file_bytes), {"multi_column_detected": False, "page_count": 1}
        if lower_name.endswith((".png", ".jpg", ".jpeg")):
            return self.ocr_service.extract_text_from_image(file_bytes), {"multi_column_detected": False, "page_count": 1}
        return file_bytes.decode("utf-8", errors="ignore"), {"multi_column_detected": False, "page_count": 1}

    def _extract_pdf(self, file_bytes: bytes) -> str:
        try:
            return "\n".join((page.extract_text() or "") for page in PdfReader(io.BytesIO(file_bytes)).pages)
        except Exception:
            return ""

    def _extract_pdf_layout_aware(self, file_bytes: bytes) -> tuple[str, dict[str, Any]]:
        pages_out = []
        multi_column_detected = False
        page_count = 0
        try:
            with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
                page_count = len(pdf.pages)
                for page in pdf.pages:
                    words = page.extract_words(use_text_flow=True, keep_blank_chars=False)
                    if self._looks_multi_column(page=page, words=words):
                        multi_column_detected = True
                        pages_out.append(self._extract_multi_column_page(page=page, words=words))
                        continue
                    words = sorted(words, key=lambda item: (round(float(item["top"]) / 12), float(item["x0"])))
                    lines: dict[int, list[str]] = {}
                    for word in words:
                        lines.setdefault(int(float(word["top"]) / 12), []).append(word["text"])
                    pages_out.append("\n".join(" ".join(parts) for _, parts in sorted(lines.items())))
        except Exception:
            return "", {"multi_column_detected": False, "page_count": 0}
        return "\n\n".join(pages_out), {"multi_column_detected": multi_column_detected, "page_count": page_count}

    def _looks_multi_column(self, *, page, words: list[dict]) -> bool:
        if len(words) < 40:
            return False
        split_x = float(page.width) * 0.58
        left_words = [word for word in words if float(word["x0"]) < split_x]
        right_words = [word for word in words if float(word["x0"]) >= split_x]
        if len(left_words) < 20 or len(right_words) < 12:
            return False
        right_share = len(right_words) / max(len(words), 1)
        return right_share >= 0.18

    def _extract_multi_column_page(self, *, page, words: list[dict]) -> str:
        split_x = float(page.width) * 0.58
        header_cutoff = 110.0
        header_words = [word for word in words if float(word["top"]) < header_cutoff]
        left_words = [word for word in words if float(word["top"]) >= header_cutoff and float(word["x0"]) < split_x]
        right_words = [word for word in words if float(word["top"]) >= header_cutoff and float(word["x0"]) >= split_x]

        segments = [
            self._render_words_as_lines(header_words),
            self._render_words_as_lines(left_words),
            self._render_words_as_lines(right_words),
        ]
        return "\n\n".join(segment for segment in segments if segment.strip())

    def _render_words_as_lines(self, words: list[dict]) -> str:
        if not words:
            return ""
        ordered = sorted(words, key=lambda item: (round(float(item["top"]) / 12), float(item["x0"])))
        lines: dict[int, list[str]] = {}
        for word in ordered:
            lines.setdefault(int(float(word["top"]) / 12), []).append(word["text"])
        return "\n".join(" ".join(parts) for _, parts in sorted(lines.items()))

    def _extract_docx(self, file_bytes: bytes) -> str:
        try:
            return "\n".join(paragraph.text for paragraph in Document(io.BytesIO(file_bytes)).paragraphs)
        except Exception:
            return ""

    def _group_section_lines(self, text: str) -> dict[str, list[str]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        sections: dict[str, list[str]] = {key: [] for key in SECTION_HEADERS}
        current = "summary"
        for line in lines:
            lowered = line.lower().strip(":")
            matched = next((key for key, headers in SECTION_HEADERS.items() if lowered in headers), None)
            if matched:
                current = matched
                continue
            inline_headers = [key for alias, key in HEADER_ALIASES.items() if lowered == alias or lowered.startswith(f"{alias} ")]
            if inline_headers:
                current = inline_headers[0]
                alias = next(alias for alias, key in HEADER_ALIASES.items() if key == current and (lowered == alias or lowered.startswith(f"{alias} ")))
                remainder = line[len(alias):].strip(" :-")
                if remainder and remainder.lower() not in HEADER_ALIASES:
                    sections.setdefault(current, []).append(remainder)
                continue
            compact_headers = [HEADER_ALIASES[token] for token in lowered.replace("&", " ").split() if token in HEADER_ALIASES]
            if compact_headers and len(compact_headers) == len(lowered.replace("&", " ").split()):
                current = compact_headers[0]
                continue
            sections.setdefault(current, []).append(line)
        return {key: value for key, value in sections.items() if value}

    def _split_sections(self, text: str) -> dict[str, str]:
        return {
            key: normalize_whitespace(" ".join(value))
            for key, value in self._group_section_lines(text).items()
            if value
        }

    def _extract_contact(self, text: str) -> dict[str, str | None]:
        email = EMAIL_RE.search(text)
        phone = PHONE_RE.search(text)
        return {"email": email.group(0) if email else None, "phone": phone.group(0) if phone else None}

    def _analyze_parse_signals(
        self,
        *,
        text: str,
        normalized: str,
        sections: dict[str, str],
        grouped_sections: dict[str, list[str]],
        skills: list[str],
        extract_meta: dict[str, Any],
    ) -> dict[str, Any]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        explicit_headers = 0
        inline_headers = 0
        merged_headers = 0
        quantified_lines = 0
        bullet_like_lines = 0

        for line in lines:
            lowered = line.lower().strip(":")
            header_hits = [alias for alias in HEADER_ALIASES if lowered == alias or lowered.startswith(f"{alias} ")]
            if header_hits:
                if any(lowered == alias for alias in header_hits):
                    explicit_headers += 1
                if any(lowered.startswith(f"{alias} ") for alias in header_hits):
                    inline_headers += 1
            compact_hits = [HEADER_ALIASES[token] for token in lowered.replace("&", " ").split() if token in HEADER_ALIASES]
            if len(compact_hits) >= 2:
                merged_headers += 1
            if QUANTIFIED_LINE_RE.search(line):
                quantified_lines += 1
            if BULLET_LINE_RE.search(line):
                bullet_like_lines += 1

        experience_text = sections.get("experience", "")
        education_text = sections.get("education", "")
        section_leakage = 0
        if experience_text and EDUCATION_CONTEXT_RE.search(experience_text):
            section_leakage += 1
        if education_text and WORK_CONTEXT_RE.search(education_text):
            section_leakage += 1

        suspicious_urls = 0
        for url in URL_RE.findall(normalized):
            lowered = url.lower()
            if "linkedin" not in lowered and "github" not in lowered and "http" in lowered:
                suspicious_urls += 1
            if any(marker in lowered for marker in OCR_NOISE_MARKERS):
                suspicious_urls += 1

        suspicious_tokens = {
            token
            for token in normalized.split()
            if "=" in token or any(marker in token.lower() for marker in OCR_NOISE_MARKERS)
        }

        inferred_skills_section = "skills" in sections and not any(line.lower().strip(":") == "skills" for line in lines)
        date_range_count = len(DATE_RANGE_RE.findall(text))
        contact_link_count = sum(
            1
            for url in URL_RE.findall(normalized)
            if any(domain in url.lower() for domain in ("linkedin", "github", "portfolio", "behance", "dribbble"))
        )
        portfolio_link_count = sum(
            1
            for url in URL_RE.findall(normalized)
            if any(domain in url.lower() for domain in ("github", "portfolio", "behance", "dribbble"))
        )

        section_word_counts = {
            "summary": len(sections.get("summary", "").split()),
            "experience": len(sections.get("experience", "").split()),
            "projects": len(sections.get("projects", "").split()),
            "education": len(sections.get("education", "").split()),
            "skills": len(sections.get("skills", "").split()),
            "certifications": len(sections.get("certifications", "").split()),
            "research": len(sections.get("research", "").split()),
            "teaching": len(sections.get("teaching", "").split()),
        }
        focus_total_words = sum(section_word_counts.values())
        dominant_section_share = (
            round(max(section_word_counts.values()) / focus_total_words, 3)
            if focus_total_words
            else 0.0
        )
        evidence_section_words = (
            section_word_counts["experience"]
            + section_word_counts["projects"]
            + section_word_counts["research"]
            + section_word_counts["teaching"]
        )
        skills_focus_share = (
            round(section_word_counts["skills"] / focus_total_words, 3)
            if focus_total_words
            else 0.0
        )
        project_focus_share = (
            round(section_word_counts["projects"] / max(evidence_section_words, 1), 3)
            if evidence_section_words
            else 0.0
        )

        experience_lines = grouped_sections.get("experience", [])
        project_lines = grouped_sections.get("projects", [])
        teaching_lines = grouped_sections.get("teaching", [])
        research_lines = grouped_sections.get("research", [])
        evidence_lines = experience_lines + project_lines + teaching_lines + research_lines

        experience_bullets = sum(1 for line in experience_lines if BULLET_LINE_RE.search(line))
        project_bullets = sum(1 for line in project_lines if BULLET_LINE_RE.search(line))
        evidence_bullets = sum(1 for line in evidence_lines if BULLET_LINE_RE.search(line))
        experience_quantified = sum(1 for line in experience_lines if QUANTIFIED_LINE_RE.search(line))
        project_quantified = sum(1 for line in project_lines if QUANTIFIED_LINE_RE.search(line))
        evidence_quantified = sum(1 for line in evidence_lines if QUANTIFIED_LINE_RE.search(line))
        experience_action_lines = sum(1 for line in experience_lines if ACTION_LINE_RE.search(line))
        project_action_lines = sum(1 for line in project_lines if ACTION_LINE_RE.search(line))
        evidence_action_lines = sum(1 for line in evidence_lines if ACTION_LINE_RE.search(line))
        chronology_signal_count = date_range_count + (1 if YEARS_EXPERIENCE_RE.search(normalized.lower()) else 0)

        section_balance_score = 0.0
        balance_candidates = [
            section_word_counts["summary"],
            section_word_counts["experience"],
            section_word_counts["projects"],
            section_word_counts["skills"],
        ]
        populated_candidates = [count for count in balance_candidates if count > 0]
        if populated_candidates:
            average = sum(populated_candidates) / len(populated_candidates)
            spread = sum(abs(count - average) for count in populated_candidates) / len(populated_candidates)
            section_balance_score = round(max(0.0, 100.0 - min(100.0, (spread / max(average, 1.0)) * 55.0)), 2)

        return {
            "word_count": len(normalized.split()),
            "section_count": len(sections),
            "explicit_header_count": explicit_headers,
            "inline_header_count": inline_headers,
            "merged_header_count": merged_headers,
            "section_leakage_count": section_leakage,
            "suspicious_token_count": len(suspicious_tokens),
            "suspicious_url_count": suspicious_urls,
            "inferred_skills_section": inferred_skills_section,
            "skills_count": len(skills),
            "quantified_line_count": quantified_lines,
            "bullet_like_line_count": bullet_like_lines,
            "date_range_count": date_range_count,
            "contact_link_count": contact_link_count,
            "portfolio_link_count": portfolio_link_count,
            "multi_column_detected": bool(extract_meta.get("multi_column_detected")),
            "page_count": int(extract_meta.get("page_count", 0) or 0),
            "summary_section_word_count": section_word_counts["summary"],
            "experience_section_word_count": section_word_counts["experience"],
            "projects_section_word_count": section_word_counts["projects"],
            "skills_section_word_count": section_word_counts["skills"],
            "certifications_section_word_count": section_word_counts["certifications"],
            "dominant_section_share": dominant_section_share,
            "skills_focus_share": skills_focus_share,
            "project_focus_share": project_focus_share,
            "section_balance_score": section_balance_score,
            "experience_bullet_count": experience_bullets,
            "project_bullet_count": project_bullets,
            "evidence_bullet_count": evidence_bullets,
            "experience_quantified_line_count": experience_quantified,
            "projects_quantified_line_count": project_quantified,
            "evidence_quantified_line_count": evidence_quantified,
            "experience_action_line_count": experience_action_lines,
            "projects_action_line_count": project_action_lines,
            "evidence_action_line_count": evidence_action_lines,
            "chronology_signal_count": chronology_signal_count,
            "academic_marker_count": len(ACADEMIC_MARKER_RE.findall(normalized)),
            "teaching_marker_count": len(TEACHING_MARKER_RE.findall(normalized)),
            "executive_marker_count": len(EXECUTIVE_MARKER_RE.findall(normalized)),
            "creative_marker_count": len(CREATIVE_MARKER_RE.findall(normalized)),
            "technical_portfolio_marker_count": len(TECHNICAL_PORTFOLIO_MARKER_RE.findall(normalized)),
            "certification_marker_count": len(CERTIFICATION_MARKER_RE.findall(normalized)),
            "objective_marker_count": len(OBJECTIVE_MARKER_RE.findall(normalized)),
        }

    def _detect_resume_archetype(
        self,
        *,
        normalized: str,
        sections: dict[str, str],
        parse_signals: dict[str, Any],
        experience_years: float,
    ) -> dict[str, Any]:
        lowered = normalized.lower()
        section_names = set(sections.keys())
        word_count = parse_signals.get("word_count", 0)
        has_links = parse_signals.get("contact_link_count", 0) >= 1
        has_projects = "projects" in section_names
        has_research = bool(section_names & {"research", "publications"})
        skills_words = len(sections.get("skills", "").split())
        experience_words = len(sections.get("experience", "").split())
        summary_words = len(sections.get("summary", "").split())
        certifications_words = len(sections.get("certifications", "").split())
        archetype_labels = {
            "europass_cv": "Europass CV",
            "academic_cv": "Academic CV",
            "government_resume": "Government Resume",
            "teaching_cv": "Teaching CV",
            "executive_cv": "Executive CV",
            "long_form_cv": "Long-Form CV",
            "hybrid_resume": "Hybrid Resume",
            "career_change_resume": "Career-Change Resume",
            "modern_two_column_project_first": "Modern Two-Column Project-First Resume",
            "creative_portfolio_resume": "Creative Portfolio Resume",
            "modern_two_column": "Modern Two-Column Resume",
            "project_first_entry_level": "Project-First Entry-Level Resume",
            "functional_resume": "Functional Resume",
            "skills_first": "Skills-First Resume",
            "technical_portfolio_resume": "Technical Portfolio Resume",
            "one_page_concise": "One-Page Concise Resume",
            "certification_first_resume": "Certification-First Resume",
            "research_transition_resume": "Research-to-Industry Resume",
            "reverse_chronological": "Reverse-Chronological Resume",
            "general_resume": "General Resume",
        }
        scores = {key: 0.0 for key in archetype_labels}
        reasons: dict[str, list[str]] = {key: [] for key in archetype_labels}

        def boost(name: str, points: float, reason: str) -> None:
            scores[name] += points
            if reason not in reasons[name]:
                reasons[name].append(reason)

        academic_sections = {"research", "publications", "teaching", "awards"}
        if "europass" in lowered or "personal information" in lowered or "mother tongue" in lowered:
            boost("europass_cv", 95, "Detected Europass-style markers and section language.")
        if section_names & academic_sections:
            boost("academic_cv", 42, "Detected research, publication, teaching, or awards sections common in academic CVs.")
        if has_research:
            boost("academic_cv", 22, "Research-heavy sections are present in the document.")
        if parse_signals.get("academic_marker_count", 0) >= 2:
            boost("academic_cv", 18, "Academic keywords reinforce a CV-style structure.")
        if "publications" in lowered and parse_signals.get("page_count", 0) >= 2:
            boost("academic_cv", 16, "Publication-heavy multi-page structure aligns with academic CVs.")

        if "clearance" in lowered or "federal" in lowered or "government" in lowered or "citizenship" in lowered:
            boost("government_resume", 72, "Detected federal or compliance-heavy language common in government resumes.")

        if ("teaching" in section_names or parse_signals.get("teaching_marker_count", 0) >= 2) and "education" in section_names:
            boost("teaching_cv", 46, "Detected teaching-specific evidence with education-led structure.")
        if "lecturer" in lowered or "curriculum" in lowered or "lesson planning" in lowered:
            boost("teaching_cv", 22, "Teaching and curriculum language points to a teaching CV.")

        if experience_years >= 8:
            boost("executive_cv", 18, "Seniority level suggests an executive-leaning profile.")
        if parse_signals.get("executive_marker_count", 0) >= 2:
            boost("executive_cv", 36, "Leadership-oriented language is strong in the document.")

        if parse_signals.get("page_count", 0) >= 3 and word_count >= 900 and not has_research:
            boost("long_form_cv", 52, "Detected a long multi-page document without a research-heavy profile.")

        if experience_years >= 4 and has_projects and "skills" in section_names and summary_words >= 18:
            boost("hybrid_resume", 38, "Summary, skills, projects, and experience are all materially present.")
        if experience_words >= 90 and skills_words >= 20 and has_projects:
            boost("hybrid_resume", 16, "Balanced evidence across experience, projects, and skills suggests a hybrid format.")

        if parse_signals.get("objective_marker_count", 0) >= 1 and has_projects and experience_years < 4:
            boost("career_change_resume", 38, "Objective-led framing with projects suggests a transition-focused resume.")

        if parse_signals.get("multi_column_detected"):
            boost("modern_two_column", 34, "Detected a multi-column PDF layout with sidebar-style sections.")
            if has_projects and experience_years < 1.5:
                boost("modern_two_column_project_first", 44, "Projects are carrying a large share of the evidence in a multi-column layout.")
            if has_links and (parse_signals.get("creative_marker_count", 0) >= 2 or "behance" in lowered or "dribbble" in lowered):
                boost("creative_portfolio_resume", 46, "Portfolio links and multi-column layout suggest a creative portfolio resume.")

        if has_projects and experience_years < 1.5 and parse_signals.get("project_focus_share", 0.0) >= 0.34:
            boost("project_first_entry_level", 44, "Projects carry more evidence than formal work history, which is common in fresher resumes.")

        if "skills" in section_names and parse_signals.get("skills_focus_share", 0.0) >= 0.34:
            boost("skills_first", 24, "Skills section dominates the document relative to experience.")
            if experience_words < 45 or parse_signals.get("chronology_signal_count", 0) == 0:
                boost("functional_resume", 42, "Minimal chronology with a dominant skills section suggests a functional resume.")

        if has_projects and has_links and parse_signals.get("technical_portfolio_marker_count", 0) >= 2:
            boost("technical_portfolio_resume", 42, "Project, portfolio, and technical delivery signals indicate a technical portfolio resume.")

        if word_count <= 420 and parse_signals.get("page_count", 0) <= 1 and parse_signals.get("section_count", 0) >= 4:
            boost("one_page_concise", 42, "Compact one-page structure with clear sections suggests a concise resume style.")

        if certifications_words >= 16 or parse_signals.get("certification_marker_count", 0) >= 2:
            boost("certification_first_resume", 26, "Certifications and training are emphasized strongly in this document.")
            if experience_years <= 2:
                boost("certification_first_resume", 14, "Training is emphasized more than work history at this stage.")

        if has_research and experience_years < 3:
            boost("research_transition_resume", 44, "Research-heavy content paired with early-career experience suggests a research-to-industry transition resume.")

        if "experience" in section_names and parse_signals.get("chronology_signal_count", 0) >= 1:
            boost("reverse_chronological", 34, "Experience-led structure with clear timeline signals.")
        if "experience" in section_names and experience_words >= skills_words:
            boost("reverse_chronological", 12, "Work history is at least as prominent as the skills summary.")
        if parse_signals.get("section_balance_score", 0) >= 72 and summary_words >= 12 and "experience" in section_names:
            boost("hybrid_resume", 8, "Section balance is stronger than a single-section-heavy layout.")
        if parse_signals.get("skills_focus_share", 0.0) >= 0.5 and not has_projects:
            boost("skills_first", 10, "The resume leans heavily on the skills block.")

        best_type = max(scores, key=scores.get)
        ordered_scores = sorted(scores.values(), reverse=True)
        best_score = scores[best_type]
        runner_up = ordered_scores[1] if len(ordered_scores) > 1 else 0.0
        score_gap = best_score - runner_up

        if best_score < 26:
            if "experience" in section_names:
                return {
                    "type": "reverse_chronological",
                    "label": archetype_labels["reverse_chronological"],
                    "confidence": 0.64,
                    "reasons": ["Experience-led structure with some timeline evidence."],
                }
            return {
                "type": "general_resume",
                "label": archetype_labels["general_resume"],
                "confidence": 0.58,
                "reasons": ["Detected a general-purpose resume structure."],
            }

        confidence = round(min(0.93, max(0.62, 0.6 + (best_score / 160) + min(score_gap, 24) / 120)), 2)
        chosen_reasons = reasons[best_type][:3] or ["Detected a general-purpose resume structure."]
        return {
            "type": best_type,
            "label": archetype_labels[best_type],
            "confidence": confidence,
            "reasons": chosen_reasons,
        }

    def _estimate_experience_years(self, text: str, sections: dict[str, str]) -> float:
        focus_keys = {"experience", "projects", "summary", "teaching", "research"}
        section_focus = normalize_whitespace(
            " ".join(section_text for key, section_text in sections.items() if key in focus_keys)
        )
        relevant_text = (section_focus or text).replace("\u2013", "-").replace("\u2014", "-")
        lowered = relevant_text.lower()

        direct = YEARS_EXPERIENCE_RE.search(lowered)
        direct_years = round(float(direct.group(1)), 2) if direct else 0.0

        direct_months = MONTHS_EXPERIENCE_RE.search(lowered)
        direct_month_years = round(int(direct_months.group(1)) / 12, 2) if direct_months else 0.0

        primary_section_intervals: list[tuple[date, date]] = []
        project_section_intervals: list[tuple[date, date]] = []
        weighted_project_months = 0.0

        for key, section_text in sections.items():
            if key not in focus_keys:
                continue
            normalized_section = normalize_whitespace(section_text).replace("\u2013", "-").replace("\u2014", "-")
            if not normalized_section:
                continue

            target_bucket = primary_section_intervals
            if key in {"projects", "summary"}:
                target_bucket = project_section_intervals

            for match in DATE_RANGE_RE.finditer(normalized_section):
                context = normalized_section[max(0, match.start() - 120): min(len(normalized_section), match.end() + 120)]
                has_work_signal = key in {"experience", "teaching", "research"} or bool(WORK_CONTEXT_RE.search(context))
                has_project_signal = key == "projects" or bool(PROJECT_CONTEXT_RE.search(context))
                has_education_signal = bool(EDUCATION_CONTEXT_RE.search(context))
                if not (has_work_signal or has_project_signal):
                    continue
                if has_education_signal and key not in {"projects", "summary"} and not has_work_signal:
                    continue

                start_raw, end_raw = match.groups()
                start_date = self._parse_resume_date(start_raw)
                end_date = self._parse_resume_date(end_raw)
                if start_date and end_date and start_date <= end_date:
                    target_bucket.append((start_date, end_date))

        if project_section_intervals:
            merged_projects = self._merge_date_ranges(project_section_intervals)
            for start_date, end_date in merged_projects:
                months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1
                weighted_project_months += max(months, 0) * 0.55

        primary_months = 0
        if primary_section_intervals:
            merged_primary = self._merge_date_ranges(primary_section_intervals)
            for start_date, end_date in merged_primary:
                months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1
                primary_months += max(months, 0)

        interval_years = round((primary_months + min(weighted_project_months, 12)) / 12, 2)
        if interval_years <= 0:
            return max(direct_years, direct_month_years)

        return round(max(interval_years, direct_years, direct_month_years), 2)

    def _parse_resume_date(self, value: str) -> date | None:
        token = value.strip().lower()
        if token in {"present", "current", "now"}:
            today = date.today()
            return date(today.year, today.month, 1)

        month_lookup = {
            "jan": 1,
            "january": 1,
            "feb": 2,
            "february": 2,
            "mar": 3,
            "march": 3,
            "apr": 4,
            "april": 4,
            "may": 5,
            "jun": 6,
            "june": 6,
            "jul": 7,
            "july": 7,
            "aug": 8,
            "august": 8,
            "sep": 9,
            "sept": 9,
            "september": 9,
            "oct": 10,
            "october": 10,
            "nov": 11,
            "november": 11,
            "dec": 12,
            "december": 12,
        }

        month_match = re.fullmatch(rf"({MONTH_NAME_RE})\s+(\d{{4}})", token, re.IGNORECASE)
        if month_match:
            month = month_lookup[month_match.group(1).lower()]
            year = int(month_match.group(2))
            return date(year, month, 1)

        numeric_match = re.fullmatch(r"(\d{1,2})/(\d{4})", token)
        if numeric_match:
            month = int(numeric_match.group(1))
            year = int(numeric_match.group(2))
            if 1 <= month <= 12:
                return date(year, month, 1)
            return None

        year_match = re.fullmatch(r"(19|20)\d{2}", token)
        if year_match:
            return date(int(token), 1, 1)

        return None

    def _merge_date_ranges(self, intervals: list[tuple[date, date]]) -> list[tuple[date, date]]:
        if not intervals:
            return []
        ordered = sorted(intervals, key=lambda item: item[0])
        merged = [ordered[0]]
        for start_date, end_date in ordered[1:]:
            last_start, last_end = merged[-1]
            if start_date <= last_end:
                merged[-1] = (last_start, max(last_end, end_date))
            else:
                merged.append((start_date, end_date))
        return merged

