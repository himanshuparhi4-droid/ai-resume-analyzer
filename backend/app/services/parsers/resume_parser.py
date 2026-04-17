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
    r"\b(intern|internship|analyst|engineer|developer|consultant|associate|scientist|systems|techworld|solutions|ltd|pvt|company|corp)\b",
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
        sections = self._split_sections(text)
        if "skills" not in sections and re.search(r"\bskills\b", normalized, re.IGNORECASE) and skills:
            sections["skills"] = ", ".join(skills[:12])
        experience_years = self._estimate_experience_years(normalized, sections)
        parse_signals = self._analyze_parse_signals(text=text, normalized=normalized, sections=sections, skills=skills, extract_meta=extract_meta)
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

    def _split_sections(self, text: str) -> dict[str, str]:
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
        return {key: normalize_whitespace(" ".join(value)) for key, value in sections.items() if value}

    def _extract_contact(self, text: str) -> dict[str, str | None]:
        email = EMAIL_RE.search(text)
        phone = PHONE_RE.search(text)
        return {"email": email.group(0) if email else None, "phone": phone.group(0) if phone else None}

    def _analyze_parse_signals(self, *, text: str, normalized: str, sections: dict[str, str], skills: list[str], extract_meta: dict[str, Any]) -> dict[str, Any]:
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
            "multi_column_detected": bool(extract_meta.get("multi_column_detected")),
            "page_count": int(extract_meta.get("page_count", 0) or 0),
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
        reasons: list[str] = []
        word_count = parse_signals.get("word_count", 0)
        has_links = parse_signals.get("contact_link_count", 0) >= 1
        has_projects = "projects" in section_names
        has_research = bool(section_names & {"research", "publications"})
        skills_words = len(sections.get("skills", "").split())
        experience_words = len(sections.get("experience", "").split())
        summary_words = len(sections.get("summary", "").split())
        certifications_words = len(sections.get("certifications", "").split())

        if "europass" in lowered or "personal information" in lowered or "mother tongue" in lowered:
            reasons.append("Detected Europass-style markers and section language.")
            return {"type": "europass_cv", "label": "Europass CV", "confidence": 0.82, "reasons": reasons}

        academic_sections = {"research", "publications", "teaching", "awards"}
        if section_names & academic_sections or ("publications" in lowered and parse_signals.get("page_count", 0) >= 2):
            reasons.append("Detected research, publication, teaching, or awards sections common in academic CVs.")
            return {"type": "academic_cv", "label": "Academic CV", "confidence": 0.86, "reasons": reasons}

        if "clearance" in lowered or "federal" in lowered or "government" in lowered or "citizenship" in lowered:
            reasons.append("Detected federal or compliance-heavy language common in government resumes.")
            return {"type": "government_resume", "label": "Government Resume", "confidence": 0.77, "reasons": reasons}

        if ("teaching" in section_names or "lesson planning" in lowered or "curriculum" in lowered) and "education" in section_names:
            reasons.append("Detected teaching-specific evidence with education-led structure.")
            return {"type": "teaching_cv", "label": "Teaching CV", "confidence": 0.8, "reasons": reasons}

        if experience_years >= 8 and ("leadership" in lowered or "director" in lowered or "vp" in lowered or "head of" in lowered):
            reasons.append("Detected seniority and leadership-oriented language typical of executive CVs.")
            return {"type": "executive_cv", "label": "Executive CV", "confidence": 0.84, "reasons": reasons}

        if parse_signals.get("page_count", 0) >= 3 and word_count >= 900 and not has_research:
            reasons.append("Detected a long-form multi-page document without a research-heavy profile.")
            return {"type": "long_form_cv", "label": "Long-Form CV", "confidence": 0.75, "reasons": reasons}

        if experience_years >= 5 and has_projects and "skills" in section_names and summary_words >= 18:
            reasons.append("Detected a combined summary, skills, and experience structure typical of hybrid resumes.")
            return {"type": "hybrid_resume", "label": "Hybrid Resume", "confidence": 0.8, "reasons": reasons}

        if "objective" in lowered and has_projects and experience_years < 3 and "experience" in section_names:
            reasons.append("Detected transition-style summary language with mixed project and experience evidence.")
            return {"type": "career_change_resume", "label": "Career-Change Resume", "confidence": 0.72, "reasons": reasons}

        if parse_signals.get("multi_column_detected"):
            reasons.append("Detected a multi-column PDF layout with sidebar-style sections.")
            if "projects" in section_names and experience_years < 1:
                reasons.append("Projects are carrying a large share of the evidence, which is common in modern student resumes.")
                return {"type": "modern_two_column_project_first", "label": "Modern Two-Column Project-First Resume", "confidence": 0.83, "reasons": reasons}
            if has_links and ("behance" in lowered or "dribbble" in lowered or "portfolio" in lowered):
                reasons.append("Portfolio links and multi-column layout suggest a creative portfolio resume.")
                return {"type": "creative_portfolio_resume", "label": "Creative Portfolio Resume", "confidence": 0.82, "reasons": reasons}
            return {"type": "modern_two_column", "label": "Modern Two-Column Resume", "confidence": 0.8, "reasons": reasons}

        if has_projects and experience_years < 1:
            reasons.append("Projects carry more evidence than formal work history, which is common in fresher resumes.")
            return {"type": "project_first_entry_level", "label": "Project-First Entry-Level Resume", "confidence": 0.78, "reasons": reasons}

        if "skills" in section_names and (experience_words == 0 or skills_words > max(50, experience_words * 0.7)):
            reasons.append("Skills section dominates the document relative to experience.")
            if experience_words < 40:
                reasons.append("Minimal chronology with a dominant skills section suggests a functional resume.")
                return {"type": "functional_resume", "label": "Functional Resume", "confidence": 0.79, "reasons": reasons}
            return {"type": "skills_first", "label": "Skills-First Resume", "confidence": 0.72, "reasons": reasons}

        if has_projects and has_links and ("github" in lowered or "portfolio" in lowered):
            reasons.append("Strong project and portfolio signals indicate a technical portfolio resume.")
            return {"type": "technical_portfolio_resume", "label": "Technical Portfolio Resume", "confidence": 0.77, "reasons": reasons}

        if word_count <= 420 and parse_signals.get("page_count", 0) <= 1 and parse_signals.get("section_count", 0) >= 4:
            reasons.append("Compact one-page structure with clear sections suggests a concise resume style.")
            return {"type": "one_page_concise", "label": "One-Page Concise Resume", "confidence": 0.74, "reasons": reasons}

        if certifications_words >= 16 and experience_years <= 2:
            reasons.append("Certifications and training are emphasized more than work history.")
            return {"type": "certification_first_resume", "label": "Certification-First Resume", "confidence": 0.76, "reasons": reasons}

        if has_research and experience_years < 3:
            reasons.append("Research-heavy content paired with early-career experience suggests a research-to-industry transition resume.")
            return {"type": "research_transition_resume", "label": "Research-to-Industry Resume", "confidence": 0.78, "reasons": reasons}

        if "experience" in section_names:
            reasons.append("Experience-led structure with clear timeline sections.")
            return {"type": "reverse_chronological", "label": "Reverse-Chronological Resume", "confidence": 0.74, "reasons": reasons}

        return {"type": "general_resume", "label": "General Resume", "confidence": 0.6, "reasons": ["Detected a general-purpose resume structure."]}

    def _estimate_experience_years(self, text: str, sections: dict[str, str]) -> float:
        section_focus = normalize_whitespace(
            " ".join(
                section_text
                for key, section_text in sections.items()
                if key in {"experience", "projects", "summary"}
            )
        )
        relevant_text = (section_focus or text).replace("\u2013", "-").replace("\u2014", "-")
        lowered = relevant_text.lower()

        direct = YEARS_EXPERIENCE_RE.search(lowered)
        if direct:
            return round(float(direct.group(1)), 2)

        direct_months = MONTHS_EXPERIENCE_RE.search(lowered)
        if direct_months:
            return round(int(direct_months.group(1)) / 12, 2)

        intervals = []
        for match in DATE_RANGE_RE.finditer(relevant_text):
            context = relevant_text[max(0, match.start() - 120): min(len(relevant_text), match.end() + 120)]
            has_work_signal = bool(WORK_CONTEXT_RE.search(context))
            has_education_signal = bool(EDUCATION_CONTEXT_RE.search(context))
            if not has_work_signal or (has_education_signal and not has_work_signal):
                continue

            start_raw, end_raw = match.groups()
            start_date = self._parse_resume_date(start_raw)
            end_date = self._parse_resume_date(end_raw)
            if start_date and end_date and start_date <= end_date:
                intervals.append((start_date, end_date))

        if not intervals:
            return 0.0

        merged = self._merge_date_ranges(intervals)
        total_months = 0
        for start_date, end_date in merged:
            months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1
            total_months += max(months, 0)

        return round(total_months / 12, 2)

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

