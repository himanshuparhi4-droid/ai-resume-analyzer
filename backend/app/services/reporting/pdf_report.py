from __future__ import annotations

from io import BytesIO
from xml.sax.saxutils import escape

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

from app.schemas.analysis import AnalysisResponse


class PDFReportService:
    @staticmethod
    def _text(value: object) -> str:
        return escape(str(value or "").strip())

    @staticmethod
    def _skills(items: list[dict], *, include_weak_proof: bool) -> str:
        skills = [
            str(item.get("skill", "")).strip()
            for item in items
            if bool(item.get("signal_source") == "weak-resume-proof") == include_weak_proof and str(item.get("skill", "")).strip()
        ]
        return ", ".join(skills) if skills else "No major items found."

    @staticmethod
    def _skill_names(items: list[dict]) -> str:
        skills = [str(item.get("skill", "")).strip() for item in items if str(item.get("skill", "")).strip()]
        return ", ".join(skills) if skills else "No major items found."

    def build_report(self, analysis: AnalysisResponse) -> bytes:
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, title="Resume Analysis Report")
        styles = getSampleStyleSheet()
        strengths = analysis.ai_summary.get("strengths") if isinstance(analysis.ai_summary, dict) else []
        weaknesses = analysis.ai_summary.get("weaknesses") if isinstance(analysis.ai_summary, dict) else []
        weak_proofs = analysis.weak_skill_proofs or [item for item in analysis.missing_skills if item.get("signal_source") == "weak-resume-proof"]
        top_jobs = analysis.top_job_matches[:5]
        story = [
            Paragraph("Resume Analysis Report", styles["Title"]),
            Spacer(1, 12),
            Paragraph(f"Target Role: {self._text(analysis.role_query)}", styles["Heading2"]),
            Paragraph(f"Overall Score: {round(float(analysis.overall_score), 2)}/100", styles["BodyText"]),
            Paragraph(f"Market Location: {self._text(analysis.analysis_context.get('target_location', 'Not specified'))}", styles["BodyText"]),
            Spacer(1, 12),
            Paragraph("Score Breakdown", styles["Heading2"]),
            Paragraph(
                self._text(
                    ", ".join(
                        f"{key.replace('_', ' ').title()}: {round(float(value), 2)}/100"
                        for key, value in analysis.breakdown.model_dump().items()
                    )
                ),
                styles["BodyText"],
            ),
            Spacer(1, 12),
            Paragraph("Matched Skills", styles["Heading2"]),
            Paragraph(self._text(", ".join(analysis.matched_skills) or "No strong overlap found."), styles["BodyText"]),
            Spacer(1, 12),
            Paragraph("Missing Skills", styles["Heading2"]),
            Paragraph(self._text(self._skills(analysis.missing_skills, include_weak_proof=False)), styles["BodyText"]),
            Spacer(1, 12),
            Paragraph("Skills Needing Stronger Proof", styles["Heading2"]),
            Paragraph(self._text(self._skill_names(weak_proofs)), styles["BodyText"]),
            Spacer(1, 12),
            Paragraph("Strengths", styles["Heading2"]),
            *[Paragraph(f"- {self._text(item)}", styles["BodyText"]) for item in strengths[:5]],
            Spacer(1, 12),
            Paragraph("Weaknesses", styles["Heading2"]),
            *[Paragraph(f"- {self._text(item)}", styles["BodyText"]) for item in weaknesses[:5]],
            Spacer(1, 12),
            Paragraph("Recommendations", styles["Heading2"]),
        ]
        for item in analysis.recommendations:
            story.append(Paragraph(f"- {self._text(item.title)} ({self._text(item.impact)}): {self._text(item.detail)}", styles["BodyText"]))
        story.extend([Spacer(1, 12), Paragraph("Top Job Matches", styles["Heading2"])])
        for index, job in enumerate(top_jobs, start=1):
            story.append(Paragraph(f"{index}. {self._text(job.title)} at {self._text(job.company)} - {self._text(job.location)}", styles["BodyText"]))
        doc.build(story)
        return buffer.getvalue()
