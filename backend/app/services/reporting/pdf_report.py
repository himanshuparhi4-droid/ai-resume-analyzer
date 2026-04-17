from __future__ import annotations

from io import BytesIO

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

from app.schemas.analysis import AnalysisResponse


class PDFReportService:
    def build_report(self, analysis: AnalysisResponse) -> bytes:
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, title="Resume Analysis Report")
        styles = getSampleStyleSheet()
        story = [
            Paragraph("Resume Analysis Report", styles["Title"]),
            Spacer(1, 12),
            Paragraph(f"Target Role: {analysis.role_query}", styles["Heading2"]),
            Paragraph(f"Overall Score: {analysis.overall_score}/100", styles["BodyText"]),
            Spacer(1, 12),
            Paragraph("Matched Skills", styles["Heading2"]),
            Paragraph(", ".join(analysis.matched_skills) or "No strong overlap found.", styles["BodyText"]),
            Spacer(1, 12),
            Paragraph("Missing Skills", styles["Heading2"]),
            Paragraph(", ".join(item["skill"] for item in analysis.missing_skills) or "No critical gaps found.", styles["BodyText"]),
            Spacer(1, 12),
            Paragraph("Recommendations", styles["Heading2"]),
        ]
        for item in analysis.recommendations:
            story.append(Paragraph(f"- {item.title}: {item.detail}", styles["BodyText"]))
        doc.build(story)
        return buffer.getvalue()
