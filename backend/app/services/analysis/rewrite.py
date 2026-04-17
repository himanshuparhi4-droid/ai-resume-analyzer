from __future__ import annotations

from sqlalchemy.orm import Session

from app.models.analysis import AnalysisRun


class BulletRewriteService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def rewrite(self, analysis_id: str, bullet: str) -> dict:
        analysis = self.db.get(AnalysisRun, analysis_id)
        missing_skills = (analysis.missing_skills if analysis else [])[:3] if analysis else []
        matched_skills = (analysis.matched_skills if analysis else [])[:3] if analysis else []
        notes = []
        if missing_skills:
            notes.append("High-demand missing skills: " + ", ".join(item["skill"] for item in missing_skills))
        if matched_skills:
            notes.append("Keep proven strengths visible: " + ", ".join(matched_skills))
        notes.append("Use action + impact + metric format grounded in real work.")
        rewritten = bullet.strip()
        if not any(char.isdigit() for char in rewritten):
            rewritten = f"Improved {rewritten.lower()} to deliver measurable business impact and stronger execution evidence."
        else:
            rewritten = f"Led and optimized {rewritten.lower()} while making the metric-driven outcome easier to notice."
        return {
            "original_bullet": bullet,
            "rewritten_bullet": rewritten[0].upper() + rewritten[1:],
            "grounding_notes": notes,
            "confidence": "medium",
        }
