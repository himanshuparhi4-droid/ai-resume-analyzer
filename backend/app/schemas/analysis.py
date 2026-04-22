from datetime import datetime

from pydantic import BaseModel

from app.schemas.common import ScoreBreakdown
from app.schemas.job import JobListingOut


class RecommendationItem(BaseModel):
    title: str
    detail: str
    impact: str


class AnalysisUploadRequest(BaseModel):
    filename: str
    content_type: str
    file_base64: str
    role_query: str
    location: str = "Global"
    limit: int = 12


class AnalysisResponse(BaseModel):
    analysis_id: str
    role_query: str
    overall_score: float
    breakdown: ScoreBreakdown
    matched_skills: list[str]
    missing_skills: list[dict]
    matched_skill_details: list[dict] = []
    missing_skill_details: list[dict] = []
    market_skill_frequency: list[dict] = []
    top_job_matches: list[JobListingOut]
    analysis_context: dict = {}
    resume_archetype: dict = {}
    component_feedback: dict = {}
    recommendations: list[RecommendationItem]
    ai_summary: dict
    resume_sections: dict
    resume_preview: str
    share_token: str | None = None
    created_at: datetime | None = None
