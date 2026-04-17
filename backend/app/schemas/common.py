from pydantic import BaseModel


class ScoreBreakdown(BaseModel):
    skill_match: float
    semantic_match: float
    experience_match: float
    market_demand: float
    resume_quality: float
    ats_compliance: float
