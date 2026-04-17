from datetime import datetime

from pydantic import BaseModel


class HistoryItem(BaseModel):
    analysis_id: str
    role_query: str
    overall_score: float
    created_at: datetime
    share_token: str | None = None


class ComparisonResponse(BaseModel):
    current_analysis_id: str
    previous_analysis_id: str | None = None
    score_delta: float
    component_deltas: dict[str, float]
    summary: str


class DeleteResponse(BaseModel):
    message: str
