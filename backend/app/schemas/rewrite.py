from pydantic import BaseModel


class BulletRewriteRequest(BaseModel):
    bullet: str
    analysis_id: str


class BulletRewriteResponse(BaseModel):
    original_bullet: str
    rewritten_bullet: str
    grounding_notes: list[str]
    confidence: str
