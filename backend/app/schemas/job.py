from datetime import datetime

from pydantic import BaseModel, HttpUrl


class JobListingOut(BaseModel):
    id: str | None = None
    source: str
    external_id: str | None = None
    title: str
    company: str
    location: str
    remote: bool
    url: HttpUrl | str
    description: str
    tags: list[str]
    normalized_data: dict
    posted_at: datetime | None = None
    preview: str | None = None
    relevance_score: float | None = None


class JobSearchResponse(BaseModel):
    query: str
    total: int
    items: list[JobListingOut]


class JobSyncResponse(BaseModel):
    synced_queries: int
    synced_jobs: int
    cache_ttl_minutes: int
