from __future__ import annotations

from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.jobs.aggregator import JobAggregator


class JobSyncService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.aggregator = JobAggregator(db)

    async def sync_defaults(self) -> dict:
        synced_jobs = 0
        synced_queries = 0
        for query in settings.sync_default_queries:
            for location in settings.sync_default_locations:
                items = await self.aggregator.fetch_jobs(query=query, location=location, limit=settings.fetch_limit, force_refresh=True)
                synced_queries += 1
                synced_jobs += len(items)
        return {
            "synced_queries": synced_queries,
            "synced_jobs": synced_jobs,
            "cache_ttl_minutes": settings.job_cache_ttl_minutes,
        }
