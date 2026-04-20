from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import desc, or_, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.job import JobListing
from app.services.jobs.taxonomy import dedupe_key, normalize_role


class JobCacheService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_cached_jobs(self, query: str, location: str, limit: int) -> list[dict]:
        normalized_role = normalize_role(query)
        now = datetime.now(UTC).replace(tzinfo=None)
        stmt = (
            select(JobListing)
            .where(
                JobListing.normalized_role == normalized_role,
                JobListing.location_query == location,
                or_(JobListing.cached_until.is_(None), JobListing.cached_until > now),
            )
            .order_by(desc(JobListing.last_seen_at))
            .limit(limit)
        )
        return [self._to_dict(item) for item in self.db.scalars(stmt).all()]

    def get_cached_jobs_any_location(self, query: str, limit: int) -> list[dict]:
        normalized_role = normalize_role(query)
        now = datetime.now(UTC).replace(tzinfo=None)
        stmt = (
            select(JobListing)
            .where(
                JobListing.normalized_role == normalized_role,
                or_(JobListing.cached_until.is_(None), JobListing.cached_until > now),
            )
            .order_by(desc(JobListing.last_seen_at))
            .limit(limit)
        )
        return [self._to_dict(item) for item in self.db.scalars(stmt).all()]

    def store_jobs(self, *, jobs: list[dict], query: str, location: str) -> list[dict]:
        normalized_role = normalize_role(query)
        now = datetime.now(UTC).replace(tzinfo=None)
        cached_until = now + timedelta(minutes=settings.job_cache_ttl_minutes)
        stored: list[dict] = []
        seen_keys: set[str] = set()
        for job in jobs:
            dedupe = dedupe_key(job)
            if dedupe in seen_keys:
                continue
            seen_keys.add(dedupe)
            existing = self.db.scalar(
                select(JobListing).where(
                    JobListing.source == job["source"],
                    JobListing.external_id == job.get("external_id"),
                )
            )
            if existing:
                existing.title = job["title"]
                existing.company = job["company"]
                existing.location = job["location"]
                existing.remote = job["remote"]
                existing.url = job["url"]
                existing.description = job["description"]
                existing.tags = job["tags"]
                existing.normalized_data = job["normalized_data"]
                existing.posted_at = job.get("posted_at")
                existing.query_role = query
                existing.normalized_role = normalized_role
                existing.location_query = location
                existing.cached_until = cached_until
                existing.last_seen_at = now
                stored.append(self._to_dict(existing))
                continue
            model = JobListing(
                source=job["source"],
                external_id=job.get("external_id"),
                title=job["title"],
                company=job["company"],
                location=job["location"],
                remote=job["remote"],
                url=job["url"],
                description=job["description"],
                tags=job["tags"],
                normalized_data=job["normalized_data"],
                query_role=query,
                normalized_role=normalized_role,
                location_query=location,
                posted_at=job.get("posted_at"),
                cached_until=cached_until,
                last_seen_at=now,
            )
            self.db.add(model)
            self.db.flush()
            stored.append(self._to_dict(model))
        self.db.commit()
        return stored

    def _to_dict(self, item: JobListing) -> dict:
        return {
            "id": item.id,
            "source": item.source,
            "external_id": item.external_id,
            "title": item.title,
            "company": item.company,
            "location": item.location,
            "remote": item.remote,
            "url": item.url,
            "description": item.description,
            "tags": item.tags,
            "normalized_data": item.normalized_data,
            "posted_at": item.posted_at,
            "query_role": item.query_role,
            "normalized_role": item.normalized_role,
            "location_query": item.location_query,
        }
