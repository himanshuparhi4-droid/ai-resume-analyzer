from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import verify_sync_secret
from app.core.database import get_db
from app.schemas.job import JobSearchResponse, JobSyncResponse
from app.services.jobs.aggregator import JobAggregator
from app.services.jobs.sync import JobSyncService

router = APIRouter()


@router.get("/search", response_model=JobSearchResponse)
async def search_jobs(query: str = Query(..., min_length=2), location: str = Query("India"), limit: int = Query(12, ge=1, le=30), db: Session = Depends(get_db)) -> JobSearchResponse:
    jobs = await JobAggregator(db).fetch_jobs(query=query, location=location, limit=limit)
    if not jobs:
        raise HTTPException(status_code=404, detail="No jobs found for the supplied role or tag.")
    return JobSearchResponse(query=query, total=len(jobs), items=jobs)


@router.post("/sync", response_model=JobSyncResponse, dependencies=[Depends(verify_sync_secret)])
async def sync_jobs(db: Session = Depends(get_db)) -> JobSyncResponse:
    return JobSyncResponse(**(await JobSyncService(db).sync_defaults()))
