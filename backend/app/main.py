from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from app.api.router import api_router
from app.core.config import settings
from app.core.database import Base, SessionLocal, engine
from app.core.logging import RequestLoggingMiddleware, configure_logging
from app.core.rate_limit import limiter
from app.models import analysis, job, resume, user  # noqa: F401
from app.services.jobs.sync import JobSyncService

scheduler = AsyncIOScheduler()
BUILD_TAG = "2026-04-19-livefetch-debug-11"


async def _scheduled_sync() -> None:
    db = SessionLocal()
    try:
        await JobSyncService(db).sync_defaults()
    finally:
        db.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    configure_logging()
    if settings.environment != "production":
        Base.metadata.create_all(bind=engine)
    if settings.enable_internal_scheduler and not scheduler.running:
        scheduler.add_job(_scheduled_sync, "interval", minutes=settings.sync_interval_minutes, id="job-sync", replace_existing=True)
        scheduler.start()
    yield
    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(title=settings.app_name, version="2.0.0", description="AI-powered resume analysis backend.", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_origin_regex=settings.cors_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(api_router, prefix=settings.api_prefix)


@app.get("/", tags=["root"])
def root() -> dict[str, str]:
    return {"message": "AI Resume Analyzer API is running.", "docs": "/docs", "build_tag": BUILD_TAG}


@app.get("/healthz", tags=["root"])
def healthz() -> dict[str, str]:
    return {"status": "ok", "build_tag": BUILD_TAG}
