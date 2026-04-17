from fastapi import APIRouter

from app.api.routes import analyses, auth, health, jobs, public, reports, rewrite, users

api_router = APIRouter()
api_router.include_router(health.router, tags=["health"])
api_router.include_router(auth.router, tags=["auth"])
api_router.include_router(users.router, tags=["users"])
api_router.include_router(public.router, tags=["public"])
api_router.include_router(reports.router, tags=["reports"])
api_router.include_router(rewrite.router, tags=["rewrite"])
api_router.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
api_router.include_router(analyses.router, prefix="/analyses", tags=["analyses"])
