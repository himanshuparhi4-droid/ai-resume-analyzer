import logging
from base64 import b64decode

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from sqlalchemy.orm import Session

from app.api.deps import get_optional_user
from app.core.config import settings
from app.core.database import get_db
from app.models.user import User
from app.schemas.analysis import AnalysisResponse, AnalysisUploadRequest
from app.services.analysis.orchestrator import AnalysisOrchestrator

router = APIRouter()
logger = logging.getLogger(__name__)


def _extract_request_context(request: Request) -> dict:
    forwarded_for = request.headers.get("x-forwarded-for", "")
    user_ip = forwarded_for.split(",", 1)[0].strip() if forwarded_for else ""
    if not user_ip:
        user_ip = (request.headers.get("x-real-ip") or "").strip()
    if not user_ip and request.client:
        user_ip = request.client.host or ""
    return {
        "user_ip": user_ip,
        "user_agent": request.headers.get("user-agent", ""),
        "referer": request.headers.get("referer", ""),
    }


@router.post("/resume", response_model=AnalysisResponse)
async def analyze_resume(request: Request, resume: UploadFile = File(...), role_query: str = Form(...), location: str = Form("India"), limit: int = Form(12), db: Session = Depends(get_db), current_user: User | None = Depends(get_optional_user)) -> AnalysisResponse:
    file_bytes = await resume.read()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    try:
        return await AnalysisOrchestrator(db).analyze_resume(
            filename=resume.filename or "resume.pdf",
            content_type=resume.content_type or "application/octet-stream",
            file_bytes=file_bytes,
            role_query=role_query,
            location=location,
            limit=limit,
            user=current_user,
            request_context=_extract_request_context(request),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Resume analysis failed")
        detail = f"Analysis failed internally: {exc}" if settings.environment == "development" else "Analysis failed internally."
        raise HTTPException(status_code=500, detail=detail) from exc


@router.post("/resume-json", response_model=AnalysisResponse)
async def analyze_resume_json(request: Request, payload: AnalysisUploadRequest, db: Session = Depends(get_db), current_user: User | None = Depends(get_optional_user)) -> AnalysisResponse:
    try:
        file_bytes = b64decode(payload.file_base64)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Uploaded file encoding is invalid.") from exc
    if not file_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    try:
        return await AnalysisOrchestrator(db).analyze_resume(
            filename=payload.filename or "resume.pdf",
            content_type=payload.content_type or "application/octet-stream",
            file_bytes=file_bytes,
            role_query=payload.role_query,
            location=payload.location,
            limit=payload.limit,
            user=current_user,
            request_context=_extract_request_context(request),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("JSON resume analysis failed")
        detail = f"Analysis failed internally: {exc}" if settings.environment == "development" else "Analysis failed internally."
        raise HTTPException(status_code=500, detail=detail) from exc


@router.get("/{analysis_id}", response_model=AnalysisResponse)
def get_analysis(analysis_id: str, db: Session = Depends(get_db)) -> AnalysisResponse:
    analysis = AnalysisOrchestrator(db).get_analysis(analysis_id)
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found.")
    return analysis
