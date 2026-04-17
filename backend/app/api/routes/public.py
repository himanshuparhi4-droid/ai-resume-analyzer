from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.schemas.analysis import AnalysisResponse
from app.services.analysis.orchestrator import AnalysisOrchestrator

router = APIRouter(prefix="/public")


@router.get("/analyses/{share_token}", response_model=AnalysisResponse)
def public_analysis(share_token: str, db: Session = Depends(get_db)) -> AnalysisResponse:
    analysis = AnalysisOrchestrator(db).get_public_analysis(share_token)
    if not analysis:
        raise HTTPException(status_code=404, detail="Public analysis not found")
    return analysis
