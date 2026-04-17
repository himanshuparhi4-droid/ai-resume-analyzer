from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.database import get_db
from app.models.user import User
from app.schemas.user import ComparisonResponse, DeleteResponse, HistoryItem
from app.services.analysis.orchestrator import AnalysisOrchestrator

router = APIRouter(prefix="/users/me")


@router.get("/analyses", response_model=list[HistoryItem])
def my_analyses(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> list[HistoryItem]:
    history = AnalysisOrchestrator(db).list_user_history(current_user)
    return [HistoryItem(analysis_id=item.id, role_query=item.role_query, overall_score=item.overall_score, created_at=item.created_at, share_token=item.share_token) for item in history]


@router.get("/analyses/compare", response_model=ComparisonResponse)
def compare_analyses(current_id: str, previous_id: str | None = None, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> ComparisonResponse:
    try:
        result = AnalysisOrchestrator(db).compare(user=current_user, current_id=current_id, previous_id=previous_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return ComparisonResponse(**result)


@router.delete("/analyses/{analysis_id}", response_model=DeleteResponse)
def delete_analysis(analysis_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> DeleteResponse:
    if not AnalysisOrchestrator(db).delete_analysis(user=current_user, analysis_id=analysis_id):
        raise HTTPException(status_code=404, detail="Analysis not found")
    return DeleteResponse(message="Analysis deleted")


@router.delete("/data", response_model=DeleteResponse)
def delete_my_data(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> DeleteResponse:
    AnalysisOrchestrator(db).delete_user_data(current_user)
    return DeleteResponse(message="User account and related data deleted")
