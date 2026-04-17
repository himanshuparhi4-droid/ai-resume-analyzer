from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.schemas.rewrite import BulletRewriteRequest, BulletRewriteResponse
from app.services.analysis.rewrite import BulletRewriteService

router = APIRouter(prefix="/rewrite")


@router.post("/bullet", response_model=BulletRewriteResponse)
def rewrite_bullet(payload: BulletRewriteRequest, db: Session = Depends(get_db)) -> BulletRewriteResponse:
    return BulletRewriteResponse(**BulletRewriteService(db).rewrite(payload.analysis_id, payload.bullet))
