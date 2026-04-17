from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.analysis.orchestrator import AnalysisOrchestrator
from app.services.reporting.pdf_report import PDFReportService

router = APIRouter(prefix="/reports")


@router.get("/analyses/{analysis_id}.pdf")
def download_report(analysis_id: str, db: Session = Depends(get_db)) -> Response:
    analysis = AnalysisOrchestrator(db).get_analysis(analysis_id)
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")
    pdf = PDFReportService().build_report(analysis)
    return Response(content=pdf, media_type="application/pdf", headers={"Content-Disposition": f"attachment; filename=analysis-{analysis_id}.pdf"})
