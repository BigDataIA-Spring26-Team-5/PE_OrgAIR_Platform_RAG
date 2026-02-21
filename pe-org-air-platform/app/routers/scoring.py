"""
CS3 Dimensions Scoring API Router
app/routers/scoring.py

Endpoints:
  POST /api/v1/scoring/{ticker}           — Score one company (full pipeline → Snowflake)
  POST /api/v1/scoring/all                — Score all companies with CS2 data
  GET  /api/v1/scoring/{ticker}/matrix    — View mapping matrix (Table 1) from Snowflake
  GET  /api/v1/scoring/{ticker}/dimensions — View 7 dimension scores from Snowflake
  GET  /api/v1/scoring/{ticker}/full      — View matrix + dimensions + coverage
  GET  /api/v1/scoring/summary            — View all companies' dimension scores
  DELETE /api/v1/scoring/{ticker}         — Delete scoring data for a company

Register in main.py:
    from app.routers.scoring import router as scoring_router
    app.include_router(scoring_router)
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import logging
import time

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["CS3 Dimensions Scoring"])


# =====================================================================
# Response Models
# =====================================================================

class ScoringResponse(BaseModel):
    """Response from scoring a single company."""
    ticker: str
    company_id: Optional[str] = None
    status: str  # "success" or "failed"
    scored_at: Optional[str] = None
    dimension_scores: Optional[List[Dict[str, Any]]] = None
    mapping_matrix: Optional[List[Dict[str, Any]]] = None
    coverage: Optional[Dict[str, Any]] = None
    evidence_sources: Optional[Dict[str, Any]] = None
    persisted: bool = False
    duration_seconds: Optional[float] = None
    error: Optional[str] = None


class AllScoringResponse(BaseModel):
    """Response from scoring all companies."""
    status: str
    companies_scored: int
    companies_failed: int
    results: List[ScoringResponse]
    duration_seconds: float


class MappingMatrixResponse(BaseModel):
    """Response for viewing the mapping matrix."""
    ticker: str
    rows: List[Dict[str, Any]]
    row_count: int


class DimensionScoresResponse(BaseModel):
    """Response for viewing dimension scores."""
    ticker: str
    scores: List[Dict[str, Any]]
    score_count: int


class FullScoringView(BaseModel):
    """Full view of all scoring data for a company."""
    ticker: str
    company_id: Optional[str] = None
    mapping_matrix: List[Dict[str, Any]]
    dimension_scores: List[Dict[str, Any]]
    coverage: Optional[Dict[str, Any]] = None
    evidence_sources: Optional[Dict[str, Any]] = None
    last_scored: Optional[str] = None


class SummaryResponse(BaseModel):
    """Summary of all companies' dimension scores."""
    companies: List[Dict[str, Any]]
    total_companies: int


# =====================================================================
# POST /api/v1/scoring/all — Score all companies
# NOTE: This MUST be defined BEFORE /scoring/{ticker} so FastAPI
#       matches the static "/all" path before the dynamic "{ticker}".
# =====================================================================

@router.post(
    "/scoring/all",
    response_model=AllScoringResponse,
    summary="Score all companies with CS2 data",
    description="""
    Runs the CS3 Dimensions Scoring pipeline for every company that has a signal summary
    in `company_signal_summaries`. Returns individual results for each.
    """,
    tags=["CS3 Dimensions Scoring"],
)
async def score_all_companies():
    """Score all companies."""
    start = time.time()

    try:
        from app.services.scoring_service import get_scoring_service
        service = get_scoring_service()
        results = service.score_all_companies()

        responses = []
        scored = 0
        failed = 0
        for r in results:
            if r.get("error"):
                failed += 1
                responses.append(ScoringResponse(
                    ticker=r["ticker"],
                    status="failed",
                    error=r["error"],
                ))
            else:
                scored += 1
                responses.append(ScoringResponse(
                    ticker=r["ticker"],
                    company_id=r.get("company_id"),
                    status="success",
                    scored_at=r.get("scored_at"),
                    dimension_scores=r.get("dimension_scores"),
                    persisted=r.get("persisted", False),
                ))

        return AllScoringResponse(
            status="completed",
            companies_scored=scored,
            companies_failed=failed,
            results=responses,
            duration_seconds=round(time.time() - start, 2),
        )
    except Exception as e:
        logger.error(f"Scoring all failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# POST /api/v1/scoring/{ticker} — Score one company
# =====================================================================

@router.post(
    "/scoring/{ticker}",
    response_model=ScoringResponse,
    summary="Score a company (CS3 full pipeline)",
    description="""
    Runs the full CS3 Dimensions Scoring pipeline for a single company:

    1. **Reads CS2 signals** from `company_signal_summaries` (hiring, innovation, digital, leadership)
    2. **Reads SEC sections** from `document_chunks` + S3 (Item 1, 1A, 7)
    3. **Rubric scores** SEC text against 7-dimension rubrics (Task 5.0b)
    4. **Maps evidence** to 7 dimensions using weighted matrix (Task 5.0a)
    5. **Persists** mapping matrix + dimension scores to Snowflake

    **Prerequisite:** Company must have CS2 signal data (run signal scoring first).
    """,
    tags=["CS3 Dimensions Scoring"],
)
async def score_company(ticker: str):
    """Score one company — full CS3 pipeline."""
    start = time.time()
    ticker = ticker.upper()

    try:
        from app.services.scoring_service import get_scoring_service
        service = get_scoring_service()
        result = service.score_company(ticker)

        return ScoringResponse(
            ticker=ticker,
            company_id=result.get("company_id"),
            status="success",
            scored_at=result.get("scored_at"),
            dimension_scores=result.get("dimension_scores"),
            mapping_matrix=result.get("mapping_matrix"),
            coverage=result.get("coverage"),
            evidence_sources=result.get("evidence_sources"),
            persisted=result.get("persisted", False),
            duration_seconds=round(time.time() - start, 2),
        )
    except Exception as e:
        logger.error(f"Scoring failed for {ticker}: {e}", exc_info=True)
        return ScoringResponse(
            ticker=ticker,
            status="failed",
            error=str(e),
            duration_seconds=round(time.time() - start, 2),
        )


# =====================================================================
# GET /api/v1/scoring/{ticker}/matrix — View mapping matrix
# =====================================================================

@router.get(
    "/scoring/{ticker}/matrix",
    response_model=MappingMatrixResponse,
    summary="View signal-to-dimension mapping matrix (Table 1)",
    description="""
    Returns the CS3 Table 1 mapping matrix for a company from Snowflake.

    Each row is one evidence source (e.g. `technology_hiring`, `sec_item_1a`)
    with its raw score and weight contributions to each of the 7 dimensions.

    **Equivalent Snowflake query:**
    ```sql
    SELECT * FROM signal_dimension_mapping WHERE ticker = '{ticker}'
    ```
    """,
    tags=["CS3 Dimensions Scoring"],
)
async def get_mapping_matrix(ticker: str):
    """View the mapping matrix from Snowflake."""
    ticker = ticker.upper()

    try:
        from app.repositories.scoring_repository import get_scoring_repository
        repo = get_scoring_repository()
        rows = repo.get_mapping_matrix(ticker)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No mapping matrix found for {ticker}. Run POST /api/v1/scoring/{ticker} first."
            )

        # Convert Decimal types for JSON serialization
        clean_rows = [_serialize_row(r) for r in rows]

        return MappingMatrixResponse(
            ticker=ticker,
            rows=clean_rows,
            row_count=len(clean_rows),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get matrix for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# GET /api/v1/scoring/{ticker}/dimensions — View dimension scores
# =====================================================================

@router.get(
    "/scoring/{ticker}/dimensions",
    response_model=DimensionScoresResponse,
    summary="View 7 dimension scores for a company",
    description="""
    Returns the 7 aggregated dimension scores for a company from Snowflake.

    **Equivalent Snowflake query:**
    ```sql
    SELECT * FROM evidence_dimension_scores WHERE ticker = '{ticker}'
    ```
    """,
    tags=["CS3 Dimensions Scoring"],
)
async def get_dimension_scores(ticker: str):
    """View dimension scores from Snowflake."""
    ticker = ticker.upper()

    try:
        from app.repositories.scoring_repository import get_scoring_repository
        repo = get_scoring_repository()
        rows = repo.get_dimension_scores(ticker)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No dimension scores found for {ticker}. Run POST /api/v1/scoring/{ticker} first."
            )

        clean_rows = [_serialize_row(r) for r in rows]

        return DimensionScoresResponse(
            ticker=ticker,
            scores=clean_rows,
            score_count=len(clean_rows),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get dimensions for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# GET /api/v1/scoring/{ticker}/full — Full view (matrix + dimensions)
# =====================================================================

@router.get(
    "/scoring/{ticker}/full",
    response_model=FullScoringView,
    summary="Full scoring view (matrix + dimensions + coverage)",
    description="""
    Returns everything: the mapping matrix, dimension scores, coverage report,
    and evidence source info — all from Snowflake.
    """,
    tags=["CS3 Dimensions Scoring"],
)
async def get_full_scoring_view(ticker: str):
    """Full scoring view from Snowflake."""
    ticker = ticker.upper()

    try:
        from app.repositories.scoring_repository import get_scoring_repository
        from app.repositories.company_repository import CompanyRepository

        scoring_repo = get_scoring_repository()
        company_repo = CompanyRepository()

        company = company_repo.get_by_ticker(ticker)
        company_id = str(company["id"]) if company else None

        matrix = scoring_repo.get_mapping_matrix(ticker)
        dimensions = scoring_repo.get_dimension_scores(ticker)

        if not matrix and not dimensions:
            raise HTTPException(
                status_code=404,
                detail=f"No scoring data found for {ticker}. Run POST /api/v1/scoring/{ticker} first."
            )

        # Build coverage from dimension scores
        coverage = {}
        for row in dimensions:
            dim = row.get("dimension", "")
            coverage[dim] = {
                "has_evidence": row.get("source_count", 0) > 0,
                "source_count": row.get("source_count", 0),
                "sources": row.get("sources", ""),
            }

        # Build evidence_sources from matrix
        ev_sources = {
            "cs2_signals": sum(
                1 for r in matrix
                if r.get("source") in ("technology_hiring", "innovation_activity",
                                       "digital_presence", "leadership_signals")
                and r.get("raw_score") is not None
            ),
            "sec_sections": sum(
                1 for r in matrix
                if r.get("source", "").startswith("sec_")
                and r.get("raw_score") is not None
            ),
            "total": sum(1 for r in matrix if r.get("raw_score") is not None),
        }

        # Get last scored timestamp
        last_scored = None
        if matrix:
            created = matrix[0].get("created_at")
            if created:
                last_scored = str(created)

        return FullScoringView(
            ticker=ticker,
            company_id=company_id,
            mapping_matrix=[_serialize_row(r) for r in matrix],
            dimension_scores=[_serialize_row(r) for r in dimensions],
            coverage=coverage,
            evidence_sources=ev_sources,
            last_scored=last_scored,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get full view for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# GET /api/v1/scoring/summary — All companies' dimension scores
# =====================================================================

@router.get(
    "/scoring/summary",
    response_model=SummaryResponse,
    summary="View dimension scores for all companies",
    description="""
    Returns a summary of dimension scores across all scored companies.
    Useful for comparing AI readiness across the portfolio.
    """,
    tags=["CS3 Dimensions Scoring"],
)
async def get_scoring_summary():
    """View all companies' dimension scores."""
    try:
        from app.repositories.scoring_repository import get_scoring_repository
        repo = get_scoring_repository()
        all_scores = repo.get_all_dimension_scores()

        # Group by ticker
        companies = {}
        for row in all_scores:
            ticker = row.get("ticker", "")
            if ticker not in companies:
                companies[ticker] = {"ticker": ticker, "dimensions": {}}
            dim = row.get("dimension", "")
            companies[ticker]["dimensions"][dim] = {
                "score": _safe_float(row.get("score")),
                "confidence": _safe_float(row.get("confidence")),
                "source_count": row.get("source_count", 0),
            }

        company_list = list(companies.values())

        return SummaryResponse(
            companies=company_list,
            total_companies=len(company_list),
        )
    except Exception as e:
        logger.error(f"Failed to get summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# DELETE /api/v1/scoring/{ticker} — Delete scoring data
# =====================================================================

@router.delete(
    "/scoring/{ticker}",
    summary="Delete scoring data for a company",
    description="Removes mapping matrix and dimension scores from Snowflake.",
    tags=["CS3 Dimensions Scoring"],
)
async def delete_scoring_data(ticker: str):
    """Delete scoring data for a company."""
    ticker = ticker.upper()

    try:
        from app.repositories.scoring_repository import get_scoring_repository
        repo = get_scoring_repository()

        mapping_deleted = repo.delete_mapping_matrix(ticker)
        scores_deleted = repo.delete_dimension_scores(ticker)

        return {
            "ticker": ticker,
            "mapping_rows_deleted": mapping_deleted,
            "dimension_scores_deleted": scores_deleted,
            "message": f"Scoring data deleted for {ticker}",
        }
    except Exception as e:
        logger.error(f"Failed to delete scoring for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# Helpers
# =====================================================================

def _serialize_row(row: Dict) -> Dict:
    """Convert Decimal/datetime types to JSON-safe types."""
    from decimal import Decimal
    clean = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            clean[k] = float(v)
        elif isinstance(v, datetime):
            clean[k] = v.isoformat()
        else:
            clean[k] = v
    return clean


def _safe_float(val) -> Optional[float]:
    """Safely convert to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# =====================================================================
# GET /api/v1/scoring/report/all — Download portfolio summary report
# NOTE: Must be BEFORE /report/{ticker}
# =====================================================================

@router.get(
    "/scoring/report/all",
    summary="Download portfolio summary report (.md)",
    description="Returns a downloadable markdown comparison report across all scored companies.",
    tags=["CS3 Reports"],
)
async def generate_portfolio_report():
    """Download portfolio summary as .md file."""
    from fastapi.responses import StreamingResponse
    import io

    try:
        from app.repositories.scoring_repository import get_scoring_repository
        from app.repositories.signal_repository import get_signal_repository
        from app.services.report_generator import generate_portfolio_summary

        repo = get_scoring_repository()
        signal_repo = get_signal_repository()

        all_scores = repo.get_all_dimension_scores()
        all_summaries = signal_repo.get_all_summaries()

        if not all_scores:
            raise HTTPException(status_code=404, detail="No scoring data. Run POST /api/v1/scoring/all first.")

        md = generate_portfolio_summary(all_scores, all_summaries)

        return StreamingResponse(
            io.BytesIO(md.encode("utf-8")),
            media_type="text/markdown",
            headers={"Content-Disposition": "attachment; filename=cs3_portfolio_summary.md"},
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Report generation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# GET /api/v1/scoring/report/{ticker} — Download single company report
# =====================================================================

@router.get(
    "/scoring/report/{ticker}",
    summary="Download scoring report for a company (.md)",
    description="Returns a downloadable markdown scoring report for a single company.",
    tags=["CS3 Reports"],
)
async def generate_company_report_endpoint(ticker: str):
    """Download company scoring report as .md file."""
    from fastapi.responses import StreamingResponse
    import io

    ticker = ticker.upper()

    try:
        from app.repositories.scoring_repository import get_scoring_repository
        from app.repositories.signal_repository import get_signal_repository
        from app.services.report_generator import generate_company_report

        repo = get_scoring_repository()
        signal_repo = get_signal_repository()

        matrix = repo.get_mapping_matrix(ticker)
        dimensions = repo.get_dimension_scores(ticker)

        if not matrix and not dimensions:
            raise HTTPException(
                status_code=404,
                detail=f"No scoring data for {ticker}. Run POST /api/v1/scoring/{ticker} first."
            )

        signal_summary = signal_repo.get_summary_by_ticker(ticker)

        clean_matrix = [_serialize_row(r) for r in matrix]
        clean_dims = [_serialize_row(r) for r in dimensions]

        md = generate_company_report(ticker, clean_matrix, clean_dims, signal_summary)

        return StreamingResponse(
            io.BytesIO(md.encode("utf-8")),
            media_type="text/markdown",
            headers={"Content-Disposition": f"attachment; filename={ticker}_cs3_report.md"},
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Report failed for {ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))