"""
routers/orgair_scoring.py — CS3 Task 6.4 Endpoints

Endpoints:
  POST /api/v1/scoring/orgair/{ticker}        — Compute Org-AI-R for one company
  POST /api/v1/scoring/orgair/portfolio       — Compute Org-AI-R for all 5 CS3 companies
  POST /api/v1/scoring/orgair/results         — Generate results/*.json for submission
  GET  /api/v1/scoring/orgair/portfolio       — Read portfolio from Snowflake
  GET  /api/v1/scoring/orgair/{ticker}        — Read one from Snowflake
"""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
import logging
import time

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/scoring", tags=["CS3 Org-AI-R"])

# The 5 CS3 portfolio companies
CS3_PORTFOLIO = ["NVDA", "JPM", "WMT", "GE", "DG"]

# Sector assignments (matches hr_scoring.py)
COMPANY_SECTORS = {
    "NVDA": "technology",
    "JPM": "financial_services",
    "WMT": "retail",
    "GE": "manufacturing",
    "DG": "retail",
}

# Market cap percentiles (manual input)
MARKET_CAP_PERCENTILES = {
    "NVDA": 0.95, "JPM": 0.85, "WMT": 0.60, "GE": 0.50, "DG": 0.30,
}

COMPANY_NAMES = {
    "NVDA": "NVIDIA Corporation",
    "JPM": "JPMorgan Chase & Co.",
    "WMT": "Walmart Inc.",
    "GE": "GE Aerospace",
    "DG": "Dollar General Corporation",
}

# Expected OrgAIR ranges from case study Table 5
EXPECTED_ORGAIR_RANGES = {
    "NVDA": (85.0, 95.0),
    "JPM":  (65.0, 75.0),
    "WMT":  (55.0, 65.0),
    "GE":   (45.0, 55.0),
    "DG":   (35.0, 45.0),
}

EXPECTED_TC_RANGES = {
    "NVDA": (0.05, 0.20),
    "JPM":  (0.10, 0.25),
    "WMT":  (0.12, 0.28),
    "GE":   (0.18, 0.35),
    "DG":   (0.22, 0.40),
}

EXPECTED_PF_RANGES = {
    "NVDA": (0.7, 1.0),
    "JPM":  (0.3, 0.7),
    "WMT":  (0.1, 0.5),
    "GE":   (-0.2, 0.2),
    "DG":   (-0.5, -0.1),
}

# Sector-specific timing factors (CS3 §6.3: TimingFactor ∈ [0.8, 1.2])
SECTOR_TIMING = {
    "technology": 1.20,
    "financial_services": 1.05,
    "retail": 1.00,
    "manufacturing": 1.00,
}


# =====================================================================
# Response Models
# =====================================================================

class CIBreakdown(BaseModel):
    ci_lower: float
    ci_upper: float
    sem: float
    reliability: float
    score_type: str


class OrgAIRBreakdown(BaseModel):
    org_air_score: float
    vr_score: float
    hr_score: float
    synergy_score: float
    weighted_base: float
    synergy_contribution: float
    vr_weighted: float
    hr_weighted: float
    alpha: float
    beta: float
    vr_ci: Optional[CIBreakdown] = None
    hr_ci: Optional[CIBreakdown] = None
    orgair_ci: Optional[CIBreakdown] = None


class OrgAIRValidation(BaseModel):
    orgair_in_range: bool
    orgair_expected: str
    status: str


class OrgAIRResponse(BaseModel):
    ticker: str
    status: str

    org_air_score: Optional[float] = None
    breakdown: Optional[OrgAIRBreakdown] = None
    validation: Optional[OrgAIRValidation] = None

    duration_seconds: Optional[float] = None
    error: Optional[str] = None
    scored_at: Optional[str] = None


class PortfolioOrgAIRResponse(BaseModel):
    status: str
    companies_scored: int
    companies_failed: int
    results: List[OrgAIRResponse]
    summary_table: List[Dict[str, Any]]
    duration_seconds: float


class ResultsGenerationResponse(BaseModel):
    status: str
    files_generated: int
    local_files: List[str]
    s3_files: List[str]
    summary: List[Dict[str, Any]]
    duration_seconds: float


# =====================================================================
# Helper: Compute Org-AI-R (Fix #7 — no double execution)
# =====================================================================

def _compute_orgair(ticker: str) -> OrgAIRResponse:
    start = time.time()
    ticker = ticker.upper()

    try:
        logger.info("=" * 60)
        logger.info(f"Org-AI-R CALCULATION: {ticker}")
        logger.info("=" * 60)

        # ---- 1. Get V^R ----
        from app.routers.tc_vr_scoring import _compute_tc_vr
        vr_response = _compute_tc_vr(ticker)

        if vr_response.status != "success":
            raise ValueError(f"V^R calculation failed: {vr_response.error}")

        vr_score = vr_response.vr_result.vr_score if vr_response.vr_result else None
        if vr_score is None:
            raise ValueError("V^R score missing from TC+VR response")

        logger.info(f"[{ticker}] V^R = {vr_score:.2f}")

        # ---- 2. Get H^R (direct — avoids second pipeline run) ----
        from app.scoring.hr_calculator import HRCalculator
        from app.scoring.position_factor import PositionFactorCalculator

        sector = COMPANY_SECTORS.get(ticker, "")
        mcap = MARKET_CAP_PERCENTILES.get(ticker, 0.50)

        pf = PositionFactorCalculator().calculate_position_factor(vr_score, sector, mcap)
        hr_result = HRCalculator().calculate(sector, float(pf))
        hr_score = float(hr_result.hr_score)

        logger.info(f"[{ticker}] PF = {float(pf):.4f}")
        logger.info(f"[{ticker}] H^R = {hr_score:.2f}")

        # ---- 3. Synergy ----
        from app.scoring.synergy_calculator import SynergyCalculator

        timing = SECTOR_TIMING.get(sector, 1.0)
        synergy_result = SynergyCalculator().calculate(vr_score, hr_score, timing_factor=timing)
        synergy_score = float(synergy_result.synergy_score)
        logger.info(f"[{ticker}] Synergy = {synergy_score:.2f}")

        # ---- 4. Org-AI-R ----
        from app.scoring.orgair_calculator import OrgAIRCalculator
        orgair_result = OrgAIRCalculator().calculate(vr_score, hr_score, synergy_score)
        org_air_score = float(orgair_result.org_air_score)
        logger.info(f"[{ticker}] Org-AI-R = {org_air_score:.2f}")

        # ---- 5. Confidence Intervals ----
        from app.scoring.confidence_calculator import ConfidenceCalculator
        ci_calc = ConfidenceCalculator()

        vr_ci_result = ci_calc.calculate(vr_score, 7, "vr")
        hr_ci_result = ci_calc.calculate(hr_score, 7, "hr")
        orgair_ci_result = ci_calc.calculate(org_air_score, 7, "org_air")

        vr_ci = CIBreakdown(
            ci_lower=float(vr_ci_result.ci_lower), ci_upper=float(vr_ci_result.ci_upper),
            sem=float(vr_ci_result.sem), reliability=float(vr_ci_result.reliability), score_type="vr",
        )
        hr_ci = CIBreakdown(
            ci_lower=float(hr_ci_result.ci_lower), ci_upper=float(hr_ci_result.ci_upper),
            sem=float(hr_ci_result.sem), reliability=float(hr_ci_result.reliability), score_type="hr",
        )
        orgair_ci = CIBreakdown(
            ci_lower=float(orgair_ci_result.ci_lower), ci_upper=float(orgair_ci_result.ci_upper),
            sem=float(orgair_ci_result.sem), reliability=float(orgair_ci_result.reliability), score_type="org_air",
        )

        logger.info(
            f"[{ticker}] Org-AI-R CI = [{float(orgair_ci_result.ci_lower):.2f}, "
            f"{float(orgair_ci_result.ci_upper):.2f}]  reliability={float(orgair_ci_result.reliability):.4f}"
        )

        # ---- 6. Validate ----
        validation = None
        if ticker in EXPECTED_ORGAIR_RANGES:
            exp_lo, exp_hi = EXPECTED_ORGAIR_RANGES[ticker]
            in_range = exp_lo <= org_air_score <= exp_hi
            status = "✅" if in_range else "⚠️"
            validation = OrgAIRValidation(
                orgair_in_range=in_range,
                orgair_expected=f"{exp_lo:.1f} to {exp_hi:.1f}",
                status=status,
            )
            logger.info(f"[{ticker}] Validation: Org-AI-R={org_air_score:.2f} expected [{exp_lo:.1f}, {exp_hi:.1f}]  {status}")

        breakdown = OrgAIRBreakdown(
            org_air_score=org_air_score, vr_score=vr_score, hr_score=hr_score,
            synergy_score=synergy_score, weighted_base=float(orgair_result.weighted_base),
            synergy_contribution=float(orgair_result.synergy_contribution),
            vr_weighted=float(orgair_result.vr_weighted), hr_weighted=float(orgair_result.hr_weighted),
            alpha=float(orgair_result.alpha), beta=float(orgair_result.beta),
            vr_ci=vr_ci, hr_ci=hr_ci, orgair_ci=orgair_ci,
        )

        logger.info("=" * 60)

        return OrgAIRResponse(
            ticker=ticker, status="success", org_air_score=org_air_score,
            breakdown=breakdown, validation=validation,
            duration_seconds=round(time.time() - start, 2),
            scored_at=datetime.now(timezone.utc).isoformat(),
        )

    except Exception as e:
        logger.error(f"Org-AI-R calculation failed for {ticker}: {e}", exc_info=True)
        return OrgAIRResponse(
            ticker=ticker, status="failed", error=str(e),
            duration_seconds=round(time.time() - start, 2),
        )


# =====================================================================
# POST /api/v1/scoring/orgair/results — Generate results/*.json
# =====================================================================

@router.post(
    "/orgair/results",
    response_model=ResultsGenerationResponse,
    summary="Generate results/*.json files for CS3 submission",
    description="""
    Runs the full Org-AI-R pipeline for all 5 companies, then generates
    individual JSON result files (nvda.json, jpm.json, etc.) saved both
    locally in results/ and to S3 under scoring/results/.

    Each JSON contains: final Org-AI-R score, V^R, H^R, synergy,
    7 dimension scores, TC, PF, confidence intervals, job analysis,
    and validation against CS3 Table 5 expected ranges.
    """,
)
async def generate_results():
    """Generate results JSON files for CS3 submission."""
    start = time.time()

    logger.info("=" * 70)
    logger.info("GENERATING CS3 RESULTS JSON FILES")
    logger.info("=" * 70)

    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)

    local_files = []
    s3_files = []
    summary = []
    files_generated = 0

    for ticker in CS3_PORTFOLIO:
        logger.info(f"\n{'─'*50}")
        logger.info(f"Scoring {ticker}...")

        # 1. Run Org-AI-R
        response = _compute_orgair(ticker)
        if response.status != "success":
            logger.error(f"[{ticker}] FAILED: {response.error}")
            continue

        b = response.breakdown

        # 2. Get TC+VR details for dimension scores
        from app.routers.tc_vr_scoring import _compute_tc_vr
        tc_vr = _compute_tc_vr(ticker)

        tc_val = tc_vr.talent_concentration if tc_vr else None
        tc_breakdown = tc_vr.tc_breakdown if tc_vr else None
        dim_scores = tc_vr.dimension_scores if tc_vr else None
        job_analysis = tc_vr.job_analysis if tc_vr else None

        # 3. Get PF
        from app.scoring.position_factor import PositionFactorCalculator
        sector = COMPANY_SECTORS.get(ticker, "")
        mcap = MARKET_CAP_PERCENTILES.get(ticker, 0.50)
        pf = float(PositionFactorCalculator().calculate_position_factor(b.vr_score, sector, mcap))

        # 4. Validation ranges
        org_air_range = EXPECTED_ORGAIR_RANGES.get(ticker, (0, 100))
        tc_range = EXPECTED_TC_RANGES.get(ticker, (0, 1))
        pf_range = EXPECTED_PF_RANGES.get(ticker, (-1, 1))

        # 5. Build result JSON
        result_data = {
            "ticker": ticker,
            "company_name": COMPANY_NAMES.get(ticker, ticker),
            "sector": sector,
            "scored_at": datetime.now(timezone.utc).isoformat(),

            "org_air_score": b.org_air_score,
            "org_air_ci": {
                "lower": b.orgair_ci.ci_lower if b.orgair_ci else None,
                "upper": b.orgair_ci.ci_upper if b.orgair_ci else None,
                "sem": b.orgair_ci.sem if b.orgair_ci else None,
                "reliability": b.orgair_ci.reliability if b.orgair_ci else None,
            },

            "vr_score": b.vr_score,
            "vr_ci": {
                "lower": b.vr_ci.ci_lower if b.vr_ci else None,
                "upper": b.vr_ci.ci_upper if b.vr_ci else None,
            },

            "hr_score": b.hr_score,
            "hr_ci": {
                "lower": b.hr_ci.ci_lower if b.hr_ci else None,
                "upper": b.hr_ci.ci_upper if b.hr_ci else None,
            },

            "synergy_score": b.synergy_score,

            "formula": {
                "alpha": b.alpha,
                "beta": b.beta,
                "vr_weighted": b.vr_weighted,
                "hr_weighted": b.hr_weighted,
                "weighted_base": b.weighted_base,
                "synergy_contribution": b.synergy_contribution,
            },

            "position_factor": pf,
            "market_cap_percentile": mcap,

            "talent_concentration": float(tc_val) if tc_val else None,
            "tc_breakdown": tc_breakdown if tc_breakdown else None,

            "dimension_scores": dim_scores if dim_scores else None,

            "job_analysis": job_analysis if job_analysis else None,

            "validation": {
                "org_air_in_range": org_air_range[0] <= b.org_air_score <= org_air_range[1],
                "org_air_expected": f"{org_air_range[0]:.1f} - {org_air_range[1]:.1f}",
                "tc_in_range": tc_range[0] <= float(tc_val) <= tc_range[1] if tc_val else None,
                "tc_expected": f"{tc_range[0]} - {tc_range[1]}",
                "pf_in_range": pf_range[0] <= pf <= pf_range[1],
                "pf_expected": f"{pf_range[0]} - {pf_range[1]}",
            },
        }

        # 6. Save locally
        local_path = results_dir / f"{ticker.lower()}.json"
        local_path.write_text(json.dumps(result_data, indent=2, default=str), encoding="utf-8")
        local_files.append(str(local_path))
        logger.info(f"[{ticker}] ✅ Local: {local_path}")

        # 7. Save to S3
        try:
            from app.services.s3_storage import get_s3_service
            s3 = get_s3_service()
            s3_key = f"scoring/results/{ticker.lower()}.json"
            s3.upload_json(result_data, s3_key)
            s3_files.append(s3_key)
            logger.info(f"[{ticker}] ✅ S3: {s3_key}")
        except Exception as e:
            logger.warning(f"[{ticker}] S3 upload failed (non-fatal): {e}")

        files_generated += 1

        summary.append({
            "ticker": ticker,
            "org_air_score": b.org_air_score,
            "vr_score": b.vr_score,
            "hr_score": b.hr_score,
            "synergy_score": b.synergy_score,
            "tc": float(tc_val) if tc_val else None,
            "pf": pf,
            "in_range": result_data["validation"]["org_air_in_range"],
        })

    # 8. Save portfolio summary
    portfolio_summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline": "CS3 Org-AI-R Scoring Engine",
        "companies": files_generated,
        "all_in_range": all(s["in_range"] for s in summary),
        "results": summary,
    }

    summary_local = results_dir / "portfolio_summary.json"
    summary_local.write_text(json.dumps(portfolio_summary, indent=2, default=str), encoding="utf-8")
    local_files.append(str(summary_local))

    try:
        from app.services.s3_storage import get_s3_service
        s3 = get_s3_service()
        s3.upload_json(portfolio_summary, "scoring/results/portfolio_summary.json")
        s3_files.append("scoring/results/portfolio_summary.json")
    except Exception:
        pass

    # 9. Print final table
    logger.info(f"\n{'='*70}")
    logger.info(f"CS3 RESULTS — FINAL SCORES")
    logger.info(f"{'='*70}")
    logger.info(f"{'Ticker':<6} {'Org-AI-R':>9} {'V^R':>7} {'H^R':>7} {'Syn':>7} {'TC':>7} {'PF':>7} {'Range':>12} {'✓':>3}")
    logger.info(f"{'-'*6} {'-'*9} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*12} {'-'*3}")
    for s in summary:
        exp = EXPECTED_ORGAIR_RANGES.get(s["ticker"], (0, 100))
        status = "✅" if s["in_range"] else "⚠️"
        logger.info(
            f"{s['ticker']:<6} {s['org_air_score']:>9.2f} {s['vr_score']:>7.2f} "
            f"{s['hr_score']:>7.2f} {s['synergy_score']:>7.2f} "
            f"{s['tc']:>7.4f} {s['pf']:>7.4f} "
            f"{exp[0]:.0f}-{exp[1]:.0f}:>12 {status:>3}"
        )
    logger.info(f"{'='*70}")

    passed = sum(1 for s in summary if s["in_range"])
    logger.info(f"Validation: {passed}/{len(summary)} within expected range")
    logger.info(f"Files: {len(local_files)} local, {len(s3_files)} S3")
    logger.info(f"Duration: {time.time() - start:.2f}s")

    return ResultsGenerationResponse(
        status="success",
        files_generated=files_generated,
        local_files=local_files,
        s3_files=s3_files,
        summary=summary,
        duration_seconds=round(time.time() - start, 2),
    )


# =====================================================================
# POST /api/v1/scoring/orgair/portfolio
# =====================================================================

@router.post(
    "/orgair/portfolio",
    response_model=PortfolioOrgAIRResponse,
    summary="Calculate Org-AI-R for all 5 CS3 portfolio companies",
)
async def score_portfolio_orgair():
    """Calculate Org-AI-R for all 5 companies."""
    start = time.time()

    logger.info("=" * 70)
    logger.info("Org-AI-R PORTFOLIO SCORING — 5 COMPANIES")
    logger.info("=" * 70)

    results = []
    scored = 0
    failed = 0

    for ticker in CS3_PORTFOLIO:
        result = _compute_orgair(ticker)
        results.append(result)
        if result.status == "success":
            scored += 1
            _save_orgair_result(result)
        else:
            failed += 1

    summary = []
    logger.info("")
    logger.info("=" * 70)
    logger.info("Org-AI-R SUMMARY TABLE")
    logger.info("=" * 70)
    logger.info(f"{'Ticker':<8} {'V^R':>8} {'H^R':>8} {'Synergy':>9} {'Org-AI-R':>10} {'Range':>15} {'✓':>3}")
    logger.info("-" * 70)

    for r in results:
        if r.status == "success" and r.breakdown:
            b = r.breakdown
            val_status = r.validation.status if r.validation else "—"
            range_str = r.validation.orgair_expected if r.validation else "—"
            logger.info(
                f"{r.ticker:<8} {b.vr_score:>8.2f} {b.hr_score:>8.2f} "
                f"{b.synergy_score:>9.2f} {b.org_air_score:>10.2f} "
                f"{range_str:>15} {val_status:>3}"
            )
            summary.append({
                "ticker": r.ticker, "vr_score": b.vr_score, "hr_score": b.hr_score,
                "synergy_score": b.synergy_score, "org_air_score": b.org_air_score,
                "weighted_base": b.weighted_base, "synergy_contribution": b.synergy_contribution,
                "orgair_in_expected_range": r.validation.orgair_in_range if r.validation else None,
            })
        else:
            logger.info(f"{r.ticker:<8} FAILED: {r.error}")
            summary.append({"ticker": r.ticker, "status": "failed", "error": r.error})

    logger.info("-" * 70)
    orgair_pass = sum(1 for r in results if r.validation and r.validation.orgair_in_range)
    orgair_total = sum(1 for r in results if r.validation)
    logger.info(f"Scored: {scored}  Failed: {failed}")
    logger.info(f"Org-AI-R Validation: {orgair_pass}/{orgair_total} within expected range")
    logger.info(f"Duration: {time.time() - start:.2f}s")
    logger.info("=" * 70)

    return PortfolioOrgAIRResponse(
        status="success" if failed == 0 else "partial",
        companies_scored=scored, companies_failed=failed,
        results=results, summary_table=summary,
        duration_seconds=round(time.time() - start, 2),
    )


# =====================================================================
# POST /api/v1/scoring/orgair/{ticker}
# =====================================================================

@router.post(
    "/orgair/{ticker}",
    response_model=OrgAIRResponse,
    summary="Calculate Org-AI-R for one company",
)
async def score_orgair(ticker: str):
    """Calculate Org-AI-R for one company."""
    result = _compute_orgair(ticker.upper())
    if result.status == "success":
        _save_orgair_result(result)
    return result


# =====================================================================
# Persistence helpers
# =====================================================================

def _save_orgair_result(result: OrgAIRResponse) -> None:
    ticker = result.ticker
    try:
        from app.services.s3_storage import get_s3_service
        s3 = get_s3_service()
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"scoring/orgair/{ticker}/{ts}.json"
        s3.upload_json(result.model_dump(), s3_key)
        logger.info(f"  ✅ Uploaded to S3: {s3_key}")
    except Exception as e:
        logger.warning(f"[{ticker}] S3 save failed (non-fatal): {e}")

    try:
        _upsert_scoring_orgair(ticker, result)
        logger.info(f"[{ticker}] SCORING table upserted: org_air={result.org_air_score}")
    except Exception as e:
        logger.warning(f"[{ticker}] Snowflake SCORING upsert failed (non-fatal): {e}")


def _upsert_scoring_orgair(ticker: str, result: OrgAIRResponse) -> None:
    from app.services.snowflake import get_snowflake_connection

    if not result.breakdown:
        return

    b = result.breakdown
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        sql = """
            MERGE INTO SCORING AS tgt
            USING (SELECT %s AS ticker) AS src
            ON tgt.ticker = src.ticker
            WHEN MATCHED THEN UPDATE SET
                org_air = %s, vr_score = %s, hr_score = %s,
                synergy_score = %s, ci_lower = %s, ci_upper = %s,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (ticker, org_air, vr_score, hr_score, synergy_score,
                 ci_lower, ci_upper, scored_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """
        ci_lower = b.orgair_ci.ci_lower if b.orgair_ci else None
        ci_upper = b.orgair_ci.ci_upper if b.orgair_ci else None
        cursor.execute(sql, [
            ticker, b.org_air_score, b.vr_score, b.hr_score,
            b.synergy_score, ci_lower, ci_upper,
            ticker, b.org_air_score, b.vr_score, b.hr_score,
            b.synergy_score, ci_lower, ci_upper,
        ])
        conn.commit()
        cursor.close()
    finally:
        conn.close()


# =====================================================================
# GET endpoints (Snowflake reads)
# =====================================================================

class OrgAIRScoringRecord(BaseModel):
    ticker: str
    org_air: Optional[float] = None
    scored_at: Optional[str] = None
    updated_at: Optional[str] = None


class PortfolioOrgAIRScoringResponse(BaseModel):
    status: str
    results: List[OrgAIRScoringRecord]
    message: Optional[str] = None


def _fetch_orgair_row(ticker: str) -> Optional[OrgAIRScoringRecord]:
    from app.services.snowflake import get_snowflake_connection
    from snowflake.connector import DictCursor
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            "SELECT ticker, org_air, scored_at, updated_at FROM SCORING WHERE ticker = %s",
            [ticker.upper()],
        )
        row = cursor.fetchone()
        cursor.close()
        if not row:
            return None
        return OrgAIRScoringRecord(
            ticker=row["TICKER"], org_air=row.get("ORG_AIR"),
            scored_at=str(row["SCORED_AT"]) if row.get("SCORED_AT") else None,
            updated_at=str(row["UPDATED_AT"]) if row.get("UPDATED_AT") else None,
        )
    finally:
        conn.close()


@router.get("/orgair/portfolio", response_model=PortfolioOrgAIRScoringResponse,
            summary="Get last computed Org-AI-R for all 5 CS3 companies (from Snowflake)")
async def get_portfolio_orgair():
    results = []
    for ticker in CS3_PORTFOLIO:
        try:
            row = _fetch_orgair_row(ticker)
            results.append(row if row else OrgAIRScoringRecord(ticker=ticker))
        except Exception as e:
            logger.warning(f"[{ticker}] Failed to fetch SCORING row: {e}")
            results.append(OrgAIRScoringRecord(ticker=ticker))

    scored = sum(1 for r in results if r.org_air is not None)
    return PortfolioOrgAIRScoringResponse(
        status="ok", results=results,
        message=f"{scored}/{len(CS3_PORTFOLIO)} companies have stored Org-AI-R scores",
    )


@router.get("/orgair/{ticker}", response_model=OrgAIRScoringRecord,
            summary="Get last computed Org-AI-R for one company (from Snowflake)")
async def get_orgair(ticker: str):
    from fastapi import HTTPException
    row = _fetch_orgair_row(ticker.upper())
    if not row:
        raise HTTPException(status_code=404, detail=f"No scoring record for {ticker.upper()}. Run POST first.")
    return row