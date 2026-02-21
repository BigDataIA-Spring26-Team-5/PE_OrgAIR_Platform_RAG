# app/services/vr_scoring_service.py
"""
VR Scoring Service — CS3 Task 5.0e + 5.2 integration
------------------------------------------------------
Orchestrates Talent Concentration (TC) and V^R computation for a ticker
by loading raw job postings and Glassdoor reviews directly from S3.

Designed to run AFTER ScoringService.score_company() has produced a
dimension_summary.  It does NOT modify scoring_service.py.

Usage:
    from app.services.vr_scoring_service import get_vr_scoring_service

    dim_summary = scoring_service.score_company("NVDA")["dimension_scores"]
    svc = get_vr_scoring_service()
    result = svc.score("NVDA", dim_summary)

    print(result.vr_result.vr_score)          # e.g. Decimal('58.43')
    print(result.talent_concentration)        # e.g. Decimal('0.1897')
    print(result.vr_result.talent_risk_adj)   # e.g. Decimal('0.9850')
"""
import json
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

from app.scoring.talent_concentration import GlassdoorReview, TalentConcentrationCalculator
from app.scoring.vr_calculator import VRCalculator, VRResult

logger = logging.getLogger(__name__)


@dataclass
class VRScoringResult:
    """Full output from VRScoringService.score()."""
    ticker: str
    talent_concentration: Decimal
    individual_mentions: int      # Reviews naming a specific executive
    review_count: int
    total_jobs: int               # Raw job postings loaded from S3
    ai_jobs: int                  # Subset classified as AI roles
    vr_result: VRResult           # .vr_score, .talent_risk_adj, .weighted_dim_score


class VRScoringService:
    """Orchestrates TC → V^R computation from S3 data."""

    def __init__(self) -> None:
        self._tc = TalentConcentrationCalculator()
        self._vr = VRCalculator()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def score(
        self,
        ticker: str,
        dimension_summary: list,
    ) -> VRScoringResult:
        """
        Compute TC and V^R for a ticker given its dimension scores.

        Args:
            ticker: Stock ticker, e.g. "NVDA".
            dimension_summary: List of dicts with keys "dimension" (str) and
                               "score" (float 0-100).  This is the
                               dimension_scores value returned by
                               ScoringService.score_company().

        Returns:
            VRScoringResult with TC, V^R, and all intermediate values.
        """
        ticker = ticker.upper()

        job_postings   = self._load_jobs_from_s3(ticker)
        glassdoor_revs = self._load_glassdoor_from_s3(ticker)

        job_analysis              = self._tc.analyze_job_postings(job_postings)
        indiv_mentions, rev_count = self._tc.count_individual_mentions(glassdoor_revs)
        tc                        = self._tc.calculate_tc(
            job_analysis, indiv_mentions, rev_count,
        )

        dim_score_dict = {row["dimension"]: row["score"] for row in dimension_summary}
        vr_result      = self._vr.calculate(dim_score_dict, float(tc))

        logger.info(
            "[%s] TC=%.4f  TalentRiskAdj=%.4f  V^R=%.2f"
            "  (ai_jobs=%d  individual_mentions=%d/%d)",
            ticker, tc, vr_result.talent_risk_adj, vr_result.vr_score,
            job_analysis.total_ai_jobs, indiv_mentions, rev_count,
        )

        return VRScoringResult(
            ticker=ticker,
            talent_concentration=tc,
            individual_mentions=indiv_mentions,
            review_count=rev_count,
            total_jobs=len(job_postings),
            ai_jobs=job_analysis.total_ai_jobs,
            vr_result=vr_result,
        )

    # ------------------------------------------------------------------
    # S3 loaders (logic proven in test_tc_real.py)
    # ------------------------------------------------------------------

    def _load_jobs_from_s3(self, ticker: str) -> list:
        """
        Load job postings from s3://...signals/jobs/{ticker}/*.json.
        Iterates newest → oldest; returns the first non-empty job_postings list.
        Normalises ai_keywords_found → ai_skills_found so that
        TalentConcentrationCalculator.analyze_job_postings() works correctly.
        Returns [] on any error (TC will use safe defaults).
        """
        from app.services.s3_storage import get_s3_service
        s3 = get_s3_service()
        prefix = f"signals/jobs/{ticker}/"
        try:
            keys = s3.list_files(prefix)
            for key in sorted(keys, reverse=True):
                raw = s3.get_file(key)
                if raw is None:
                    continue
                data = json.loads(raw)
                postings = data.get("job_postings", [])
                if postings:
                    for p in postings:
                        if "ai_skills_found" not in p:
                            p["ai_skills_found"] = p.get("ai_keywords_found", [])
                    logger.info("[%s] Jobs: %d postings from %s", ticker, len(postings), key)
                    return postings
                logger.debug("[%s] Skipping empty job file: %s", ticker, key)
        except Exception as exc:
            logger.warning("[%s] Job S3 load failed: %s", ticker, exc)
        return []

    def _load_glassdoor_from_s3(self, ticker: str) -> List[GlassdoorReview]:
        """
        Load Glassdoor reviews from s3://.../glassdoor_signals/raw/{ticker}_raw.json.
        Returns [] on any error (TC individual_factor will use the 0.5 neutral default).
        """
        from app.services.s3_storage import get_s3_service
        s3 = get_s3_service()
        key = f"glassdoor_signals/raw/{ticker}_raw.json"
        try:
            raw = s3.get_file(key)
            if raw is None:
                logger.warning("[%s] No Glassdoor snapshot at %s", ticker, key)
                return []
            wrapper = json.loads(raw)
            reviews = [
                GlassdoorReview(
                    review_id=r.get("review_id", ""),
                    rating=float(r.get("rating") or 0.0),
                    title=r.get("title") or "",
                    pros=r.get("pros") or "",
                    cons=r.get("cons") or "",
                    advice_to_management=r.get("advice_to_management"),
                    is_current_employee=bool(r.get("is_current_employee", False)),
                    job_title=r.get("job_title") or "",
                    review_date=r.get("review_date"),
                    source=r.get("source", "unknown"),
                )
                for r in wrapper.get("reviews", [])
            ]
            logger.info("[%s] Glassdoor: %d reviews from %s", ticker, len(reviews), key)
            return reviews
        except Exception as exc:
            logger.warning("[%s] Glassdoor S3 load failed: %s", ticker, exc)
            return []


# ---------------------------------------------------------------------------
# Singleton accessor
# ---------------------------------------------------------------------------

_service: Optional[VRScoringService] = None


def get_vr_scoring_service() -> VRScoringService:
    global _service
    if _service is None:
        _service = VRScoringService()
    return _service
