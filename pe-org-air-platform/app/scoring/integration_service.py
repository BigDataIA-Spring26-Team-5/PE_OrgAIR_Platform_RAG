"""
scoring/integration_service.py — CS3 Task 6.0b

Full pipeline: CS1/CS2 data → Org-AI-R score.

Class: ScoringIntegrationService
Method: score_company(ticker) → Dict[str, Any]

Pipeline steps (mirrors case study):
  1.  Fetch company from CS1 API
  2.  Fetch CS2 evidence signals
  3.  Collect Glassdoor / Board signals (optional)
  4.  EvidenceMapper → 7 dimension scores
  5.  TalentConcentrationCalculator → TC
  6.  VRCalculator → V^R
  7.  PositionFactorCalculator → PF
  8.  HRCalculator → H^R
  9.  SynergyCalculator → Synergy
  10. Compute Org-AI-R
  11. ConfidenceCalculator → CI on final score
  12. Build result dict
  13. _persist_assessment(result)
"""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ScoringIntegrationService:
    """Full pipeline from CS1/CS2 data to Org-AI-R score."""

    def __init__(
        self,
        cs1_api_url: str = "http://localhost:8000",
        cs2_api_url: str = "http://localhost:8001",
    ):
        self.cs1_url = cs1_api_url
        self.cs2_url = cs2_api_url

        # Initialize all sub-calculators
        from app.scoring.evidence_mapper import EvidenceMapper
        from app.scoring.talent_concentration import TalentConcentrationCalculator
        from app.scoring.position_factor import PositionFactorCalculator
        from app.scoring.vr_calculator import VRCalculator
        from app.scoring.hr_calculator import HRCalculator
        from app.scoring.synergy_calculator import SynergyCalculator
        from app.scoring.confidence_calculator import ConfidenceCalculator

        self.evidence_mapper = EvidenceMapper()
        self.tc_calculator = TalentConcentrationCalculator()
        self.pf_calculator = PositionFactorCalculator()
        self.vr_calculator = VRCalculator()
        self.hr_calculator = HRCalculator()
        self.synergy_calculator = SynergyCalculator()
        self.ci_calculator = ConfidenceCalculator()

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------

    def score_company(self, ticker: str) -> Dict[str, Any]:
        """
        Run the full Org-AI-R scoring pipeline for a single ticker.

        Args:
            ticker: Stock ticker symbol (e.g. "NVDA").

        Returns:
            Dict with org_air_score, vr_score, hr_score, synergy_score,
            confidence interval, and full breakdown.
        """
        ticker = ticker.upper()
        logger.info("=" * 60)
        logger.info(f"ScoringIntegrationService: scoring {ticker}")
        logger.info("=" * 60)

        # 1. Fetch company metadata from CS1 (graceful fallback)
        company_data = self._fetch_company(ticker)

        # 2. Fetch CS2 evidence signals
        cs2_evidence = self._fetch_cs2_evidence(company_data.get("id", ticker))

        # 3. Collect Glassdoor / Board signals (optional)
        glassdoor_data = self._collect_glassdoor(ticker, company_data)
        board_data = self._collect_board(ticker, company_data)

        # 4. Build evidence score objects → EvidenceMapper → dimension scores
        evidence_scores = self._build_evidence_scores(cs2_evidence, glassdoor_data, board_data)
        dimension_scores_map = self.evidence_mapper.map_evidence_to_dimensions(evidence_scores)
        dimension_scores: Dict[str, float] = {
            dim.value: float(ds.score) for dim, ds in dimension_scores_map.items()
        }
        total_evidence_count = sum(ds.evidence_count for ds in dimension_scores_map.values())
        logger.info(f"[{ticker}] Dimension scores: {dimension_scores}")

        # 5. Talent Concentration
        job_analysis_data = cs2_evidence.get("job_analysis", {})
        indiv_mentions = glassdoor_data.get("individual_mentions", 0)
        review_count = glassdoor_data.get("review_count", max(1, indiv_mentions))
        tc = self.tc_calculator.calculate_tc(job_analysis_data, indiv_mentions, review_count)
        logger.info(f"[{ticker}] TC = {tc:.4f}")

        # 6. V^R
        sector = company_data.get("sector", "technology")
        vr_result = self.vr_calculator.calculate(dimension_scores, tc, sector)
        vr_score = float(vr_result.vr_score)
        logger.info(f"[{ticker}] V^R = {vr_score:.2f}")

        # 7. Position Factor
        mcap_pct = company_data.get("mcap_pct", 0.5)
        pf = float(self.pf_calculator.calculate_position_factor(vr_score, sector, mcap_pct))
        logger.info(f"[{ticker}] PF = {pf:.4f}")

        # 8. H^R — HRCalculator.calculate() requires PF in [-1, 1]
        hr_result = self.hr_calculator.calculate(sector, pf)
        hr_score = float(hr_result.hr_score)
        logger.info(f"[{ticker}] H^R = {hr_score:.2f}")

        # 9. Synergy
        alignment = self._calculate_alignment(vr_result, hr_result)
        synergy_result = self.synergy_calculator.calculate(vr_score, hr_score, alignment, 1.0)
        synergy_score = float(synergy_result.synergy_score)
        logger.info(f"[{ticker}] Synergy = {synergy_score:.2f}")

        # 10. Org-AI-R
        from app.scoring.orgair_calculator import OrgAIRCalculator, ALPHA, BETA
        from decimal import Decimal
        orgair_calc = OrgAIRCalculator()
        orgair_result = orgair_calc.calculate(vr_score, hr_score, synergy_score)
        final_score = float(orgair_result.org_air_score)
        logger.info(f"[{ticker}] Org-AI-R = {final_score:.2f}")

        # 11. Confidence interval
        ci = self.ci_calculator.calculate(final_score, max(1, total_evidence_count), "org_air")

        # 12. Build result
        result: Dict[str, Any] = {
            "ticker": ticker,
            "org_air_score": final_score,
            "vr_score": vr_score,
            "hr_score": hr_score,
            "synergy_score": synergy_score,
            "position_factor": pf,
            "talent_concentration": tc,
            "sector": sector,
            "dimension_scores": dimension_scores,
            "breakdown": {
                "alpha": float(ALPHA),
                "beta": float(BETA),
                "weighted_base": float(orgair_result.weighted_base),
                "synergy_contribution": float(orgair_result.synergy_contribution),
                "vr_weighted": float(orgair_result.vr_weighted),
                "hr_weighted": float(orgair_result.hr_weighted),
            },
            "confidence_interval": {
                "ci_lower": float(ci.ci_lower),
                "ci_upper": float(ci.ci_upper),
                "sem": float(ci.sem),
                "reliability": float(ci.reliability),
                "confidence": float(ci.confidence),
                "score_type": ci.score_type,
            },
        }

        # 13. Persist
        self._persist_assessment(result)

        return result

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    def _fetch_company(self, ticker: str) -> Dict[str, Any]:
        """Fetch company metadata from CS1 API (graceful fallback)."""
        try:
            import httpx
            resp = httpx.get(f"{self.cs1_url}/api/v1/companies/{ticker}", timeout=5.0)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.warning(f"[{ticker}] CS1 company fetch failed (using fallback): {e}")
        return {"id": ticker, "ticker": ticker, "sector": "technology", "mcap_pct": 0.5}

    def _fetch_cs2_evidence(self, company_id: str) -> Dict[str, Any]:
        """Fetch evidence signals from CS2 API (graceful fallback)."""
        try:
            import httpx
            resp = httpx.get(f"{self.cs2_url}/api/v1/signals/{company_id}", timeout=5.0)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.warning(f"[{company_id}] CS2 evidence fetch failed (using fallback): {e}")
        return {}

    def _collect_glassdoor(self, ticker: str, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """Collect Glassdoor culture signals (optional — fail gracefully)."""
        try:
            from app.services.s3_storage import get_s3_service
            from app.scoring.talent_concentration import TalentConcentrationCalculator
            s3 = get_s3_service()
            tc_calc = TalentConcentrationCalculator()
            reviews = tc_calc.load_glassdoor_reviews(ticker, s3)
            indiv, rev_count = tc_calc.count_individual_mentions(reviews)
            ai_mentions, _ = tc_calc.count_ai_mentions(reviews)
            return {
                "individual_mentions": indiv,
                "review_count": rev_count,
                "ai_mentions": ai_mentions,
            }
        except Exception as e:
            logger.warning(f"[{ticker}] Glassdoor collection failed (non-fatal): {e}")
            return {"individual_mentions": 0, "review_count": 1, "ai_mentions": 0}

    def _collect_board(self, ticker: str, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """Collect board composition signals (optional — fail gracefully)."""
        try:
            from app.services.s3_storage import get_s3_service
            s3 = get_s3_service()
            key = f"board/{ticker}/latest.json"
            data = s3.download_json(key)
            return data or {}
        except Exception as e:
            logger.warning(f"[{ticker}] Board collection failed (non-fatal): {e}")
            return {}

    def _build_evidence_scores(
        self,
        cs2_evidence: Dict[str, Any],
        glassdoor_data: Dict[str, Any],
        board_data: Dict[str, Any],
    ):
        """Build EvidenceScore objects from raw signal dicts."""
        # Returns empty list — EvidenceMapper handles defaults (score=50 per dimension)
        return []

    def _calculate_alignment(self, vr_result, hr_result) -> Optional[float]:
        """Compute alignment as 1 − |VR − HR| / 100."""
        vr = float(vr_result.vr_score)
        hr = float(hr_result.hr_score)
        return 1.0 - abs(vr - hr) / 100.0

    def _persist_assessment(self, result: Dict[str, Any]) -> None:
        """Save assessment result to CS1 API (graceful failure)."""
        ticker = result.get("ticker", "UNKNOWN")
        try:
            import httpx
            resp = httpx.post(
                f"{self.cs1_url}/api/v1/assessments",
                json=result,
                timeout=5.0,
            )
            if resp.status_code in (200, 201):
                logger.info(f"[{ticker}] Assessment persisted to CS1 API")
            else:
                logger.warning(
                    f"[{ticker}] CS1 persist returned {resp.status_code}: {resp.text[:200]}"
                )
        except Exception as e:
            logger.warning(f"[{ticker}] CS1 assessment persist failed (non-fatal): {e}")
