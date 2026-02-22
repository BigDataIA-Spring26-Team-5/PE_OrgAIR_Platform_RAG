"""
Tech Signal Service — Digital Presence
app/services/tech_signal_service.py

Service layer for digital_presence signals.
Uses BuiltWith + Wappalyzer to analyze actual company tech stacks.
Stores results in S3 (raw) + Snowflake (metadata/scores).

NO local file storage. NO job-posting-derived tech data.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from app.pipelines.tech_signals import TechStackCollector, TechStackResult
from app.services.base_signal_service import BaseSignalService
from app.services.s3_storage import get_s3_service
from app.repositories.company_repository import CompanyRepository
from app.repositories.signal_repository import get_signal_repository
from app.services.utils import make_singleton_factory

logger = logging.getLogger(__name__)


class TechSignalService(BaseSignalService):
    """Service to extract digital presence signals from website tech stacks."""

    signal_category = "digital_presence"
    summary_field = "digital_score"

    def __init__(self):
        self.collector = TechStackCollector()
        self.s3 = get_s3_service()
        self.company_repo = CompanyRepository()
        self.signal_repo = get_signal_repository()

    async def _collect(self, ticker: str, company_id: str, company: dict, **kwargs) -> dict:
        result: TechStackResult = await self.collector.analyze_company(
            company_id=company_id,
            ticker=ticker,
        )
        self._store_to_s3(ticker, result)

        return {
            "source": "builtwith_wappalyzer",
            "signal_date": datetime.now(timezone.utc),
            "raw_value": (
                f"Tech stack analysis: {len(result.technologies)} techs detected "
                f"from {result.domain}"
            ),
            "normalized_score": result.score,
            "confidence": result.confidence,
            "metadata": {
                "domain": result.domain,
                "score": result.score,
                "ai_tools_score": result.ai_tools_score,
                "infra_score": result.infra_score,
                "breadth_score": result.breadth_score,
                "builtwith_live_count": result.builtwith_total_live,
                "wappalyzer_tech_count": len(result.wappalyzer_techs),
                "ai_technologies": [t.name for t in result.technologies if t.is_ai_related],
                "analysis_sources": self._active_sources(result),
                "errors": result.errors,
            },
            # extra fields used by _build_response
            "breakdown": {
                "sophistication_score": round(result.ai_tools_score, 1),
                "infrastructure_score": round(result.infra_score, 1),
                "breadth_score": round(result.breadth_score, 1),
            },
            "tech_metrics": {
                "domain": result.domain,
                "total_technologies": len(result.technologies),
                "builtwith_live_count": result.builtwith_total_live,
                "wappalyzer_tech_count": len(result.wappalyzer_techs),
                "ai_technologies": [t.name for t in result.technologies if t.is_ai_related],
            },
            "data_sources": self._active_sources(result),
            "collected_at": result.collected_at,
            "errors": result.errors,
        }

    def _build_response(self, ticker: str, company: dict, result: dict) -> dict:
        return {
            "ticker": ticker,
            "company_id": str(company["id"]),
            "company_name": company.get("name", ticker),
            "normalized_score": round(result["normalized_score"], 2),
            "confidence": round(result["confidence"], 3),
            "breakdown": result["breakdown"],
            "tech_metrics": result["tech_metrics"],
            "data_sources": result["data_sources"],
            "collected_at": result["collected_at"],
            "errors": result["errors"],
        }

    async def analyze_company(self, ticker: str, force_refresh: bool = False) -> Dict[str, Any]:
        ticker = ticker.upper()
        logger.info("=" * 60)
        logger.info(f"🌐 ANALYZING DIGITAL PRESENCE FOR: {ticker}")
        logger.info("=" * 60)
        try:
            result = await super().analyze_company(ticker)
            logger.info("=" * 60)
            logger.info(f"📊 DIGITAL PRESENCE COMPLETE: {ticker}")
            logger.info("=" * 60)
            return result
        except Exception as e:
            logger.error(f"❌ Error analyzing digital presence for {ticker}: {e}")
            raise

    def _store_to_s3(self, ticker: str, result: TechStackResult) -> None:
        try:
            data = TechStackCollector.result_to_dict(result)
            self.s3.store_signal_data(signal_type="digital", ticker=ticker, data=data)
            logger.info(f"  📤 Stored tech stack data to S3 for {ticker}")
        except Exception as e:
            logger.warning(f"  ⚠️ Failed to store to S3: {e}")

    @staticmethod
    def _active_sources(result: TechStackResult) -> List[str]:
        sources = []
        if result.builtwith_groups:
            sources.append("builtwith")
        if result.wappalyzer_techs:
            sources.append("wappalyzer")
        return sources or ["none"]

    async def analyze_all_companies(self, force_refresh: bool = False) -> Dict[str, Any]:
        tickers = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
        logger.info("=" * 60)
        logger.info("🌐 ANALYZING DIGITAL PRESENCE FOR ALL COMPANIES")
        logger.info("=" * 60)
        results, success, failed = [], 0, 0
        for ticker in tickers:
            try:
                r = await self.analyze_company(ticker, force_refresh)
                results.append({
                    "ticker": ticker, "status": "success",
                    "score": r["normalized_score"],
                    "technologies": r["tech_metrics"]["total_technologies"],
                })
                success += 1
            except Exception as e:
                logger.error(f"❌ {ticker}: {e}")
                results.append({"ticker": ticker, "status": "failed", "error": str(e)})
                failed += 1
        logger.info(f"✅ Done: {success} succeeded, {failed} failed")
        return {"total": len(tickers), "successful": success, "failed": failed, "results": results}


get_tech_signal_service = make_singleton_factory(TechSignalService)
