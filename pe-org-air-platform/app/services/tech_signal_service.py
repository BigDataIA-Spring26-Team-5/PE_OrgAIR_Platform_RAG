# # app/services/tech_signal_service.py
# import json
# import logging
# from typing import Dict, List, Optional
# from datetime import datetime, timezone
# from app.services.job_data_service import get_tech_signal_service
# from app.services.s3_storage import get_s3_service
# from app.repositories.company_repository import CompanyRepository
# from app.repositories.signal_repository import get_signal_repository

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s | %(levelname)-8s | %(message)s',
#     datefmt='%H:%M:%S'
# )
# logger = logging.getLogger(__name__)


# class TechSignalService:
#     """Service to extract digital presence signals from tech stack analysis."""
    
#     def __init__(self):
#         self.job_data_service = get_job_data_service()
#         self.s3_service = get_s3_service()
#         self.company_repo = CompanyRepository()
#         self.signal_repo = get_signal_repository()
    
#     async def analyze_company(self, ticker: str, force_refresh: bool = False) -> Dict:
#         """
#         Analyze digital presence for a company and create digital_presence signals.
#         Uses tech stack analysis from shared job data.
        
#         Args:
#             ticker: Company ticker symbol
#             force_refresh: If True, force fresh data collection
            
#         Returns:
#             Dictionary with analysis results
#         """
#         ticker = ticker.upper()
#         logger.info("=" * 60)
#         logger.info(f"🎯 ANALYZING DIGITAL PRESENCE SIGNALS FOR: {ticker}")
#         logger.info("=" * 60)
        
#         # Get company from database
#         company = self.company_repo.get_by_ticker(ticker)
#         if not company:
#             raise ValueError(f"Company not found: {ticker}")
        
#         company_id = str(company['id'])
#         company_name = company['name']
#         logger.info(f"✅ Found company: {company_name} (ID: {company_id})")
        
#         # Delete existing digital_presence signals for fresh analysis
#         deleted = self.signal_repo.delete_signals_by_category(company_id, "digital_presence")
#         if deleted:
#             logger.info(f"  🗑️ Deleted {deleted} existing digital_presence signals")
        
#         try:
#             # Get job data (collect fresh if needed)
#             logger.info("📊 Getting job data for tech stack analysis...")
#             job_data = await self.job_data_service.collect_job_data(ticker, force_refresh=force_refresh)

#             if not job_data or "job_postings" not in job_data:
#                 raise ValueError(f"No job data available for {ticker}")

#             # Store raw tech stack data to S3 BEFORE analysis
#             logger.info("💾 Storing raw tech stack data to S3...")
#             try:
#                 # Extract tech keywords from job postings for raw storage
#                 all_tech_keywords = []
#                 for posting in job_data.get("job_postings", []):
#                     all_tech_keywords.extend(posting.get("techstack_keywords_found", []))

#                 raw_techstack_data = {
#                     "company_id": company_id,
#                     "company_name": company_name,
#                     "ticker": ticker,
#                     "total_jobs": len(job_data.get("job_postings", [])),
#                     "raw_tech_keywords": list(set(all_tech_keywords)),
#                     "data_collected_at": job_data.get("collected_at"),
#                     "stored_at": datetime.now(timezone.utc).isoformat()
#                 }
#                 self.s3_service.store_signal_data(
#                     signal_type="techstack",
#                     ticker=ticker,
#                     data=raw_techstack_data
#                 )
#                 logger.info(f"  📤 Stored raw tech stack data to S3 for {ticker}")
#             except Exception as e:
#                 logger.warning(f"  ⚠️ Failed to store raw tech stack data to S3: {e}")

#             # Analyze tech stack from the shared job data (AFTER storing raw data)
#             logger.info("🔧 Analyzing tech stack...")
#             analysis_result = self.job_data_service.analyze_tech_stack(job_data)
            
#             techstack_score = analysis_result["techstack_scores"].get(company_id, 0.0)
#             techstack_keywords = analysis_result.get("techstack_keywords", [])
#             total_jobs = analysis_result["total_jobs"]
            
#             # Calculate confidence based on data quality
#             confidence = self._calculate_confidence(total_jobs, len(techstack_keywords))
            
#             # Create signal record
#             self.signal_repo.create_signal(
#                 company_id=company_id,
#                 category="digital_presence",
#                 source="tech_stack_analysis",
#                 signal_date=datetime.now(timezone.utc),
#                 raw_value=f"Tech stack analysis: {len(techstack_keywords)} tech keywords from {total_jobs} jobs",
#                 normalized_score=techstack_score,
#                 confidence=confidence,
#                 metadata={
#                     "techstack_score": techstack_score,
#                     "techstack_keywords": techstack_keywords,
#                     "total_jobs_analyzed": total_jobs,
#                     "unique_tech_keywords": len(techstack_keywords),
#                     "data_collected_at": job_data.get("collected_at"),
#                     "analysis_method": "tech_stack_scoring",
#                     "notes": "Based on job posting analysis. Could be extended with BuiltWith/SimilarTech APIs."
#                 }
#             )
            
#             # Update company signal summary
#             logger.info("-" * 40)
#             logger.info(f"📊 Updating company signal summary...")
#             self.signal_repo.upsert_summary(
#                 company_id=company_id,
#                 ticker=ticker,
#                 digital_score=techstack_score
#             )
            
#             # Summary
#             logger.info("=" * 60)
#             logger.info(f"📊 DIGITAL PRESENCE ANALYSIS COMPLETE FOR: {ticker}")
#             logger.info(f"   Total jobs analyzed: {total_jobs}")
#             logger.info(f"   Tech Stack Score: {techstack_score:.1f}/100")
#             logger.info(f"   Tech Keywords Found: {len(techstack_keywords)}")
#             logger.info(f"   Confidence: {confidence:.2f}")
#             logger.info(f"   Data freshness: {job_data.get('collected_at', 'unknown')}")
#             if techstack_keywords:
#                 logger.info(f"   Top keywords: {', '.join(techstack_keywords[:5])}{'...' if len(techstack_keywords) > 5 else ''}")
#             logger.info("=" * 60)
            
#             return {
#                 "ticker": ticker,
#                 "company_id": company_id,
#                 "company_name": company_name,
#                 "normalized_score": round(techstack_score, 2),
#                 "confidence": round(confidence, 3),
#                 "breakdown": {
#                     "techstack_score": round(techstack_score, 1)
#                 },
#                 "tech_metrics": {
#                     "total_jobs_analyzed": total_jobs,
#                     "unique_tech_keywords": len(techstack_keywords),
#                     "tech_keywords": techstack_keywords
#                 },
#                 "data_freshness": job_data.get("collected_at"),
#                 "analysis_method": "tech_stack_scoring",
#                 "notes": "Based on job posting analysis. Could be extended with BuiltWith/SimilarTech APIs."
#             }
            
#         except Exception as e:
#             logger.error(f"❌ Error analyzing tech signals for {ticker}: {e}")
#             raise
    
#     def _calculate_confidence(self, total_jobs: int, unique_keywords: int) -> float:
#         """
#         Calculate confidence score based on data quality.
        
#         Factors:
#         1. Total jobs analyzed (more data = higher confidence)
#         2. Unique tech keywords found (more keywords = higher confidence)
#         3. Keyword density (keywords per job)
#         """
#         if total_jobs == 0:
#             return 0.3  # Low confidence with no data
        
#         # Base confidence from total jobs (0.3 to 0.7)
#         total_confidence = min(0.7, 0.3 + (total_jobs / 50.0))
        
#         # Keyword count confidence (more keywords = better)
#         keyword_confidence = min(0.5, unique_keywords / 20.0)
        
#         # Keyword density confidence (balanced is best)
#         keyword_density = unique_keywords / total_jobs if total_jobs > 0 else 0
#         density_confidence = min(1.0, keyword_density * 10.0)  # 0.1 keywords per job = 1.0 confidence
        
#         # Combined confidence (weighted average)
#         confidence = (total_confidence * 0.4) + (keyword_confidence * 0.3) + (density_confidence * 0.3)
        
#         return min(0.95, max(0.3, confidence))  # Clamp between 0.3 and 0.95
    
#     async def analyze_all_companies(self, force_refresh: bool = False) -> Dict:
#         """Analyze digital presence signals for all target companies."""
#         target_tickers = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
        
#         logger.info("=" * 60)
#         logger.info("🎯 ANALYZING DIGITAL PRESENCE SIGNALS FOR ALL COMPANIES")
#         logger.info(f"   Force refresh: {force_refresh}")
#         logger.info("=" * 60)
        
#         results = []
#         success_count = 0
#         failed_count = 0
        
#         for ticker in target_tickers:
#             try:
#                 result = await self.analyze_company(ticker, force_refresh=force_refresh)
#                 results.append({
#                     "ticker": ticker,
#                     "status": "success",
#                     "score": result["normalized_score"],
#                     "jobs_analyzed": result["tech_metrics"]["total_jobs_analyzed"],
#                     "tech_keywords": result["tech_metrics"]["unique_tech_keywords"]
#                 })
#                 success_count += 1
#             except Exception as e:
#                 logger.error(f"❌ Failed to analyze {ticker}: {e}")
#                 results.append({
#                     "ticker": ticker,
#                     "status": "failed",
#                     "error": str(e)
#                 })
#                 failed_count += 1
        
#         logger.info("=" * 60)
#         logger.info("📊 ALL COMPANIES DIGITAL PRESENCE ANALYSIS COMPLETE")
#         logger.info(f"   Successful: {success_count}")
#         logger.info(f"   Failed: {failed_count}")
#         logger.info(f"   Note: Based on job posting analysis")
#         logger.info(f"   Future: Could be extended with BuiltWith/SimilarTech APIs")
#         logger.info("=" * 60)
        
#         return {
#             "total_companies": len(target_tickers),
#             "successful": success_count,
#             "failed": failed_count,
#             "force_refresh": force_refresh,
#             "results": results,
#             "notes": "Based on job posting analysis. Could be extended with BuiltWith/SimilarTech APIs."
#         }


# # Singleton
# _service: Optional[TechSignalService] = None

# def get_tech_signal_service() -> TechSignalService:
#     global _service
#     if _service is None:
#         _service = TechSignalService()
#     return _service

"""
Tech Signal Service — Digital Presence
app/services/tech_signal_service.py

Service layer for digital_presence signals.
Uses BuiltWith + Wappalyzer to analyze actual company tech stacks.
Stores results in S3 (raw) + Snowflake (metadata/scores).

NO local file storage. NO job-posting-derived tech data.
"""
# from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.pipelines.tech_signals import TechStackCollector, TechStackResult
from app.services.s3_storage import get_s3_service
from app.repositories.company_repository import CompanyRepository
from app.repositories.signal_repository import get_signal_repository

logger = logging.getLogger(__name__)


class TechSignalService:
    """Service to extract digital presence signals from website tech stacks."""

    def __init__(self):
        self.collector = TechStackCollector()
        self.s3 = get_s3_service()
        self.company_repo = CompanyRepository()
        self.signal_repo = get_signal_repository()

    async def analyze_company(
        self, ticker: str, force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Analyze digital presence for a company via BuiltWith + Wappalyzer.

        Steps:
          1. Look up company in Snowflake
          2. Delete stale digital_presence signals
          3. Run BuiltWith + Wappalyzer analysis
          4. Store raw results to S3
          5. Insert signal record to Snowflake
          6. Update company signal summary

        Args:
            ticker: Company ticker symbol
            force_refresh: Currently unused (no caching yet)

        Returns:
            Dict with normalized_score, confidence, breakdown, etc.
        """
        ticker = ticker.upper()
        logger.info("=" * 60)
        logger.info(f"🌐 ANALYZING DIGITAL PRESENCE FOR: {ticker}")
        logger.info("=" * 60)

        # 1. Look up company
        company = self.company_repo.get_by_ticker(ticker)
        if not company:
            raise ValueError(f"Company not found: {ticker}")

        company_id = str(company["id"])
        company_name = company["name"]
        logger.info(f"✅ Company: {company_name} (ID: {company_id})")

        # 2. Delete existing digital_presence signals
        deleted = self.signal_repo.delete_signals_by_category(
            company_id, "digital_presence"
        )
        if deleted:
            logger.info(f"  🗑️ Deleted {deleted} existing digital_presence signals")

        try:
            # 3. Run tech stack analysis (BuiltWith + Wappalyzer)
            result: TechStackResult = await self.collector.analyze_company(
                company_id=company_id,
                ticker=ticker,
            )

            # 4. Store raw results to S3
            self._store_to_s3(ticker, result)

            # 5. Insert signal record to Snowflake
            self.signal_repo.create_signal(
                company_id=company_id,
                category="digital_presence",
                source="builtwith_wappalyzer",
                signal_date=datetime.now(timezone.utc),
                raw_value=(
                    f"Tech stack analysis: {len(result.technologies)} techs detected "
                    f"from {result.domain}"
                ),
                normalized_score=result.score,
                confidence=result.confidence,
                metadata={
                    "domain": result.domain,
                    "score": result.score,
                    "ai_tools_score": result.ai_tools_score,
                    "infra_score": result.infra_score,
                    "breadth_score": result.breadth_score,
                    "builtwith_live_count": result.builtwith_total_live,
                    "wappalyzer_tech_count": len(result.wappalyzer_techs),
                    "ai_technologies": [
                        t.name for t in result.technologies if t.is_ai_related
                    ],
                    "analysis_sources": self._active_sources(result),
                    "errors": result.errors,
                },
            )

            # 6. Update company signal summary
            logger.info("📊 Updating company signal summary...")
            self.signal_repo.upsert_summary(
                company_id=company_id,
                ticker=ticker,
                digital_score=result.score,
            )

            # Log summary
            logger.info("=" * 60)
            logger.info(f"📊 DIGITAL PRESENCE COMPLETE: {ticker}")
            logger.info(f"   Domain: {result.domain}")
            logger.info(f"   Score: {result.score:.1f}/100")
            logger.info(f"   Sophistication: {result.ai_tools_score:.0f}/40")
            logger.info(f"   Infrastructure: {result.infra_score:.0f}/30")
            logger.info(f"   Breadth: {result.breadth_score:.0f}/30")
            logger.info(f"   Confidence: {result.confidence:.2f}")
            logger.info(f"   Sources: {', '.join(self._active_sources(result))}")
            if result.errors:
                logger.warning(f"   Warnings: {result.errors}")
            logger.info("=" * 60)

            return {
                "ticker": ticker,
                "company_id": company_id,
                "company_name": company_name,
                "normalized_score": round(result.score, 2),
                "confidence": round(result.confidence, 3),
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
                    "ai_technologies": [
                        t.name for t in result.technologies if t.is_ai_related
                    ],
                },
                "data_sources": self._active_sources(result),
                "collected_at": result.collected_at,
                "errors": result.errors,
            }

        except Exception as e:
            logger.error(f"❌ Error analyzing digital presence for {ticker}: {e}")
            raise

    def _store_to_s3(self, ticker: str, result: TechStackResult) -> None:
        """Store raw tech stack data to S3."""
        try:
            data = TechStackCollector.result_to_dict(result)
            self.s3.store_signal_data(
                signal_type="digital",
                ticker=ticker,
                data=data,
            )
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

    async def analyze_all_companies(
        self, force_refresh: bool = False
    ) -> Dict[str, Any]:
        """Analyze digital presence for all 10 target companies."""
        tickers = [
            "CAT", "DE", "UNH", "HCA", "ADP",
            "PAYX", "WMT", "TGT", "JPM", "GS",
        ]

        logger.info("=" * 60)
        logger.info("🌐 ANALYZING DIGITAL PRESENCE FOR ALL COMPANIES")
        logger.info("=" * 60)

        results, success, failed = [], 0, 0

        for ticker in tickers:
            try:
                r = await self.analyze_company(ticker, force_refresh)
                results.append({
                    "ticker": ticker,
                    "status": "success",
                    "score": r["normalized_score"],
                    "technologies": r["tech_metrics"]["total_technologies"],
                })
                success += 1
            except Exception as e:
                logger.error(f"❌ {ticker}: {e}")
                results.append({"ticker": ticker, "status": "failed", "error": str(e)})
                failed += 1

        logger.info(f"✅ Done: {success} succeeded, {failed} failed")
        return {
            "total": len(tickers),
            "successful": success,
            "failed": failed,
            "results": results,
        }


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------
_service: Optional[TechSignalService] = None


def get_tech_signal_service() -> TechSignalService:
    global _service
    if _service is None:
        _service = TechSignalService()
    return _service

