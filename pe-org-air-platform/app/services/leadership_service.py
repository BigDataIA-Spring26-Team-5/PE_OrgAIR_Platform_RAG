# #app/services/leadership_service.py
# import json
# import logging
# from typing import Dict, List, Optional
# from datetime import datetime, timezone
# from app.pipelines.leadership_analyzer import get_leadership_analyzer, LeadershipScores
# from app.services.s3_storage import get_s3_service
# from app.repositories.document_repository import get_document_repository
# from app.repositories.company_repository import CompanyRepository
# from app.repositories.signal_repository import get_signal_repository

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s | %(levelname)-8s | %(message)s',
#     datefmt='%H:%M:%S'
# )
# logger = logging.getLogger(__name__)


# class LeadershipSignalService:
#     """Service to extract leadership signals from DEF 14A filings."""
    
#     def __init__(self):
#         self.analyzer = get_leadership_analyzer()
#         self.s3_service = get_s3_service()
#         self.doc_repo = get_document_repository()
#         self.company_repo = CompanyRepository()
#         self.signal_repo = get_signal_repository()
    
#     def _get_parsed_s3_key(self, ticker: str, filing_type: str, filing_date: str) -> str:
#         """Get S3 key for parsed DEF 14A content."""
#         clean_filing_type = filing_type.replace(" ", "")
#         return f"sec/parsed/{ticker}/{clean_filing_type}/{filing_date}_full.json"
    
#     async def analyze_company(self, ticker: str) -> Dict:
#         """
#         Analyze all DEF 14A filings for a company and create leadership signals.
#         """
#         ticker = ticker.upper()
#         logger.info("=" * 60)
#         logger.info(f"🎯 ANALYZING LEADERSHIP SIGNALS FOR: {ticker}")
#         logger.info("=" * 60)
        
#         # Get company from database
#         company = self.company_repo.get_by_ticker(ticker)
#         if not company:
#             raise ValueError(f"Company not found: {ticker}")
        
#         company_id = str(company['id'])
#         logger.info(f"✅ Found company: {company['name']} (ID: {company_id})")
        
#         # Get all DEF 14A documents for this company
#         all_docs = self.doc_repo.get_by_ticker(ticker)
#         def14a_docs = [d for d in all_docs if d['filing_type'] in ['DEF 14A', 'DEF14A']]
        
#         if not def14a_docs:
#             logger.warning(f"❌ No DEF 14A filings found for: {ticker}")
#             raise ValueError(f"No DEF 14A filings found for: {ticker}")
        
#         logger.info(f"📚 Found {len(def14a_docs)} DEF 14A filings to analyze")
        
#         # Delete existing leadership signals for fresh analysis
#         deleted = self.signal_repo.delete_signals_by_category(company_id, "leadership_signals")
#         if deleted:
#             logger.info(f"  🗑️ Deleted {deleted} existing leadership signals")
        
#         # Analyze each filing
#         all_scores: List[LeadershipScores] = []
#         filing_dates = []
#         signals_created = 0
        
#         for idx, doc in enumerate(def14a_docs, 1):
#             filing_date = str(doc['filing_date'])
#             logger.info("-" * 40)
#             logger.info(f"📄 [{idx}/{len(def14a_docs)}] DEF 14A | {filing_date}")
            
#             # Get parsed content from S3
#             s3_key = self._get_parsed_s3_key(ticker, doc['filing_type'], filing_date)
#             logger.info(f"  ⬇️ Loading parsed content: {s3_key}")
            
#             try:
#                 content = self.s3_service.get_file(s3_key)
#                 if not content:
#                     logger.warning(f"  ⚠️ Parsed content not found, skipping")
#                     continue
                
#                 parsed_data = json.loads(content.decode('utf-8'))
#                 text_content = parsed_data.get('text_content', '')
#                 sections = parsed_data.get('sections', {})
#                 tables = parsed_data.get('tables', [])
                
#                 logger.info(f"  ✅ Loaded {len(text_content):,} chars, {len(sections)} sections, {len(tables)} tables")
                
#                 # Analyze the filing
#                 scores = self.analyzer.analyze(text_content, sections, tables)
#                 all_scores.append(scores)
#                 filing_dates.append(filing_date)
                
#                 # Calculate confidence
#                 confidence = self.analyzer.calculate_confidence(
#                     len(text_content), len(sections), len(tables)
#                 )
                
#                 # Create signal record for this filing
#                 self.signal_repo.create_signal(
#                     company_id=company_id,
#                     category="leadership_signals",
#                     source="sec_filing",
#                     signal_date=datetime.strptime(filing_date, "%Y-%m-%d"),
#                     raw_value=f"DEF 14A analysis: {scores.total_score:.1f}/100",
#                     normalized_score=scores.total_score,
#                     confidence=confidence,
#                     metadata={
#                         "filing_date": filing_date,
#                         "tech_exec_score": scores.tech_exec_score,
#                         "keyword_score": scores.keyword_score,
#                         "performance_metric_score": scores.performance_metric_score,
#                         "board_tech_score": scores.board_tech_score,
#                         "tech_execs_found": scores.tech_execs_found,
#                         "keyword_counts": scores.keyword_counts,
#                         "tech_metrics_found": scores.tech_metrics_found,
#                         "board_indicators": scores.board_indicators
#                     }
#                 )
#                 signals_created += 1
                
#             except Exception as e:
#                 logger.error(f"  ❌ Error analyzing filing: {e}")
#                 continue
        
#         if not all_scores:
#             raise ValueError(f"Could not analyze any DEF 14A filings for: {ticker}")
        
#         # Calculate average score across all filings (with recency weighting)
#         # More recent filings get higher weight
#         weights = list(range(1, len(all_scores) + 1))  # [1, 2, 3, ...] - later = higher
#         total_weight = sum(weights)
        
#         weighted_score = sum(
#             scores.total_score * weight 
#             for scores, weight in zip(all_scores, weights)
#         ) / total_weight
        
#         avg_confidence = sum(
#             self.analyzer.calculate_confidence(50000, 2, 10)  # Approximate
#             for _ in all_scores
#         ) / len(all_scores)
        
#         # Aggregate all findings
#         all_tech_execs = list(set(
#             exec for scores in all_scores for exec in scores.tech_execs_found
#         ))
#         all_keyword_counts = {}
#         for scores in all_scores:
#             for kw, count in scores.keyword_counts.items():
#                 all_keyword_counts[kw] = all_keyword_counts.get(kw, 0) + count
#         all_tech_metrics = list(set(
#             m for scores in all_scores for m in scores.tech_metrics_found
#         ))
#         all_board_indicators = list(set(
#             i for scores in all_scores for i in scores.board_indicators
#         ))
        
#         # Update company signal summary
#         logger.info("-" * 40)
#         logger.info(f"📊 Updating company signal summary...")
#         self.signal_repo.upsert_summary(
#             company_id=company_id,
#             ticker=ticker,
#             leadership_score=round(weighted_score, 2)
#         )

#         # Summary
#         logger.info("=" * 60)
#         logger.info(f"📊 LEADERSHIP ANALYSIS COMPLETE FOR: {ticker}")
#         logger.info(f"   Filings analyzed: {len(all_scores)}")
#         logger.info(f"   Signals created: {signals_created}")
#         logger.info(f"   Average Score: {weighted_score:.1f}/100")
#         logger.info(f"   Tech Execs Found: {all_tech_execs}")
#         logger.info("=" * 60)

#         return {
#             "ticker": ticker,
#             "company_id": company_id,
#             "filing_count_analyzed": len(all_scores),
#             "signals_created": signals_created,
#             "normalized_score": round(weighted_score, 2),
#             "confidence": round(avg_confidence, 3),
#             "breakdown": {
#                 "tech_exec_score": round(sum(s.tech_exec_score for s in all_scores) / len(all_scores), 1),
#                 "keyword_score": round(sum(s.keyword_score for s in all_scores) / len(all_scores), 1),
#                 "performance_metric_score": round(sum(s.performance_metric_score for s in all_scores) / len(all_scores), 1),
#                 "board_tech_score": round(sum(s.board_tech_score for s in all_scores) / len(all_scores), 1),
#                 "total_score": round(weighted_score, 1)
#             },
#             "tech_execs_found": all_tech_execs,
#             "keyword_counts": all_keyword_counts,
#             "tech_linked_metrics_found": all_tech_metrics,
#             "board_tech_indicators": all_board_indicators,
#             "filing_dates": filing_dates
#         }
    
#     async def analyze_all_companies(self) -> Dict:
#         """Analyze leadership signals for all 10 target companies."""
#         target_tickers = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
        
#         logger.info("=" * 60)
#         logger.info("🎯 ANALYZING LEADERSHIP SIGNALS FOR ALL COMPANIES")
#         logger.info("=" * 60)
        
#         results = []
#         success_count = 0
#         failed_count = 0
        
#         for ticker in target_tickers:
#             try:
#                 result = await self.analyze_company(ticker)
#                 results.append({
#                     "ticker": ticker,
#                     "status": "success",
#                     "score": result["normalized_score"],
#                     "filings_analyzed": result["filing_count_analyzed"]
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
#         logger.info("📊 ALL COMPANIES LEADERSHIP ANALYSIS COMPLETE")
#         logger.info(f"   Successful: {success_count}")
#         logger.info(f"   Failed: {failed_count}")
#         logger.info("=" * 60)
        
#         return {
#             "total_companies": len(target_tickers),
#             "successful": success_count,
#             "failed": failed_count,
#             "results": results
#         }


# # Singleton
# _service: Optional[LeadershipSignalService] = None

# def get_leadership_service() -> LeadershipSignalService:
#     global _service
#     if _service is None:
#         _service = LeadershipSignalService()
#     return _service

# app/services/leadership_service.py
"""
Leadership Signal Service — DEF 14A Analysis Orchestrator

ALIGNED WITH:
  - CS2 PDF: leadership_signals category, weight 0.20
  - CS3 PDF page 7: Maps to Leadership(0.60), AI_Governance(0.25), Culture(0.15)

FIXES from audit:
  1. ✅ Sort DEF 14A filings by date before recency weighting
  2. ✅ Use real per-filing confidence (not hardcoded approximation)
  3. ✅ Store CS3 dimension sub-scores in metadata
  4. ✅ Confidence range [0.70-0.92] consistent with other signals
"""
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone

from app.pipelines.leadership_analyzer import (
    get_leadership_analyzer,
    LeadershipScores,
)
from app.services.s3_storage import get_s3_service
from app.repositories.document_repository import get_document_repository
from app.repositories.company_repository import CompanyRepository
from app.repositories.signal_repository import get_signal_repository

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


class LeadershipSignalService:
    """Service to extract leadership signals from DEF 14A filings."""

    def __init__(self):
        self.analyzer = get_leadership_analyzer()
        self.s3_service = get_s3_service()
        self.doc_repo = get_document_repository()
        self.company_repo = CompanyRepository()
        self.signal_repo = get_signal_repository()

    def _get_parsed_s3_key(
        self, ticker: str, filing_type: str, filing_date: str
    ) -> str:
        clean = filing_type.replace(" ", "")
        return f"sec/parsed/{ticker}/{clean}/{filing_date}_full.json"

    # ──────────────────────────────────────────────────────────────
    # Single-company analysis
    # ──────────────────────────────────────────────────────────────
    async def analyze_company(self, ticker: str) -> Dict:
        """Analyze all DEF 14A filings for a company."""
        ticker = ticker.upper()
        logger.info("=" * 60)
        logger.info(f"🎯 ANALYZING LEADERSHIP SIGNALS FOR: {ticker}")
        logger.info("=" * 60)

        # ── Resolve company ──
        company = self.company_repo.get_by_ticker(ticker)
        if not company:
            raise ValueError(f"Company not found: {ticker}")

        company_id = str(company["id"])
        company_name = company["name"]
        logger.info(f"✅ Found company: {company_name} (ID: {company_id})")

        # ── Get DEF 14A filings ──
        all_docs = self.doc_repo.get_by_ticker(ticker)
        def14a_docs = [
            d for d in all_docs
            if d["filing_type"] in ("DEF 14A", "DEF14A")
        ]

        if not def14a_docs:
            logger.warning(f"❌ No DEF 14A filings found for: {ticker}")
            raise ValueError(f"No DEF 14A filings found for: {ticker}")

        # FIX #1: Sort by filing_date ascending so recency weights work
        def14a_docs = sorted(
            def14a_docs,
            key=lambda d: str(d["filing_date"]),
        )
        logger.info(
            f"📚 Found {len(def14a_docs)} DEF 14A filings "
            f"(sorted: {str(def14a_docs[0]['filing_date'])} → "
            f"{str(def14a_docs[-1]['filing_date'])})"
        )

        # ── Clear stale signals ──
        deleted = self.signal_repo.delete_signals_by_category(
            company_id, "leadership_signals"
        )
        if deleted:
            logger.info(f"  🗑️ Deleted {deleted} existing leadership signals")

        # ── Analyze each filing ──
        all_scores: List[LeadershipScores] = []
        all_confidences: List[float] = []          # FIX #2: real confidence per filing
        filing_dates: List[str] = []
        signals_created = 0

        for idx, doc in enumerate(def14a_docs, 1):
            filing_date = str(doc["filing_date"])
            logger.info("-" * 40)
            logger.info(f"📄 [{idx}/{len(def14a_docs)}] DEF 14A | {filing_date}")

            s3_key = self._get_parsed_s3_key(
                ticker, doc["filing_type"], filing_date
            )
            logger.info(f"  ⬇️  Loading: {s3_key}")

            try:
                content = self.s3_service.get_file(s3_key)
                if not content:
                    logger.warning("  ⚠️  Parsed content not found, skipping")
                    continue

                parsed = json.loads(content.decode("utf-8"))
                text_content = parsed.get("text_content", "")
                sections = parsed.get("sections", {})
                tables = parsed.get("tables", [])

                logger.info(
                    f"  ✅ Loaded {len(text_content):,} chars, "
                    f"{len(sections)} sections, {len(tables)} tables"
                )

                # Analyze
                scores = self.analyzer.analyze(text_content, sections, tables)
                all_scores.append(scores)
                filing_dates.append(filing_date)

                # FIX #2: Real confidence from actual content metrics
                # Pass cleaned text length for accurate confidence calc
                cleaned_len = len(self.analyzer._clean_xbrl_text(text_content))
                confidence = self.analyzer.calculate_confidence(
                    len(text_content), len(sections), len(tables),
                    cleaned_text_length=cleaned_len,
                )
                all_confidences.append(confidence)

                # Persist per-filing signal with CS3-ready metadata
                self.signal_repo.create_signal(
                    company_id=company_id,
                    category="leadership_signals",
                    source="sec_filing",
                    signal_date=datetime.strptime(filing_date, "%Y-%m-%d"),
                    raw_value=f"DEF 14A analysis: {scores.total_score:.1f}/100",
                    normalized_score=scores.total_score,
                    confidence=confidence,
                    metadata={
                        "filing_date": filing_date,
                        # Component scores
                        "tech_exec_score": scores.tech_exec_score,
                        "strategy_keyword_score": scores.strategy_keyword_score,
                        "comp_metric_score": scores.comp_metric_score,
                        "board_tech_score": scores.board_tech_score,
                        "governance_score": scores.governance_score,
                        "culture_score": scores.culture_score,
                        # CS3 dimension sub-scores
                        "cs3_leadership_sub": scores.leadership_sub,
                        "cs3_governance_sub": scores.governance_sub,
                        "cs3_culture_sub": scores.culture_sub,
                        # Evidence details
                        "tech_execs_found": scores.tech_execs_found,
                        "strategy_keywords_found": scores.strategy_keywords_found,
                        "comp_metrics_found": scores.comp_metrics_found,
                        "board_indicators": scores.board_indicators,
                        "governance_indicators": scores.governance_indicators,
                        "culture_indicators": scores.culture_indicators,
                    },
                )
                signals_created += 1

            except Exception as e:
                logger.error(f"  ❌ Error analyzing filing: {e}")
                continue

        if not all_scores:
            raise ValueError(
                f"Could not analyze any DEF 14A filings for: {ticker}"
            )

        # ──────────────────────────────────────────────────────────
        # Aggregate across filings with recency weighting
        # FIX #1: filings are now sorted oldest→newest
        # Weights: [1, 2, 3, ...] — most recent gets highest weight
        # ──────────────────────────────────────────────────────────
        n = len(all_scores)
        weights = list(range(1, n + 1))
        total_weight = sum(weights)

        def _weighted_avg(values: List[float]) -> float:
            return sum(v * w for v, w in zip(values, weights)) / total_weight

        weighted_total = _weighted_avg([s.total_score for s in all_scores])

        # CS3 sub-score weighted averages
        weighted_leadership_sub = _weighted_avg([s.leadership_sub for s in all_scores])
        weighted_governance_sub = _weighted_avg([s.governance_sub for s in all_scores])
        weighted_culture_sub = _weighted_avg([s.culture_sub for s in all_scores])

        # FIX #2: Real weighted average confidence
        avg_confidence = _weighted_avg(all_confidences)

        # Component averages for breakdown
        avg_tech_exec = _weighted_avg([s.tech_exec_score for s in all_scores])
        avg_strategy_kw = _weighted_avg([s.strategy_keyword_score for s in all_scores])
        avg_comp_metric = _weighted_avg([s.comp_metric_score for s in all_scores])
        avg_board_tech = _weighted_avg([s.board_tech_score for s in all_scores])
        avg_governance = _weighted_avg([s.governance_score for s in all_scores])
        avg_culture = _weighted_avg([s.culture_score for s in all_scores])

        # Aggregate evidence (union across all filings)
        all_tech_execs = sorted({
            e for s in all_scores for e in s.tech_execs_found
        })
        all_strategy_kw: Dict[str, int] = {}
        for s in all_scores:
            for kw, cnt in s.strategy_keywords_found.items():
                all_strategy_kw[kw] = all_strategy_kw.get(kw, 0) + cnt
        all_comp_metrics = sorted({
            m for s in all_scores for m in s.comp_metrics_found
        })
        all_board_indicators = sorted({
            i for s in all_scores for i in s.board_indicators
        })
        all_gov_indicators = sorted({
            i for s in all_scores for i in s.governance_indicators
        })
        all_culture_indicators = sorted({
            i for s in all_scores for i in s.culture_indicators
        })

        # ── Update summary ──
        logger.info("-" * 40)
        logger.info("📊 Updating company signal summary...")
        self.signal_repo.upsert_summary(
            company_id=company_id,
            ticker=ticker,
            leadership_score=round(weighted_total, 2),
        )

        # ── Final report ──
        logger.info("=" * 60)
        logger.info(f"📊 LEADERSHIP ANALYSIS COMPLETE FOR: {ticker}")
        logger.info(f"   Filings analyzed:  {n}")
        logger.info(f"   Signals created:   {signals_created}")
        logger.info(f"   Weighted Score:    {weighted_total:.1f}/100")
        logger.info(f"   Confidence:        {avg_confidence:.3f}")
        logger.info(f"   ── CS3 Sub-scores ──")
        logger.info(f"   Leadership sub:    {weighted_leadership_sub:.1f}/60")
        logger.info(f"   Governance sub:    {weighted_governance_sub:.1f}/30")
        logger.info(f"   Culture sub:       {weighted_culture_sub:.1f}/10")
        logger.info(f"   Tech Execs:        {all_tech_execs}")
        logger.info("=" * 60)

        return {
            "ticker": ticker,
            "company_id": company_id,
            "company_name": company_name,
            "filing_count_analyzed": n,
            "signals_created": signals_created,
            "normalized_score": round(weighted_total, 2),
            "confidence": round(avg_confidence, 3),
            "breakdown": {
                "tech_exec_score": round(avg_tech_exec, 1),
                "strategy_keyword_score": round(avg_strategy_kw, 1),
                "comp_metric_score": round(avg_comp_metric, 1),
                "board_tech_score": round(avg_board_tech, 1),
                "governance_score": round(avg_governance, 1),
                "culture_score": round(avg_culture, 1),
                "total_score": round(weighted_total, 1),
            },
            # CS3-ready dimension sub-scores
            "cs3_dimension_subscores": {
                "leadership_sub": round(weighted_leadership_sub, 1),
                "governance_sub": round(weighted_governance_sub, 1),
                "culture_sub": round(weighted_culture_sub, 1),
            },
            # Evidence
            "tech_execs_found": all_tech_execs,
            "strategy_keywords_found": all_strategy_kw,
            "comp_metrics_found": all_comp_metrics,
            "board_tech_indicators": all_board_indicators,
            "governance_indicators": all_gov_indicators,
            "culture_indicators": all_culture_indicators,
            "filing_dates": filing_dates,
        }

    # ──────────────────────────────────────────────────────────────
    # All-company analysis
    # ──────────────────────────────────────────────────────────────
    async def analyze_all_companies(self) -> Dict:
        """Analyze leadership signals for all 10 target companies."""
        tickers = [
            "CAT", "DE", "UNH", "HCA", "ADP",
            "PAYX", "WMT", "TGT", "JPM", "GS",
        ]

        logger.info("=" * 60)
        logger.info("🎯 ANALYZING LEADERSHIP SIGNALS FOR ALL COMPANIES")
        logger.info("=" * 60)

        results = []
        ok, fail = 0, 0

        for ticker in tickers:
            try:
                r = await self.analyze_company(ticker)
                results.append({
                    "ticker": ticker,
                    "status": "success",
                    "score": r["normalized_score"],
                    "confidence": r["confidence"],
                    "filings_analyzed": r["filing_count_analyzed"],
                    "cs3_subs": r["cs3_dimension_subscores"],
                })
                ok += 1
            except Exception as e:
                logger.error(f"❌ Failed to analyze {ticker}: {e}")
                results.append({
                    "ticker": ticker,
                    "status": "failed",
                    "error": str(e),
                })
                fail += 1

        logger.info("=" * 60)
        logger.info("📊 ALL COMPANIES LEADERSHIP ANALYSIS COMPLETE")
        logger.info(f"   Successful: {ok}")
        logger.info(f"   Failed:     {fail}")
        logger.info("=" * 60)

        return {
            "total_companies": len(tickers),
            "successful": ok,
            "failed": fail,
            "results": results,
        }


# ── Singleton ──
_service: Optional[LeadershipSignalService] = None


def get_leadership_service() -> LeadershipSignalService:
    global _service
    if _service is None:
        _service = LeadershipSignalService()
    return _service