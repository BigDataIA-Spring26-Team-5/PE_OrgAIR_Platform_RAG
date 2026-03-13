"""
Board Governance Service — PE Org-AI-R Platform
app/services/board_governance_service.py

HYBRID APPROACH:
- LLM: Learns patterns and finds director names
- REGEX: Determines independence (more reliable than LLM)

This prevents LLM hallucination on independence while still getting better director discovery.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from app.pipelines.board_analyzer import (
    BoardCompositionAnalyzer,
    GovernanceSignal,
    _signal_to_dict,
)
from app.repositories.company_repository import CompanyRepository
from app.repositories.document_repository import get_document_repository
from app.repositories.signal_repository import get_signal_repository
from app.services.s3_storage import get_s3_service
from app.services.utils import make_singleton_factory
from app.services.llm.router import get_llm_router

logger = logging.getLogger(__name__)


class BoardGovernanceService:
    """Service layer for board governance analysis and persistence."""

    def __init__(self):
        self.s3 = get_s3_service()
        self.doc_repo = get_document_repository()
        self.signal_repo = get_signal_repository()
        self.company_repo = CompanyRepository()
        self.llm_router = get_llm_router()
        self._analyzer = BoardCompositionAnalyzer(s3=self.s3, doc_repo=self.doc_repo)
        
        # Cache for learned patterns per ticker
        self._pattern_cache: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # LLM-Powered Pattern Learning (for director discovery only)
    # ------------------------------------------------------------------

    def _learn_governance_patterns(
        self, 
        ticker: str, 
        proxy_text: str
    ) -> Dict[str, Any]:
        """
        Use LLM to analyze proxy statement structure and suggest section headers.
        Returns dict with extraction patterns.
        """
        # Truncate proxy text for LLM analysis
        sample_text = proxy_text[:8000] if len(proxy_text) > 8000 else proxy_text
        
        prompt = f"""Analyze this DEF 14A proxy statement to find board composition section.

Company: {ticker}

Proxy Statement Sample:
---
{sample_text}
---

Return JSON with section headers that introduce board composition:

{{
  "board_section_headers": ["BOARD OF DIRECTORS", "PROPOSAL 1", "DIRECTOR NOMINEES"]
}}

Respond ONLY with valid JSON, no markdown."""

        try:
            response = self.llm_router.complete_sync(
                task="governance_pattern_extraction",
                messages=[{"role": "user", "content": prompt}],
            )
            
            # Parse JSON response
            response_text = response.strip()
            if response_text.startswith("```"):
                response_text = response_text.split("```")[1]
                if response_text.startswith("json"):
                    response_text = response_text[4:]
            response_text = response_text.strip()
            
            patterns = json.loads(response_text)
            
            logger.info(f"  🤖 LLM learned section patterns for {ticker}")
            
            return patterns
            
        except Exception as e:
            logger.warning(f"  ⚠️ LLM pattern learning failed for {ticker}: {e}")
            return self._get_default_patterns()

    @staticmethod
    def _get_default_patterns() -> Dict[str, Any]:
        """Fallback patterns if LLM learning fails."""
        return {
            "board_section_headers": [
                "BOARD OF DIRECTORS",
                "PROPOSAL 1",
                "ELECTION OF DIRECTORS",
                "DIRECTOR NOMINEES"
            ]
        }

    def _extract_governance_context(
        self, 
        ticker: str, 
        proxy_text: str,
        patterns: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Use LLM to find director NAMES only.
        DO NOT use LLM for independence - regex handles that better.
        
        Returns dict with:
        - directors: List[Dict] with name and role
        - board_size: int
        - use_llm_independence: False (flag to use regex instead)
        """
        # Find board section using learned patterns
        board_section = self._locate_board_section(proxy_text, patterns)
        
        if not board_section or len(board_section) < 200:
            logger.warning(f"  ⚠️ Could not locate board section for {ticker}")
            return {"directors": [], "board_size": 0, "use_llm_independence": False}
        
        # Truncate to reasonable size for LLM
        section_sample = board_section[:6000] if len(board_section) > 6000 else board_section
        
        # SIMPLIFIED PROMPT - Only extract names, NOT independence
        prompt = f"""Extract the list of board directors from this proxy statement.

Company: {ticker}

Board Section:
---
{section_sample}
---

Return JSON with director names and roles:

{{
  "directors": [
    {{"name": "Tim Cook", "title_or_role": "CEO, Apple"}},
    {{"name": "Art Levinson", "title_or_role": "Board Chair"}},
    {{"name": "Jane Smith", "title_or_role": "Former CEO, IBM"}}
  ]
}}

Extract ALL directors. Respond ONLY with valid JSON, no markdown."""

        try:
            response = self.llm_router.complete_sync(
                task="governance_extraction",
                messages=[{"role": "user", "content": prompt}],
            )
            
            # Parse JSON response
            response_text = response.strip()
            if response_text.startswith("```"):
                response_text = response_text.split("```")[1]
                if response_text.startswith("json"):
                    response_text = response_text[4:]
            response_text = response_text.strip()
            
            governance_data = json.loads(response_text)
            directors = governance_data.get('directors', [])
            
            # DO NOT use LLM independence - let regex handle it
            result = {
                "directors": directors,
                "board_size": len(directors),
                "use_llm_independence": False  # Flag: don't use LLM independence data
            }
            
            logger.info(f"  📊 LLM found {len(directors)} directors for {ticker}")
            
            return result
            
        except Exception as e:
            logger.error(f"  ❌ LLM extraction failed for {ticker}: {e}")
            return {"directors": [], "board_size": 0, "use_llm_independence": False}

    @staticmethod
    def _locate_board_section(text: str, patterns: Dict[str, Any]) -> Optional[str]:
        """Find the board composition section using learned header patterns."""
        text_upper = text.upper()
        headers = patterns.get("board_section_headers", [])
        
        for header in headers:
            idx = text_upper.find(header.upper())
            if idx != -1:
                return text[idx:idx + 10000]
        
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyze(
        self, 
        ticker: str,
        use_llm_patterns: bool = True
    ) -> tuple[GovernanceSignal, dict, Optional[str]]:
        """
        Run board-composition analysis for ticker.
        
        Uses HYBRID approach:
        - LLM learns patterns and finds directors
        - Regex determines independence (more reliable)
        """
        ticker = ticker.upper()
        company_id = self._resolve_company_id(ticker)

        # Learn patterns with LLM for new companies
        if ticker not in self._pattern_cache and use_llm_patterns:
            logger.info(f"  🔍 Learning governance patterns for new company: {ticker}")
            
            try:
                proxy_docs = self.doc_repo.get_documents_by_ticker_and_type(
                    ticker=ticker, 
                    doc_type="DEF 14A"
                )
                if proxy_docs:
                    latest_proxy = max(proxy_docs, key=lambda d: d.get("filing_date", ""))
                    proxy_text = latest_proxy.get("content", "")
                    
                    # Learn patterns
                    patterns = self._learn_governance_patterns(ticker, proxy_text)
                    self._pattern_cache[ticker] = patterns
                    
                    # Extract director names (NOT independence)
                    governance_context = self._extract_governance_context(
                        ticker, 
                        proxy_text, 
                        patterns
                    )
                    
                    # Pass to analyzer - it will use regex for independence
                    self._analyzer.set_extraction_context(governance_context)
                else:
                    logger.warning(f"  ⚠️ No proxy docs found for {ticker}")
            except Exception as e:
                logger.warning(f"  ⚠️ Could not learn patterns for {ticker}: {e}")

        # Run analysis (uses regex for independence)
        signal = self._analyzer.scrape_and_analyze(ticker=ticker, company_id=company_id)
        trail = self._analyzer.get_last_evidence_trail()

        # Build and upload S3 payload
        s3_key: Optional[str] = None
        try:
            payload = _signal_to_dict(signal)
            payload["_meta"] = {
                "signal_type": "board_composition",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source": "CS3 Task 5.0d + LLM director discovery",
                "score_breakdown": trail or {},
                "used_llm_patterns": ticker in self._pattern_cache,
            }
            s3_key = self.s3.store_signal_data(
                signal_type="board_composition",
                ticker=ticker,
                data=payload,
            )
        except Exception as exc:
            logger.warning("[%s] S3 save failed: %s", ticker, exc)

        # Persist to Snowflake
        try:
            normalized_score = float(signal.governance_score)
            self.signal_repo.create_signal(
                company_id=company_id,
                category="board_governance",
                source="sec_edgar_proxy",
                signal_date=datetime.now(timezone.utc),
                raw_value=str(normalized_score),
                normalized_score=normalized_score,
                confidence=float(signal.confidence),
                metadata={"ticker": ticker, "llm_enhanced": ticker in self._pattern_cache},
            )
            self.signal_repo.upsert_summary(
                company_id=company_id,
                ticker=ticker,
                leadership_score=normalized_score,
            )
        except Exception as exc:
            logger.warning("[%s] Snowflake persist failed: %s", ticker, exc)

        return signal, trail, s3_key

    def get(self, ticker: str) -> tuple[Optional[dict], Optional[str]]:
        """Return the latest board-governance payload from S3."""
        ticker = ticker.upper()
        try:
            keys = self.s3.list_files(f"signals/board_composition/{ticker}/")
            if keys:
                latest_key = sorted(keys)[-1]
                data = self.s3.get_file(latest_key)
                if data:
                    decoded = data if isinstance(data, str) else data.decode("utf-8")
                    return json.loads(decoded), latest_key
        except Exception as exc:
            logger.warning("[%s] S3 read failed: %s", ticker, exc)
        return None, None

    def _resolve_company_id(self, ticker: str) -> str:
        try:
            company = self.company_repo.get_by_ticker(ticker)
            if company:
                return company["id"]
        except Exception:
            pass
        return ticker


get_board_governance_service = make_singleton_factory(BoardGovernanceService)