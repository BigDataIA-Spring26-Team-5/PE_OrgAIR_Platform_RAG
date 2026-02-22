"""
Board Governance Service — PE Org-AI-R Platform
app/services/board_governance_service.py

Encapsulates all board-governance analysis logic so the router only
validates input, calls this service, and formats the response.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

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

logger = logging.getLogger(__name__)


class BoardGovernanceService:
    """Service layer for board governance analysis and persistence."""

    def __init__(self):
        self.s3 = get_s3_service()
        self.doc_repo = get_document_repository()
        self.signal_repo = get_signal_repository()
        self.company_repo = CompanyRepository()
        self._analyzer = BoardCompositionAnalyzer(s3=self.s3, doc_repo=self.doc_repo)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyze(self, ticker: str) -> tuple[GovernanceSignal, dict, Optional[str]]:
        """
        Run board-composition analysis for *ticker*, persist results to S3 and
        Snowflake, and return (signal, evidence_trail, s3_key).
        """
        ticker = ticker.upper()
        company_id = self._resolve_company_id(ticker)

        signal = self._analyzer.scrape_and_analyze(ticker=ticker, company_id=company_id)
        trail = self._analyzer.get_last_evidence_trail()

        # Build and upload S3 payload
        s3_key: Optional[str] = None
        try:
            payload = _signal_to_dict(signal)
            payload["_meta"] = {
                "signal_type": "board_composition",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source": "CS3 Task 5.0d",
                "score_breakdown": trail or {},
            }
            s3_key = self.s3.store_signal_data(
                signal_type="board_composition",
                ticker=ticker,
                data=payload,
            )
        except Exception as exc:
            logger.warning("[%s] S3 save failed: %s", ticker, exc)

        # Persist to Snowflake (non-fatal)
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
                metadata={"ticker": ticker},
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
        """
        Return the latest stored board-governance payload from S3 for *ticker*,
        or (None, None) if no data is found.
        """
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

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_company_id(self, ticker: str) -> str:
        try:
            company = self.company_repo.get_by_ticker(ticker)
            if company:
                return company["id"]
        except Exception:
            pass
        return ticker


get_board_governance_service = make_singleton_factory(BoardGovernanceService)
