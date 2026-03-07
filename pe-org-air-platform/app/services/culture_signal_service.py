"""
Culture Signal Service — PE Org-AI-R Platform
app/services/culture_signal_service.py

Encapsulates Glassdoor/Indeed/CareerBliss collection, S3 persistence, and
Snowflake upsert so the router only validates input, calls this service, and
formats the response.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, asdict
from decimal import Decimal
from typing import Dict, List, Optional

from app.repositories.scoring_repository import get_scoring_repository
from app.services.s3_storage import get_s3_service
from app.services.utils import make_singleton_factory

logger = logging.getLogger(__name__)


@dataclass
class CultureCollectResult:
    """Structured result returned by CultureSignalService.collect()."""
    ticker: str
    signal_dict: dict          # float-converted signal fields, for response formatting
    raw_reviews: List[dict]    # loaded from S3 raw after collection
    source_counts: Dict[str, int]
    raw_s3_key: Optional[str]
    output_s3_key: Optional[str]
    snowflake_ok: bool


class CultureSignalService:
    """Service layer for culture-signal collection and retrieval."""

    def __init__(self):
        self.s3 = get_s3_service()
        self.scoring_repo = get_scoring_repository()
        # CultureCollector is NOT stored as an instance attribute — it manages a
        # Playwright browser lifecycle and must be created fresh per collect() call.

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def collect(self, ticker: str) -> CultureCollectResult:
        """
        Run the full CultureCollector pipeline for *ticker*:
          1. Scrape reviews from Glassdoor, Indeed, and CareerBliss
          2. Produce a CultureSignal with scored dimensions
          3. Upsert the result into Snowflake signal_dimension_mapping
          4. Load the raw and output files just written to S3
          5. Return a CultureCollectResult with all data needed by the router
        """
        from app.pipelines.glassdoor_collector import CultureCollector

        ticker = ticker.upper()

        def _run_collection():
            """Run blocking Playwright-based collection in a thread (outside asyncio loop)."""
            collector = CultureCollector()
            try:
                return collector.collect_and_analyze(
                    ticker=ticker,
                    sources=["glassdoor", "indeed", "careerbliss"],
                    use_cache=True,
                )
            finally:
                collector.close_browser()

        signal = await asyncio.to_thread(_run_collection)

        # Convert Decimal values to float for JSON serialisation
        signal_dict: dict = {}
        for k, v in asdict(signal).items():
            signal_dict[k] = float(v) if isinstance(v, Decimal) else v

        # Upsert to Snowflake (non-fatal)
        sf_ok = self.scoring_repo.upsert_culture_mapping(ticker, signal_dict)

        # Load the raw reviews that were just uploaded to S3
        raw_data, raw_key = self._load_s3_json(f"glassdoor_signals/raw/{ticker}/")
        raw_reviews: List[dict] = []
        source_counts: Dict[str, int] = {}
        if raw_data and "reviews" in raw_data:
            for r in raw_data["reviews"]:
                raw_reviews.append(r)
                src = r.get("source", "unknown")
                source_counts[src] = source_counts.get(src, 0) + 1

        # Find the output S3 key written during collection
        _, output_key = self._load_s3_json(
            f"glassdoor_signals/output/{ticker}/",
            flat_fallback=f"glassdoor_signals/output/{ticker}_culture.json",
        )

        return CultureCollectResult(
            ticker=ticker,
            signal_dict=signal_dict,
            raw_reviews=raw_reviews,
            source_counts=source_counts,
            raw_s3_key=raw_key,
            output_s3_key=output_key,
            snowflake_ok=sf_ok,
        )

    def get(self, ticker: str) -> tuple[Optional[dict], Optional[str]]:
        """
        Return the latest stored culture-signal output from S3 for *ticker*,
        or (None, None) if no data is found.
        """
        return self._load_s3_json(
            f"glassdoor_signals/output/{ticker.upper()}/",
            flat_fallback=f"glassdoor_signals/output/{ticker.upper()}_culture.json",
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load_s3_json(
        self,
        prefix: str,
        flat_fallback: Optional[str] = None,
    ) -> tuple[Optional[dict], Optional[str]]:
        """
        Load the latest JSON object from an S3 prefix (sorted by key name).

        Falls back to *flat_fallback* key when the prefix has no objects.
        Returns (data_dict, s3_key) or (None, None).
        """
        # Attempt 1: timestamped subfolder (pick latest by lexicographic sort)
        try:
            keys = self.s3.list_files(prefix)
            if keys:
                latest_key = sorted(keys)[-1]
                raw = self.s3.get_file(latest_key)
                if raw is not None:
                    decoded = raw if isinstance(raw, str) else raw.decode("utf-8")
                    return json.loads(decoded), latest_key
        except Exception as exc:
            logger.warning("S3 list/get failed for prefix '%s': %s", prefix, exc)

        # Attempt 2: flat fallback key
        if flat_fallback:
            try:
                raw = self.s3.get_file(flat_fallback)
                if raw is not None:
                    decoded = raw if isinstance(raw, str) else raw.decode("utf-8")
                    return json.loads(decoded), flat_fallback
            except Exception as exc:
                logger.warning("S3 get failed for key '%s': %s", flat_fallback, exc)

        return None, None


get_culture_signal_service = make_singleton_factory(CultureSignalService)
