"""
Scoring Read Repository - PE Org-AI-R Platform
app/repositories/scoring_read_repository.py

Read-only access to the SCORING table. Consolidates the near-identical
_fetch_*_row() helpers that were duplicated across 4 scoring routers.
"""
from typing import Dict, List, Optional

from app.repositories.base import BaseRepository
from app.services.utils import make_singleton_factory


class ScoringReadRepository(BaseRepository):
    """Fetch rows from the Snowflake SCORING table."""

    def _query(self, ticker: str, columns: List[str]) -> Optional[Dict]:
        """Execute SELECT <columns> FROM SCORING WHERE ticker = %s."""
        from snowflake.connector import DictCursor

        cols = ", ".join(columns)
        with self.get_connection() as conn:
            cursor = conn.cursor(DictCursor)
            cursor.execute(
                f"SELECT {cols} FROM SCORING WHERE ticker = %s",
                [ticker.upper()],
            )
            row = cursor.fetchone()
            cursor.close()
            return row or None

    def fetch_tc_vr_row(self, ticker: str) -> Optional[Dict]:
        """Fetch TC, VR, PF, HR columns for one ticker."""
        return self._query(ticker, ["ticker", "tc", "vr", "pf", "hr", "scored_at", "updated_at"])

    def fetch_pf_row(self, ticker: str) -> Optional[Dict]:
        """Fetch PF column for one ticker."""
        return self._query(ticker, ["ticker", "pf", "scored_at", "updated_at"])

    def fetch_hr_row(self, ticker: str) -> Optional[Dict]:
        """Fetch HR column for one ticker."""
        return self._query(ticker, ["ticker", "hr", "scored_at", "updated_at"])

    def fetch_orgair_row(self, ticker: str) -> Optional[Dict]:
        """Fetch ORG_AIR column for one ticker."""
        return self._query(ticker, ["ticker", "org_air", "scored_at", "updated_at"])


get_scoring_read_repo = make_singleton_factory(ScoringReadRepository)

# Compatibility re-export — CompositeScoringRepository supersedes this class.
from app.repositories.composite_scoring_repository import (  # noqa: F401
    CompositeScoringRepository, get_composite_scoring_repo,
)
