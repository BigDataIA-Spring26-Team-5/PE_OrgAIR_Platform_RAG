"""
Scoring Read Repository - PE Org-AI-R Platform
app/repositories/scoring_read_repository.py

Read-only access to the SCORING table. Consolidates the near-identical
_fetch_*_row() helpers that were duplicated across 4 scoring routers.
"""
from typing import Dict, List, Optional

from app.services.utils import make_singleton_factory


class ScoringReadRepository:
    """Fetch rows from the Snowflake SCORING table."""

    def _query(self, ticker: str, columns: List[str]) -> Optional[Dict]:
        """Execute SELECT <columns> FROM SCORING WHERE ticker = %s."""
        from app.services.snowflake import get_snowflake_connection
        from snowflake.connector import DictCursor

        cols = ", ".join(columns)
        conn = get_snowflake_connection()
        try:
            cursor = conn.cursor(DictCursor)
            cursor.execute(
                f"SELECT {cols} FROM SCORING WHERE ticker = %s",
                [ticker.upper()],
            )
            row = cursor.fetchone()
            cursor.close()
            return row or None
        finally:
            conn.close()

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
