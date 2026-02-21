"""
Scoring Repository — CS3 Snowflake persistence
app/repositories/scoring_repository.py

Tables:
  - signal_dimension_mapping   (CS3 Table 1 matrix view per ticker)
  - evidence_dimension_scores  (7 aggregated dimension scores per ticker)

Follows existing repo pattern: singleton, get_snowflake_connection(), cursor-based.
"""

import logging
from typing import Dict, List, Optional
from uuid import uuid4
from app.services.snowflake import get_snowflake_connection

logger = logging.getLogger(__name__)


class ScoringRepository:
    """Repository for CS3 scoring tables in Snowflake."""

    def __init__(self):
        self.conn = get_snowflake_connection()

    # =====================================================================
    # signal_dimension_mapping — the mapping matrix (Table 1) per ticker
    # =====================================================================

    def upsert_mapping_row(
        self,
        ticker: str,
        source: str,
        raw_score: Optional[float],
        confidence: Optional[float],
        evidence_count: int,
        data_infrastructure: Optional[float],
        ai_governance: Optional[float],
        technology_stack: Optional[float],
        talent_skills: Optional[float],
        leadership_vision: Optional[float],
        use_case_portfolio: Optional[float],
        culture_change: Optional[float],
    ) -> str:
        """Upsert one row into signal_dimension_mapping (MERGE by ticker+source)."""
        row_id = str(uuid4())
        sql = """
        MERGE INTO signal_dimension_mapping t
        USING (SELECT %s AS ticker, %s AS source) s
        ON t.ticker = s.ticker AND t.source = s.source
        WHEN MATCHED THEN UPDATE SET
            raw_score = %s,
            confidence = %s,
            evidence_count = %s,
            data_infrastructure = %s,
            ai_governance = %s,
            technology_stack = %s,
            talent_skills = %s,
            leadership_vision = %s,
            use_case_portfolio = %s,
            culture_change = %s,
            created_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            id, ticker, source, raw_score, confidence, evidence_count,
            data_infrastructure, ai_governance, technology_stack,
            talent_skills, leadership_vision, use_case_portfolio, culture_change,
            created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            CURRENT_TIMESTAMP()
        )
        """
        params = (
            ticker.upper(), source,
            # UPDATE values
            raw_score, confidence, evidence_count,
            data_infrastructure, ai_governance, technology_stack,
            talent_skills, leadership_vision, use_case_portfolio, culture_change,
            # INSERT values
            row_id, ticker.upper(), source, raw_score, confidence, evidence_count,
            data_infrastructure, ai_governance, technology_stack,
            talent_skills, leadership_vision, use_case_portfolio, culture_change,
        )
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            self.conn.commit()
            return row_id
        except Exception as e:
            logger.error(f"Failed to upsert mapping row: {e}")
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def upsert_mapping_matrix(self, rows: List[Dict]) -> int:
        """
        Upsert the full mapping matrix for a ticker.

        Args:
            rows: Output of EvidenceMapper.build_mapping_matrix()

        Returns:
            Number of rows upserted
        """
        count = 0
        for row in rows:
            self.upsert_mapping_row(
                ticker=row["ticker"],
                source=row["source"],
                raw_score=row.get("raw_score"),
                confidence=row.get("confidence"),
                evidence_count=row.get("evidence_count", 0),
                data_infrastructure=row.get("data_infrastructure"),
                ai_governance=row.get("ai_governance"),
                technology_stack=row.get("technology_stack"),
                talent_skills=row.get("talent_skills"),
                leadership_vision=row.get("leadership_vision"),
                use_case_portfolio=row.get("use_case_portfolio"),
                culture_change=row.get("culture_change"),
            )
            count += 1
        logger.info(f"Upserted {count} mapping rows for {rows[0]['ticker'] if rows else '?'}")
        return count

    def get_mapping_matrix(self, ticker: str) -> List[Dict]:
        """Get the full mapping matrix for a ticker (Table 1 view)."""
        sql = """
        SELECT ticker, source, raw_score, confidence, evidence_count,
               data_infrastructure, ai_governance, technology_stack,
               talent_skills, leadership_vision, use_case_portfolio, culture_change,
               created_at
        FROM signal_dimension_mapping
        WHERE ticker = %s
        ORDER BY CASE source
            WHEN 'technology_hiring' THEN 1
            WHEN 'innovation_activity' THEN 2
            WHEN 'digital_presence' THEN 3
            WHEN 'leadership_signals' THEN 4
            WHEN 'sec_item_1' THEN 5
            WHEN 'sec_item_1a' THEN 6
            WHEN 'sec_item_7' THEN 7
            WHEN 'glassdoor_reviews' THEN 8
            WHEN 'board_composition' THEN 9
            ELSE 10
        END
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker.upper(),))
            columns = [col[0].lower() for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def get_all_mapping_matrices(self) -> List[Dict]:
        """Get mapping matrices for all tickers."""
        sql = """
        SELECT ticker, source, raw_score, confidence, evidence_count,
               data_infrastructure, ai_governance, technology_stack,
               talent_skills, leadership_vision, use_case_portfolio, culture_change
        FROM signal_dimension_mapping
        ORDER BY ticker, CASE source
            WHEN 'technology_hiring' THEN 1
            WHEN 'innovation_activity' THEN 2
            WHEN 'digital_presence' THEN 3
            WHEN 'leadership_signals' THEN 4
            WHEN 'sec_item_1' THEN 5
            WHEN 'sec_item_1a' THEN 6
            WHEN 'sec_item_7' THEN 7
            WHEN 'glassdoor_reviews' THEN 8
            WHEN 'board_composition' THEN 9
            ELSE 10
        END
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            columns = [col[0].lower() for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def delete_mapping_matrix(self, ticker: str) -> int:
        """Delete all mapping rows for a ticker."""
        sql = "DELETE FROM signal_dimension_mapping WHERE ticker = %s"
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker.upper(),))
            self.conn.commit()
            return cur.rowcount
        finally:
            cur.close()

    # =====================================================================
    # evidence_dimension_scores — 7 aggregated dimension scores per ticker
    # =====================================================================

    def upsert_dimension_score(
        self,
        ticker: str,
        dimension: str,
        score: float,
        confidence: float,
        source_count: int,
        sources: str,
        total_weight: float,
    ) -> str:
        """Upsert one dimension score (MERGE by ticker+dimension)."""
        row_id = str(uuid4())
        sql = """
        MERGE INTO evidence_dimension_scores t
        USING (SELECT %s AS ticker, %s AS dimension) s
        ON t.ticker = s.ticker AND t.dimension = s.dimension
        WHEN MATCHED THEN UPDATE SET
            score = %s,
            confidence = %s,
            source_count = %s,
            sources = %s,
            total_weight = %s,
            created_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            id, ticker, dimension, score, confidence, source_count, sources, total_weight, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP()
        )
        """
        params = (
            ticker.upper(), dimension,
            # UPDATE
            score, confidence, source_count, sources, total_weight,
            # INSERT
            row_id, ticker.upper(), dimension, score, confidence, source_count, sources, total_weight,
        )
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            self.conn.commit()
            return row_id
        except Exception as e:
            logger.error(f"Failed to upsert dimension score: {e}")
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def upsert_dimension_scores(self, rows: List[Dict]) -> int:
        """
        Upsert all 7 dimension scores for a ticker.

        Args:
            rows: Output of EvidenceMapper.build_dimension_summary()

        Returns:
            Number of rows upserted
        """
        count = 0
        for row in rows:
            self.upsert_dimension_score(
                ticker=row["ticker"],
                dimension=row["dimension"],
                score=row["score"],
                confidence=row["confidence"],
                source_count=row["source_count"],
                sources=row["sources"],
                total_weight=row["total_weight"],
            )
            count += 1
        logger.info(f"Upserted {count} dimension scores for {rows[0]['ticker'] if rows else '?'}")
        return count

    def get_dimension_scores(self, ticker: str) -> List[Dict]:
        """Get all 7 dimension scores for a ticker."""
        sql = """
        SELECT ticker, dimension, score, confidence, source_count, sources, total_weight, created_at
        FROM evidence_dimension_scores
        WHERE ticker = %s
        ORDER BY CASE dimension
            WHEN 'data_infrastructure' THEN 1
            WHEN 'ai_governance' THEN 2
            WHEN 'technology_stack' THEN 3
            WHEN 'talent_skills' THEN 4
            WHEN 'leadership_vision' THEN 5
            WHEN 'use_case_portfolio' THEN 6
            WHEN 'culture_change' THEN 7
            ELSE 8
        END
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker.upper(),))
            columns = [col[0].lower() for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def get_all_dimension_scores(self) -> List[Dict]:
        """Get dimension scores for all tickers."""
        sql = """
        SELECT ticker, dimension, score, confidence, source_count, sources, total_weight
        FROM evidence_dimension_scores
        ORDER BY ticker, CASE dimension
            WHEN 'data_infrastructure' THEN 1
            WHEN 'ai_governance' THEN 2
            WHEN 'technology_stack' THEN 3
            WHEN 'talent_skills' THEN 4
            WHEN 'leadership_vision' THEN 5
            WHEN 'use_case_portfolio' THEN 6
            WHEN 'culture_change' THEN 7
            ELSE 8
        END
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            columns = [col[0].lower() for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def delete_dimension_scores(self, ticker: str) -> int:
        """Delete dimension scores for a ticker."""
        sql = "DELETE FROM evidence_dimension_scores WHERE ticker = %s"
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker.upper(),))
            self.conn.commit()
            return cur.rowcount
        finally:
            cur.close()


# Singleton
_repo: Optional[ScoringRepository] = None

def get_scoring_repository() -> ScoringRepository:
    global _repo
    if _repo is None:
        _repo = ScoringRepository()
    return _repo