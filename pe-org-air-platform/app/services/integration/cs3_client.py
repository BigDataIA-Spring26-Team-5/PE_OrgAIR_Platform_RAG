"""CS3 Client — Scores and rubric data from the PE Org-AI-R platform."""
from __future__ import annotations

import httpx
from dataclasses import dataclass, field
from typing import List, Optional, Dict

DIMENSIONS = [
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent",
    "leadership",
    "use_case_portfolio",
    "culture",
]

SCORE_LEVELS = {
    1: ("Nascent", 0, 19),
    2: ("Developing", 20, 39),
    3: ("Adequate", 40, 59),
    4: ("Good", 60, 79),
    5: ("Excellent", 80, 100),
}


def score_to_level(score: float) -> tuple[int, str]:
    """Convert numeric score to (level_int, level_name)."""
    for level, (name, lo, hi) in SCORE_LEVELS.items():
        if lo <= score <= hi:
            return level, name
    return 5, "Excellent"


@dataclass
class DimensionScore:
    dimension: str
    score: float
    level: int
    level_name: str
    confidence_interval: tuple[float, float] = (0.0, 0.0)
    rubric_keywords: List[str] = field(default_factory=list)


@dataclass
class CompanyAssessment:
    company_id: str
    ticker: str
    dimension_scores: Dict[str, DimensionScore] = field(default_factory=dict)
    talent_concentration: float = 0.0
    valuation_risk: float = 0.0
    position_factor: float = 0.0
    human_capital_risk: float = 0.0
    synergy: float = 0.0
    org_air_score: float = 0.0
    assessment_id: Optional[str] = None


@dataclass
class RubricCriteria:
    dimension: str
    level: int
    level_name: str
    criteria: str
    keywords: List[str] = field(default_factory=list)


class CS3Client:
    """Fetches scoring and rubric data from CS3 API endpoints."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/") + "/api/v1"
        self._client = httpx.Client(timeout=30.0)

    def get_assessment(self, company_id: str) -> Optional[CompanyAssessment]:
        """Fetch the full composite assessment for a company (company_id is ticker)."""
        resp = self._client.get(f"{self.base_url}/scoring/{company_id}/dimensions")
        if resp.status_code == 200:
            return self._parse_assessment(resp.json(), company_id)
        return None

    def get_dimension_score(
        self, company_id: str, dimension: str
    ) -> Optional[DimensionScore]:
        """Fetch score for one specific dimension (company_id is ticker)."""
        assessment = self.get_assessment(company_id)
        if not assessment:
            return None
        scores = assessment.dimension_scores
        # Exact match first
        if dimension in scores:
            return scores[dimension]
        # Prefix/substring match (e.g. "talent" matches "talent_skills")
        dim_lower = dimension.lower().replace("_management", "").replace("_", "")
        for key, val in scores.items():
            if key.startswith(dimension) or dim_lower in key.replace("_", ""):
                return val
        return None

    def get_rubric(
        self, dimension: str, level: Optional[int] = None
    ) -> List[RubricCriteria]:
        """Fetch rubric criteria for a dimension (optionally filtered by level)."""
        params: dict = {"dimension": dimension}
        if level is not None:
            params["level"] = level
        resp = self._client.get(f"{self.base_url}/scoring/rubrics", params=params)
        if resp.status_code == 200:
            data = resp.json()
            items = data if isinstance(data, list) else data.get("rubrics", [])
            return [self._parse_rubric(r) for r in items]
        # Return default rubric from hardcoded data
        return self._default_rubric(dimension, level)

    def _parse_assessment(self, data: dict, company_id: str) -> CompanyAssessment:
        dim_scores: Dict[str, DimensionScore] = {}
        # /scoring/{ticker}/dimensions returns {"ticker": ..., "scores": [...]}
        raw_dims = data.get("scores", data.get("dimension_scores", data.get("dimensions", {})))
        if isinstance(raw_dims, list):
            for d in raw_dims:
                dim = d.get("dimension", d.get("dimension_name", ""))
                if dim:
                    dim_scores[dim] = self._parse_dimension_score(dim, d)
        elif isinstance(raw_dims, dict):
            for dim, val in raw_dims.items():
                if isinstance(val, (int, float)):
                    level, name = score_to_level(val)
                    dim_scores[dim] = DimensionScore(
                        dimension=dim, score=val, level=level, level_name=name
                    )
                elif isinstance(val, dict):
                    dim_scores[dim] = self._parse_dimension_score(dim, val)
        return CompanyAssessment(
            company_id=company_id,
            ticker=data.get("ticker", company_id),
            dimension_scores=dim_scores,
            talent_concentration=float(data.get("talent_concentration", 0.0)),
            valuation_risk=float(data.get("valuation_risk", 0.0)),
            position_factor=float(data.get("position_factor", 0.0)),
            human_capital_risk=float(data.get("human_capital_risk", 0.0)),
            synergy=float(data.get("synergy", 0.0)),
            org_air_score=float(data.get("org_air_score", data.get("orgair_score", 0.0))),
            assessment_id=str(data.get("assessment_id", data.get("id", ""))),
        )

    @staticmethod
    def _parse_dimension_score(dimension: str, data: dict) -> DimensionScore:
        score = float(data.get("score", data.get("value", 0.0)))
        level, level_name = score_to_level(score)
        ci = data.get("confidence_interval", [0.0, 0.0])
        if isinstance(ci, dict):
            ci = [ci.get("lower", 0.0), ci.get("upper", 0.0)]
        return DimensionScore(
            dimension=dimension,
            score=score,
            level=level,
            level_name=level_name,
            confidence_interval=tuple(ci[:2]) if len(ci) >= 2 else (0.0, 0.0),
            rubric_keywords=data.get("rubric_keywords", []),
        )

    @staticmethod
    def _parse_rubric(data: dict) -> RubricCriteria:
        level = int(data.get("level", 3))
        _, level_name = score_to_level(level * 20)
        return RubricCriteria(
            dimension=data.get("dimension", ""),
            level=level,
            level_name=data.get("level_name", level_name),
            criteria=data.get("criteria", data.get("description", "")),
            keywords=data.get("keywords", []),
        )

    @staticmethod
    def _default_rubric(dimension: str, level: Optional[int]) -> List[RubricCriteria]:
        """Minimal fallback rubric when API is unavailable."""
        _rubrics = {
            "data_infrastructure": {
                1: ("Basic data storage, no cloud architecture", ["storage", "database"]),
                2: ("Cloud data warehouse, basic pipelines", ["warehouse", "pipeline", "cloud"]),
                3: ("Modern data stack, real-time ingestion", ["streaming", "lakehouse", "ETL"]),
                4: ("AI-ready platform, feature store, MLOps", ["feature store", "MLflow", "Airflow"]),
                5: ("Unified AI data fabric, automated governance", ["data fabric", "automated", "governance"]),
            },
        }
        dim_rubrics = _rubrics.get(dimension, {})
        results = []
        for lvl, (criteria, keywords) in dim_rubrics.items():
            if level is None or lvl == level:
                _, level_name = score_to_level(lvl * 20)
                results.append(
                    RubricCriteria(
                        dimension=dimension,
                        level=lvl,
                        level_name=level_name,
                        criteria=criteria,
                        keywords=keywords,
                    )
                )
        return results

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
