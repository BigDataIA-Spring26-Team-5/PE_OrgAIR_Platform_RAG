"""CS3 Scoring Engine API client."""
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from enum import Enum
import httpx

class Dimension(str, Enum):
    """The 7 V^R dimensions from CS3."""
    DATA_INFRASTRUCTURE = "data_infrastructure"
    AI_GOVERNANCE = "ai_governance"
    TECHNOLOGY_STACK = "technology_stack"
    TALENT = "talent"
    LEADERSHIP = "leadership"
    USE_CASE_PORTFOLIO = "use_case_portfolio"
    CULTURE = "culture"

class ScoreLevel(int, Enum):
    """Score levels with ranges."""
    LEVEL_5 = 5  # 80-100: Excellent
    LEVEL_4 = 4  # 60-79: Good
    LEVEL_3 = 3  # 40-59: Adequate
    LEVEL_2 = 2  # 20-39: Developing
    LEVEL_1 = 1  # 0-19: Nascent

    @property
    def name_label(self) -> str:
        labels = {5: "Excellent", 4: "Good", 3: "Adequate",
                  2: "Developing", 1: "Nascent"}
        return labels[self.value]

    @property
    def score_range(self) -> Tuple[int, int]:
        ranges = {5: (80, 100), 4: (60, 79), 3: (40, 59),
                  2: (20, 39), 1: (0, 19)}
        return ranges[self.value]

@dataclass
class DimensionScore:
    """Single dimension score from CS3."""
    dimension: Dimension
    score: float
    level: ScoreLevel
    confidence_interval: Tuple[float, float]  # (lower, upper)
    evidence_count: int
    last_updated: str

@dataclass
class RubricCriteria:
    """Rubric criteria for a dimension level."""
    dimension: Dimension
    level: ScoreLevel
    criteria_text: str
    keywords: List[str]
    quantitative_thresholds: Dict[str, float]

@dataclass
class CompanyAssessment:
    """Full company assessment from CS3."""
    company_id: str
    assessment_date: str

    # Composite scores
    vr_score: float
    hr_score: float
    synergy_score: float
    org_air_score: float

    # Confidence interval
    confidence_interval: Tuple[float, float]

    # Component scores
    dimension_scores: Dict[Dimension, DimensionScore]

    # Risk adjustments
    talent_concentration: float
    position_factor: float

class CS3Client:
    """Client for CS3 Scoring Engine API."""

    def __init__(self, base_url: str = "http://localhost:8002"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=30.0)

    async def get_assessment(self, company_id: str) -> CompanyAssessment:
        """Fetch complete assessment for a company."""
        response = await self.client.get(
            f"{self.base_url}/api/v1/assessments/{company_id}"
        )
        response.raise_for_status()
        data = response.json()

        dim_scores = {}
        for dim_name, score_data in data["dimension_scores"].items():
            dim = Dimension(dim_name)
            dim_scores[dim] = DimensionScore(
                dimension=dim,
                score=score_data["score"],
                level=ScoreLevel(score_data["level"]),
                confidence_interval=tuple(score_data["confidence_interval"]),
                evidence_count=score_data["evidence_count"],
                last_updated=score_data["last_updated"],
            )

        return CompanyAssessment(
            company_id=data["company_id"],
            assessment_date=data["assessment_date"],
            vr_score=data["vr_score"],
            hr_score=data["hr_score"],
            synergy_score=data["synergy_score"],
            org_air_score=data["org_air_score"],
            confidence_interval=tuple(data["confidence_interval"]),
            dimension_scores=dim_scores,
            talent_concentration=data["talent_concentration"],
            position_factor=data["position_factor"],
        )

    async def get_dimension_score(
        self,
        company_id: str,
        dimension: Dimension
    ) -> DimensionScore:
        """Fetch single dimension score."""
        response = await self.client.get(
            f"{self.base_url}/api/v1/assessments/{company_id}/dimensions/{dimension.value}"
        )
        response.raise_for_status()
        data = response.json()
        return DimensionScore(
            dimension=dimension,
            score=data["score"],
            level=ScoreLevel(data["level"]),
            confidence_interval=tuple(data["confidence_interval"]),
            evidence_count=data["evidence_count"],
            last_updated=data["last_updated"],
        )

    async def get_rubric(
        self,
        dimension: Dimension,
        level: Optional[ScoreLevel] = None
    ) -> List[RubricCriteria]:
        """
        Fetch rubric criteria for a dimension.

        Args:
            dimension: Which dimension
            level: Specific level (None = all levels)
        """
        params = {}
        if level:
            params["level"] = level.value

        response = await self.client.get(
            f"{self.base_url}/api/v1/rubrics/{dimension.value}",
            params=params
        )
        response.raise_for_status()

        return [
            RubricCriteria(
                dimension=dimension,
                level=ScoreLevel(r["level"]),
                criteria_text=r["criteria_text"],
                keywords=r["keywords"],
                quantitative_thresholds=r.get("quantitative_thresholds", {}),
            )
            for r in response.json()
        ]

    async def close(self):
        await self.client.aclose()
