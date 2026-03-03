"""
cs3_client.py — CS4 RAG Search
src/services/integration/cs3_client.py

HTTP client for the CS3 scoring layer (pe-org-air-platform /scoring/* endpoints).
Data models derived from:
  - app/models/enumerations.py  (Dimension)
  - app/models/dimension.py     (DIMENSION_WEIGHTS, DimensionScoreBase)
  - app/models/assessment.py    (AssessmentResponse)
  - app/scoring/evidence_mapper.py lines 80–88 (DimensionScore dataclass)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx
import structlog

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Dimension(str, Enum):
    """
    7 V^R dimensions.
    CS4 spec renames three values for readability in prompts:
      talent     (platform: TALENT_SKILLS     / talent_skills)
      leadership (platform: LEADERSHIP_VISION / leadership_vision)
      culture    (platform: CULTURE_CHANGE    / culture_change)
    """
    DATA_INFRASTRUCTURE = "data_infrastructure"
    AI_GOVERNANCE       = "ai_governance"
    TECHNOLOGY_STACK    = "technology_stack"
    TALENT              = "talent"          # was TALENT_SKILLS
    LEADERSHIP          = "leadership"      # was LEADERSHIP_VISION
    USE_CASE_PORTFOLIO  = "use_case_portfolio"
    CULTURE             = "culture"         # was CULTURE_CHANGE


# Platform value → CS4 value (for translating API responses)
_DIMENSION_ALIAS: Dict[str, str] = {
    "talent_skills":    "talent",
    "leadership_vision": "leadership",
    "culture_change":   "culture",
}

# Default weights (from app/models/dimension.py DIMENSION_WEIGHTS)
DIMENSION_WEIGHTS: Dict[Dimension, float] = {
    Dimension.DATA_INFRASTRUCTURE: 0.25,
    Dimension.AI_GOVERNANCE:       0.20,
    Dimension.TECHNOLOGY_STACK:    0.15,
    Dimension.TALENT:              0.15,
    Dimension.LEADERSHIP:          0.10,
    Dimension.USE_CASE_PORTFOLIO:  0.10,
    Dimension.CULTURE:             0.05,
}


class ScoreLevel(int, Enum):
    """
    5-level rubric scale used by RubricScorer.
    New in CS4 — provides human-readable labels and score range helpers.
    """
    LEVEL_1 = 1
    LEVEL_2 = 2
    LEVEL_3 = 3
    LEVEL_4 = 4
    LEVEL_5 = 5

    @property
    def name_label(self) -> str:
        labels = {1: "Minimal", 2: "Basic", 3: "Developing", 4: "Advanced", 5: "Leading"}
        return labels[self.value]

    @property
    def score_range(self) -> tuple[float, float]:
        """Approximate 0–100 range for each rubric level."""
        ranges = {1: (0, 20), 2: (20, 40), 3: (40, 60), 4: (60, 80), 5: (80, 100)}
        return ranges[self.value]

    @classmethod
    def from_score(cls, score: float) -> "ScoreLevel":
        """Derive level from a 0–100 score."""
        for level in reversed(cls):
            lo, _ = level.score_range
            if score >= lo:
                return level
        return cls.LEVEL_1


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class DimensionScore:
    """
    Aggregated score for one dimension — CS3 API response + CS4 additions.
    Ported from EvidenceMapper.DimensionScore (evidence_mapper.py:80-88)
    plus DimensionScoreBase (app/models/dimension.py).
    """
    dimension: Dimension
    score: float                            # 0–100
    weight: float                           # dimension weight (0–1)
    confidence: float                       # 0–1
    evidence_count: int = 0
    level: Optional[ScoreLevel] = None      # derived from score (CS4 addition)
    last_updated: Optional[datetime] = None # CS4 addition

    def __post_init__(self) -> None:
        if self.level is None and self.score is not None:
            self.level = ScoreLevel.from_score(self.score)

    @classmethod
    def from_api_dict(cls, data: Dict[str, Any]) -> "DimensionScore":
        raw_dim = data.get("dimension", "")
        canonical = _DIMENSION_ALIAS.get(raw_dim, raw_dim)
        dim = Dimension(canonical)
        score = float(data.get("score", 0))
        return cls(
            dimension=dim,
            score=score,
            weight=float(data.get("weight", DIMENSION_WEIGHTS.get(dim, 0.0))),
            confidence=float(data.get("confidence", 0.8)),
            evidence_count=int(data.get("evidence_count", 0)),
            last_updated=_parse_dt(data.get("created_at")),
        )


@dataclass
class RubricCriteria:
    """
    Rubric criteria for a dimension level — CS4 new.
    Populated by get_rubric() if the platform endpoint exists.
    """
    dimension: Dimension
    level: ScoreLevel
    description: str
    keywords: List[str] = field(default_factory=list)
    examples: List[str] = field(default_factory=list)


@dataclass
class CompanyAssessment:
    """
    Aggregate view of a company's CS3 assessment.
    Fields combined from AssessmentResponse + scoring router fields.
    """
    assessment_id: str
    company_id: str
    assessment_type: str
    assessment_date: date
    status: str
    v_r_score: Optional[float] = None
    confidence_lower: Optional[float] = None
    confidence_upper: Optional[float] = None
    primary_assessor: Optional[str] = None
    secondary_assessor: Optional[str] = None
    created_at: Optional[datetime] = None
    dimension_scores: List[DimensionScore] = field(default_factory=list)

    @classmethod
    def from_api_dict(cls, data: Dict[str, Any]) -> "CompanyAssessment":
        return cls(
            assessment_id=str(data.get("id", "")),
            company_id=str(data.get("company_id", "")),
            assessment_type=data.get("assessment_type", ""),
            assessment_date=_parse_date(data.get("assessment_date")),
            status=data.get("status", ""),
            v_r_score=data.get("v_r_score"),
            confidence_lower=data.get("confidence_lower"),
            confidence_upper=data.get("confidence_upper"),
            primary_assessor=data.get("primary_assessor"),
            secondary_assessor=data.get("secondary_assessor"),
            created_at=_parse_dt(data.get("created_at")),
        )


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _parse_date(value: Optional[str]) -> date:
    if value is None:
        return date.today()
    try:
        return date.fromisoformat(str(value)[:10])
    except (ValueError, AttributeError):
        return date.today()


# ---------------------------------------------------------------------------
# CS3Client
# ---------------------------------------------------------------------------

class CS3Client:
    """
    Async HTTP client for the CS3 scoring endpoints.
    Base URL defaults to CS3_BASE_URL env var (fallback: http://localhost:8000).
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self._base_url = (base_url or os.getenv("CS3_BASE_URL", "http://localhost:8000")).rstrip("/")
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=timeout)
        self._log = logger.bind(client="cs3")

    async def get_assessment(self, assessment_id: str) -> CompanyAssessment:
        """GET /assessments/{id}"""
        self._log.info("fetching assessment", assessment_id=assessment_id)
        resp = await self._client.get(f"/assessments/{assessment_id}")
        resp.raise_for_status()
        assessment = CompanyAssessment.from_api_dict(resp.json())

        # Attempt to hydrate dimension scores
        try:
            scores = await self.get_dimension_score(assessment_id)
            assessment.dimension_scores = scores
        except httpx.HTTPError:
            pass  # scores unavailable — return assessment without them

        return assessment

    async def get_dimension_score(self, assessment_id: str) -> List[DimensionScore]:
        """
        GET /assessments/{id}/scores
        Falls back to GET /scores?assessment_id={id} if the direct endpoint
        returns 404 (known platform gap — no per-dimension sub-route yet).
        """
        url = f"/assessments/{assessment_id}/scores"
        self._log.info("fetching dimension scores", assessment_id=assessment_id)
        resp = await self._client.get(url)
        if resp.status_code == 404:
            resp = await self._client.get("/scores", params={"assessment_id": assessment_id})
        resp.raise_for_status()
        data = resp.json()
        items = data if isinstance(data, list) else data.get("items", [])
        return [DimensionScore.from_api_dict(row) for row in items]

    async def get_rubric(self, dimension: Dimension) -> List[RubricCriteria]:
        """
        GET /rubrics/{dimension}
        Stubbed — platform does not expose this endpoint yet.
        """
        raise NotImplementedError(
            f"Platform does not expose GET /rubrics/{dimension.value}. "
            "Implement a local rubric store or wait for the endpoint."
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "CS3Client":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()
