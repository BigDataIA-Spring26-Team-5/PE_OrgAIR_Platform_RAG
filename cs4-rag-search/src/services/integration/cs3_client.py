"""CS3 Scoring Engine API client.

Covers every active endpoint on the pe-org-air-platform at localhost:8000
that belongs to CS1 (company metadata) and CS3 (assessments, dimension scores,
scoring pipeline, rubrics).

Sections:
  1. CS1  — Company Metadata       GET/POST/PUT/DELETE /api/v1/companies/...
  2. CS3  — Assessment CRUD        POST/GET/PATCH /api/v1/assessments/...
  3. CS3  — Dimension Score CRUD   POST/GET/PUT /api/v1/assessments/{id}/scores, /scores/{id}
  4. CS3  — Scoring Pipeline       POST/GET /api/v1/scoring/...
  5. CS3  — Rubric & Keywords      local rubric text + Groq-expanded keywords
  6. CS3  — Groq Gap-Filling       estimate missing/zero scores with Groq LLM

Groq is used in two ways:
  a. keyword expansion   — per dimension, per ticker
  b. gap-filling         — when a dimension score is 0 or has no evidence, Groq
                           generates an estimated score + rationale from public knowledge
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import httpx
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Groq configuration (same key as cs2_client uses)
# ---------------------------------------------------------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = "llama-3.1-8b-instant"
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"


# ===========================================================================
# Enums
# ===========================================================================

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
    """Score levels (1–5)."""
    LEVEL_5 = 5  # 80-100 Excellent
    LEVEL_4 = 4  # 60-79  Good
    LEVEL_3 = 3  # 40-59  Adequate
    LEVEL_2 = 2  # 20-39  Developing
    LEVEL_1 = 1  # 0-19   Nascent

    @property
    def name_label(self) -> str:
        return {5: "Excellent", 4: "Good", 3: "Adequate", 2: "Developing", 1: "Nascent"}[self.value]

    @property
    def score_range(self) -> Tuple[int, int]:
        return {5: (80, 100), 4: (60, 79), 3: (40, 59), 2: (20, 39), 1: (0, 19)}[self.value]


class AssessmentStatus(str, Enum):
    DRAFT = "draft"
    IN_PROGRESS = "in_progress"
    SUBMITTED = "submitted"
    APPROVED = "approved"
    SUPERSEDED = "superseded"


class AssessmentType(str, Enum):
    INITIAL = "initial"
    FOLLOW_UP = "follow_up"
    ANNUAL = "annual"


# ===========================================================================
# Dataclasses
# ===========================================================================

@dataclass
class Company:
    """CS1 company record from Snowflake."""
    company_id: str
    name: str
    ticker: Optional[str]
    industry_id: str
    position_factor: float
    sector: Optional[str] = None
    sub_sector: Optional[str] = None
    market_cap_percentile: Optional[float] = None
    revenue_millions: Optional[float] = None
    employee_count: Optional[int] = None
    fiscal_year_end: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class DimensionScore:
    """Single dimension score from CS3."""
    dimension: Dimension
    score: float
    level: ScoreLevel
    confidence_interval: Tuple[float, float]
    evidence_count: int
    last_updated: str
    groq_estimated: bool = False        # True when score was filled by Groq
    groq_rationale: Optional[str] = None


@dataclass
class RubricCriteria:
    """Rubric criteria for one dimension/level."""
    dimension: Dimension
    level: ScoreLevel
    criteria_text: str
    keywords: List[str]
    quantitative_thresholds: Dict[str, float] = field(default_factory=dict)


@dataclass
class Assessment:
    """CS3 assessment record."""
    assessment_id: str
    company_id: str
    assessment_type: str
    assessment_date: str
    status: str
    primary_assessor: Optional[str] = None
    secondary_assessor: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class DimensionScoreRecord:
    """Dimension score attached to an assessment."""
    score_id: str
    assessment_id: str
    dimension: str
    score: float
    weight: float
    confidence: float
    evidence_count: int
    created_at: Optional[str] = None


@dataclass
class CompanyAssessment:
    """Full company assessment built from the scoring pipeline."""
    company_id: str
    assessment_date: str
    vr_score: float
    hr_score: float
    synergy_score: float
    org_air_score: float
    confidence_interval: Tuple[float, float]
    dimension_scores: Dict[Dimension, DimensionScore]
    talent_concentration: float
    position_factor: float


@dataclass
class GroqScoreEstimate:
    """Groq-generated score estimate for a missing/zero dimension."""
    dimension: Dimension
    ticker: str
    estimated_score: float
    level: ScoreLevel
    rationale: str
    confidence: float  # Groq's self-reported confidence (0–1)
    keywords: List[str]


# ===========================================================================
# Static rubric definitions
# ===========================================================================

_RUBRIC_TEXT: Dict[Dimension, Dict[int, str]] = {
    Dimension.DATA_INFRASTRUCTURE: {
        5: "Enterprise-grade, real-time data platform with ML-ready pipelines and data governance.",
        4: "Robust data warehouse with good pipeline coverage and partial ML readiness.",
        3: "Functional data infrastructure with some gaps in pipeline automation.",
        2: "Basic data storage with limited pipeline automation.",
        1: "Minimal or ad-hoc data infrastructure; no ML-ready pipelines.",
    },
    Dimension.AI_GOVERNANCE: {
        5: "Comprehensive AI ethics framework, model governance, and regulatory compliance.",
        4: "Formal AI governance policies with bias monitoring and explainability practices.",
        3: "Some AI governance in place; policies defined but inconsistently applied.",
        2: "Ad-hoc AI oversight; limited formal governance structure.",
        1: "No formal AI governance or ethics framework.",
    },
    Dimension.TECHNOLOGY_STACK: {
        5: "Best-in-class cloud-native ML platform with full MLOps and CI/CD for models.",
        4: "Modern cloud stack with MLOps tooling and containerised deployments.",
        3: "Cloud adoption with partial MLOps; some manual deployment steps.",
        2: "Hybrid on-premise/cloud with limited ML tooling.",
        1: "Legacy on-premise stack with no ML infrastructure.",
    },
    Dimension.TALENT: {
        5: "Deep AI/ML talent pool with specialised researchers and broad skills coverage.",
        4: "Strong data science and ML engineering team with diverse skills.",
        3: "Adequate ML team; some skill gaps in emerging areas.",
        2: "Small ML team; heavy reliance on a few individuals.",
        1: "Minimal AI talent; no dedicated ML roles.",
    },
    Dimension.LEADERSHIP: {
        5: "C-suite AI champion with published strategy, dedicated AI budget, and innovation labs.",
        4: "Strong executive sponsorship of AI with a defined roadmap.",
        3: "AI strategy exists but leadership engagement is inconsistent.",
        2: "Limited leadership visibility on AI initiatives.",
        1: "No clear AI strategy or executive sponsorship.",
    },
    Dimension.USE_CASE_PORTFOLIO: {
        5: "Broad portfolio of production AI use cases generating measurable business value.",
        4: "Several production AI use cases with clear ROI.",
        3: "Mix of production and pilot AI use cases.",
        2: "A few pilot AI projects; limited production deployments.",
        1: "Exploratory AI discussions only; no production use cases.",
    },
    Dimension.CULTURE: {
        5: "Data-driven culture embedded across all functions; continuous experimentation norm.",
        4: "Strong data-driven culture with active AI adoption programmes.",
        3: "Growing data culture; AI adoption varies across business units.",
        2: "Emerging data awareness; limited AI culture.",
        1: "Traditional culture; resistance to data-driven decision making.",
    },
}

_BASE_KEYWORDS: Dict[Dimension, List[str]] = {
    Dimension.DATA_INFRASTRUCTURE: ["data lake", "data warehouse", "ETL", "data pipeline", "real-time data", "cloud storage"],
    Dimension.AI_GOVERNANCE: ["AI ethics", "model governance", "responsible AI", "bias detection", "explainability", "AI policy"],
    Dimension.TECHNOLOGY_STACK: ["machine learning platform", "MLOps", "Kubernetes", "cloud-native", "microservices", "API gateway"],
    Dimension.TALENT: ["machine learning engineer", "data scientist", "AI researcher", "NLP", "computer vision", "deep learning"],
    Dimension.LEADERSHIP: ["Chief AI Officer", "AI strategy", "digital transformation", "technology roadmap", "innovation lab"],
    Dimension.USE_CASE_PORTFOLIO: ["AI use case", "automation", "predictive analytics", "recommendation system", "computer vision"],
    Dimension.CULTURE: ["data-driven", "experimentation", "agile", "innovation culture", "AI adoption", "continuous learning"],
}

_DIM_WEIGHTS: Dict[Dimension, float] = {
    Dimension.DATA_INFRASTRUCTURE: 0.20,
    Dimension.AI_GOVERNANCE: 0.15,
    Dimension.TECHNOLOGY_STACK: 0.15,
    Dimension.TALENT: 0.20,
    Dimension.LEADERSHIP: 0.10,
    Dimension.USE_CASE_PORTFOLIO: 0.10,
    Dimension.CULTURE: 0.10,
}

_DIM_ALIAS_MAP: Dict[str, Dimension] = {
    "data_infrastructure": Dimension.DATA_INFRASTRUCTURE,
    "ai_governance": Dimension.AI_GOVERNANCE,
    "technology_stack": Dimension.TECHNOLOGY_STACK,
    "talent": Dimension.TALENT,
    "talent_skills": Dimension.TALENT,
    "leadership": Dimension.LEADERSHIP,
    "leadership_vision": Dimension.LEADERSHIP,
    "use_case_portfolio": Dimension.USE_CASE_PORTFOLIO,
    "culture": Dimension.CULTURE,
    "culture_change": Dimension.CULTURE,
}


# ===========================================================================
# Private helpers
# ===========================================================================

def _score_to_level(score: float) -> ScoreLevel:
    if score >= 80:
        return ScoreLevel.LEVEL_5
    elif score >= 60:
        return ScoreLevel.LEVEL_4
    elif score >= 40:
        return ScoreLevel.LEVEL_3
    elif score >= 20:
        return ScoreLevel.LEVEL_2
    return ScoreLevel.LEVEL_1


def _map_platform_dimension(raw: str) -> Optional[Dimension]:
    return _DIM_ALIAS_MAP.get(raw.lower().strip())


def _weighted_vr(dim_scores: Dict[Dimension, DimensionScore]) -> float:
    total_w = sum(_DIM_WEIGHTS.get(d, 0.0) for d in dim_scores)
    if total_w == 0:
        return 0.0
    return sum(ds.score * _DIM_WEIGHTS.get(ds.dimension, 0.0) for ds in dim_scores.values()) / total_w


def _parse_company(d: dict) -> Company:
    return Company(
        company_id=str(d.get("id", "")),
        name=d.get("name", ""),
        ticker=d.get("ticker"),
        industry_id=str(d.get("industry_id", "")),
        position_factor=float(d.get("position_factor", 0.0)),
        sector=d.get("sector"),
        sub_sector=d.get("sub_sector"),
        market_cap_percentile=d.get("market_cap_percentile"),
        revenue_millions=d.get("revenue_millions"),
        employee_count=d.get("employee_count"),
        fiscal_year_end=d.get("fiscal_year_end"),
        created_at=str(d.get("created_at", "")),
        updated_at=str(d.get("updated_at", "")),
    )


def _parse_assessment(d: dict) -> Assessment:
    return Assessment(
        assessment_id=str(d.get("id", "")),
        company_id=str(d.get("company_id", "")),
        assessment_type=d.get("assessment_type", ""),
        assessment_date=str(d.get("assessment_date", "")),
        status=d.get("status", ""),
        primary_assessor=d.get("primary_assessor"),
        secondary_assessor=d.get("secondary_assessor"),
        created_at=str(d.get("created_at", "")),
        updated_at=str(d.get("updated_at", "")),
    )


def _parse_dim_score_record(d: dict) -> DimensionScoreRecord:
    return DimensionScoreRecord(
        score_id=str(d.get("id", "")),
        assessment_id=str(d.get("assessment_id", "")),
        dimension=d.get("dimension", ""),
        score=float(d.get("score", 0)),
        weight=float(d.get("weight", 0)),
        confidence=float(d.get("confidence", 0)),
        evidence_count=int(d.get("evidence_count", 0)),
        created_at=str(d.get("created_at", "")),
    )


def _build_dimension_score(row: dict) -> Optional[Tuple[Dimension, DimensionScore]]:
    dim_name = row.get("dimension", "")
    mapped = _map_platform_dimension(dim_name)
    if mapped is None:
        return None
    score_val = float(row.get("score", 0))
    level = _score_to_level(score_val)
    confidence = float(row.get("confidence", 0.8))
    half_ci = score_val * (1 - confidence) * 0.5
    ds = DimensionScore(
        dimension=mapped,
        score=score_val,
        level=level,
        confidence_interval=(max(0.0, score_val - half_ci), min(100.0, score_val + half_ci)),
        evidence_count=int(row.get("evidence_count", 0)),
        last_updated=str(row.get("created_at") or datetime.now(timezone.utc).isoformat()),
    )
    return mapped, ds


# ===========================================================================
# Groq helpers
# ===========================================================================

async def _groq_post(prompt: str, max_tokens: int = 300, temperature: float = 0.3) -> Optional[str]:
    """Call Groq and return the response text, or None on failure."""
    if not GROQ_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                GROQ_API_URL,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": GROQ_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                },
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.warning("groq_call_failed: %s", e)
        return None


async def expand_keywords(ticker: str, dimension: Dimension) -> List[str]:
    """
    Expand base keywords for a dimension using Groq, company-specific context.
    Falls back to static base keywords if Groq is unavailable.
    """
    base = _BASE_KEYWORDS.get(dimension, [])
    prompt = (
        f"You are a PE analyst. For '{ticker}', list 10 additional keywords/phrases "
        f"(comma-separated) that appear in SEC filings, earnings calls, or analyst reports "
        f"when evaluating the '{dimension.value}' AI-readiness dimension. "
        f"Base keywords: {', '.join(base)}. "
        f"Return ONLY comma-separated keywords."
    )
    text = await _groq_post(prompt, max_tokens=200)
    if not text:
        return base
    expanded = [k.strip() for k in text.split(",") if k.strip()]
    return list(set(base + expanded))


async def estimate_missing_score(ticker: str, dimension: Dimension, company_name: str = "") -> GroqScoreEstimate:
    """
    When a dimension has no evidence (score=0, evidence_count=0), use Groq to
    generate a plausible score estimate from public knowledge about the company.

    Returns a GroqScoreEstimate with estimated_score, rationale, confidence, and keywords.
    """
    dim_label = dimension.value.replace("_", " ").title()
    name_hint = f"({company_name})" if company_name else ""
    prompt = (
        f"You are a senior PE analyst assessing AI readiness. "
        f"For the company with ticker '{ticker}' {name_hint}, estimate a score (0–100) "
        f"for the '{dim_label}' dimension based on publicly available information. "
        f"Respond in this exact JSON format:\n"
        f'{{"score": <0-100>, "confidence": <0.0-1.0>, "rationale": "<2-3 sentences>", '
        f'"keywords": ["kw1", "kw2", "kw3", "kw4", "kw5"]}}\n'
        f"Base your estimate on the rubric: "
        f"{json.dumps(_RUBRIC_TEXT.get(dimension, {}))}"
    )
    text = await _groq_post(prompt, max_tokens=400, temperature=0.4)
    # Parse JSON response
    estimated_score = 0.0
    confidence = 0.3
    rationale = "Groq unavailable — score estimated as 0."
    keywords: List[str] = _BASE_KEYWORDS.get(dimension, [])

    if text:
        try:
            # Extract JSON block if wrapped in markdown
            json_str = text
            if "```" in text:
                json_str = text.split("```")[1].lstrip("json").strip()
            data = json.loads(json_str)
            estimated_score = float(data.get("score", 0))
            confidence = float(data.get("confidence", 0.3))
            rationale = str(data.get("rationale", ""))
            keywords = data.get("keywords", keywords)
        except Exception:
            # Try to parse score with regex fallback
            import re
            m = re.search(r'"score"\s*:\s*([\d.]+)', text)
            if m:
                estimated_score = float(m.group(1))
            rationale = text[:300]

    level = _score_to_level(estimated_score)
    return GroqScoreEstimate(
        dimension=dimension,
        ticker=ticker,
        estimated_score=estimated_score,
        level=level,
        rationale=rationale,
        confidence=confidence,
        keywords=keywords,
    )


async def enrich_company_fields(ticker: str, company_name: str) -> Dict[str, Any]:
    """
    Use Groq to fill in missing company metadata fields
    (sector, sub_sector, revenue, employee_count, fiscal_year_end).

    Returns a dict with any fields it can estimate.
    """
    prompt = (
        f"For the public company with ticker '{ticker}' (name: '{company_name}'), "
        f"provide the following in JSON format:\n"
        f'{{"sector": "<sector>", "sub_sector": "<sub_sector>", '
        f'"revenue_millions": <number or null>, "employee_count": <integer or null>, '
        f'"fiscal_year_end": "<MM-DD or null>"}}\n'
        f"Use your knowledge of the company. Return ONLY valid JSON."
    )
    text = await _groq_post(prompt, max_tokens=200, temperature=0.2)
    if not text:
        return {}
    try:
        json_str = text
        if "```" in text:
            json_str = text.split("```")[1].lstrip("json").strip()
        return json.loads(json_str)
    except Exception:
        return {}


# ===========================================================================
# CS3Client
# ===========================================================================

class CS3Client:
    """
    Client for CS3 Scoring Engine + CS1 Company Metadata.
    All calls go to pe-org-air-platform at localhost:8000.

    Sections:
      1. CS1 — Company Metadata
      2. CS3 — Assessment CRUD
      3. CS3 — Dimension Score CRUD
      4. CS3 — Scoring Pipeline
      5. CS3 — Rubric & Keyword Enrichment
      6. CS3 — Groq Gap-Filling
    """

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=60.0)

    # -----------------------------------------------------------------------
    # Section 1 — CS1: Company Metadata
    # -----------------------------------------------------------------------

    async def get_company(self, ticker: str) -> Company:
        """
        Get a company by ticker symbol (or UUID).
        GET /api/v1/companies/{ticker}
        """
        resp = await self.client.get(f"{self.base_url}/api/v1/companies/{ticker.upper()}")
        resp.raise_for_status()
        return _parse_company(resp.json())

    async def list_companies(self, page: int = 1, page_size: int = 100) -> List[Company]:
        """
        List companies (paginated).
        GET /api/v1/companies
        """
        resp = await self.client.get(
            f"{self.base_url}/api/v1/companies",
            params={"page": page, "page_size": page_size},
        )
        resp.raise_for_status()
        return [_parse_company(c) for c in resp.json().get("items", [])]

    async def list_all_companies(self) -> List[Company]:
        """
        Get all companies without pagination.
        GET /api/v1/companies/all
        """
        resp = await self.client.get(f"{self.base_url}/api/v1/companies/all")
        resp.raise_for_status()
        return [_parse_company(c) for c in resp.json().get("items", [])]

    async def create_company(
        self,
        name: str,
        industry_id: str,
        ticker: Optional[str] = None,
        position_factor: float = 0.0,
    ) -> Company:
        """
        Create a new company. Triggers background Groq enrichment (sector, revenue,
        employee count, fiscal year end) automatically on the server side.
        POST /api/v1/companies
        """
        payload: Dict[str, Any] = {
            "name": name,
            "industry_id": industry_id,
            "position_factor": position_factor,
        }
        if ticker:
            payload["ticker"] = ticker.upper()
        resp = await self.client.post(f"{self.base_url}/api/v1/companies", json=payload)
        resp.raise_for_status()
        return _parse_company(resp.json())

    async def update_company(self, ticker: str, **fields) -> Company:
        """
        Update company fields by ticker.
        PUT /api/v1/companies/{ticker}
        """
        resp = await self.client.put(
            f"{self.base_url}/api/v1/companies/{ticker.upper()}",
            json=fields,
        )
        resp.raise_for_status()
        return _parse_company(resp.json())

    async def delete_company(self, ticker: str) -> Dict[str, Any]:
        """
        Soft-delete a company by ticker.
        DELETE /api/v1/companies/{ticker}
        """
        resp = await self.client.delete(f"{self.base_url}/api/v1/companies/{ticker.upper()}")
        resp.raise_for_status()
        return resp.json()

    async def get_dimension_keywords(self, ticker: str, dimension: Dimension) -> List[str]:
        """
        Fetch Groq-expanded rubric keywords from the platform.
        GET /api/v1/companies/{ticker}/dimension-keywords?dimension={dimension}

        Falls back to local Groq call if the endpoint is unavailable.
        """
        try:
            resp = await self.client.get(
                f"{self.base_url}/api/v1/companies/{ticker.upper()}/dimension-keywords",
                params={"dimension": dimension.value},
            )
            if resp.status_code == 200:
                return resp.json().get("keywords", [])
        except Exception:
            pass
        # Local Groq fallback
        return await expand_keywords(ticker, dimension)

    # -----------------------------------------------------------------------
    # Section 2 — CS3: Assessment CRUD
    # -----------------------------------------------------------------------

    async def create_assessment(
        self,
        company_id: str,
        assessment_type: AssessmentType = AssessmentType.INITIAL,
        assessment_date: Optional[str] = None,
        primary_assessor: Optional[str] = None,
        secondary_assessor: Optional[str] = None,
    ) -> Assessment:
        """
        Create a new IC assessment for a company.
        POST /api/v1/assessments
        Status starts as 'draft'.
        """
        payload: Dict[str, Any] = {
            "company_id": company_id,
            "assessment_type": assessment_type.value,
            "assessment_date": assessment_date or datetime.now(timezone.utc).date().isoformat(),
        }
        if primary_assessor:
            payload["primary_assessor"] = primary_assessor
        if secondary_assessor:
            payload["secondary_assessor"] = secondary_assessor
        resp = await self.client.post(f"{self.base_url}/api/v1/assessments", json=payload)
        resp.raise_for_status()
        return _parse_assessment(resp.json())

    async def list_assessments(
        self,
        company_id: Optional[str] = None,
        assessment_type: Optional[AssessmentType] = None,
        status: Optional[AssessmentStatus] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> List[Assessment]:
        """
        List assessments with optional filters.
        GET /api/v1/assessments
        """
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        if company_id:
            params["company_id"] = company_id
        if assessment_type:
            params["assessment_type"] = assessment_type.value
        if status:
            params["status"] = status.value
        resp = await self.client.get(f"{self.base_url}/api/v1/assessments", params=params)
        resp.raise_for_status()
        return [_parse_assessment(a) for a in resp.json().get("items", [])]

    async def get_assessment_by_id(self, assessment_id: str) -> Assessment:
        """
        Get a single assessment by UUID.
        GET /api/v1/assessments/{assessment_id}
        """
        resp = await self.client.get(f"{self.base_url}/api/v1/assessments/{assessment_id}")
        resp.raise_for_status()
        return _parse_assessment(resp.json())

    async def update_assessment_status(
        self,
        assessment_id: str,
        status: AssessmentStatus,
    ) -> Assessment:
        """
        Update assessment status.
        PATCH /api/v1/assessments/{assessment_id}/status
        Valid transitions: draft → in_progress → submitted → approved → superseded
        """
        resp = await self.client.patch(
            f"{self.base_url}/api/v1/assessments/{assessment_id}/status",
            json={"status": status.value},
        )
        resp.raise_for_status()
        return _parse_assessment(resp.json())

    # -----------------------------------------------------------------------
    # Section 3 — CS3: Dimension Score CRUD
    # -----------------------------------------------------------------------

    async def add_dimension_score(
        self,
        assessment_id: str,
        dimension: Dimension,
        score: float,
        weight: float,
        confidence: float = 0.8,
        evidence_count: int = 0,
    ) -> DimensionScoreRecord:
        """
        Add a dimension score to an assessment.
        POST /api/v1/assessments/{assessment_id}/scores

        Raises 409 if a score for that dimension already exists.
        """
        payload = {
            "assessment_id": assessment_id,
            "dimension": dimension.value,
            "score": score,
            "weight": weight,
            "confidence": confidence,
            "evidence_count": evidence_count,
        }
        resp = await self.client.post(
            f"{self.base_url}/api/v1/assessments/{assessment_id}/scores",
            json=payload,
        )
        resp.raise_for_status()
        return _parse_dim_score_record(resp.json())

    async def get_assessment_scores(self, assessment_id: str) -> List[DimensionScoreRecord]:
        """
        Get all dimension scores for an assessment.
        GET /api/v1/assessments/{assessment_id}/scores
        """
        resp = await self.client.get(
            f"{self.base_url}/api/v1/assessments/{assessment_id}/scores"
        )
        resp.raise_for_status()
        return [_parse_dim_score_record(s) for s in resp.json()]

    async def update_dimension_score(
        self,
        score_id: str,
        score: Optional[float] = None,
        weight: Optional[float] = None,
        confidence: Optional[float] = None,
        evidence_count: Optional[int] = None,
    ) -> DimensionScoreRecord:
        """
        Update an existing dimension score by its UUID.
        PUT /api/v1/scores/{score_id}
        """
        payload = {k: v for k, v in {
            "score": score,
            "weight": weight,
            "confidence": confidence,
            "evidence_count": evidence_count,
        }.items() if v is not None}
        resp = await self.client.put(
            f"{self.base_url}/api/v1/scores/{score_id}",
            json=payload,
        )
        resp.raise_for_status()
        return _parse_dim_score_record(resp.json())

    async def get_dimension_weights(self) -> Dict[str, float]:
        """
        Get the configured dimension weights (must sum to 1.0).
        GET /api/v1/dimensions/weights
        """
        resp = await self.client.get(f"{self.base_url}/api/v1/dimensions/weights")
        resp.raise_for_status()
        return resp.json().get("weights", {})

    # -----------------------------------------------------------------------
    # Section 4 — CS3: Scoring Pipeline
    # -----------------------------------------------------------------------

    async def score_company(self, ticker: str) -> Dict[str, Any]:
        """
        Run the full CS3 scoring pipeline for one company.
        POST /api/v1/scoring/{ticker}

        Pipeline:
          1. Read CS2 signals from company_signal_summaries
          2. Read SEC sections from document_chunks + S3 (Item 1, 1A, 7)
          3. Rubric-score SEC text against 7-dimension rubrics
          4. Map evidence to 7 dimensions (Table 1 matrix)
          5. Persist mapping matrix + dimension scores to Snowflake

        Prerequisite: company must have CS2 signal data.
        """
        resp = await self.client.post(
            f"{self.base_url}/api/v1/scoring/{ticker.upper()}"
        )
        resp.raise_for_status()
        return resp.json()

    async def score_all_companies(self) -> Dict[str, Any]:
        """
        Run CS3 scoring pipeline for every company with CS2 signal data.
        POST /api/v1/scoring/all
        """
        resp = await self.client.post(f"{self.base_url}/api/v1/scoring/all")
        resp.raise_for_status()
        return resp.json()

    async def get_mapping_matrix(self, ticker: str) -> List[Dict[str, Any]]:
        """
        Get the signal-to-dimension mapping matrix (Table 1) from Snowflake.
        GET /api/v1/scoring/{ticker}/matrix

        Each row = one evidence source with raw score + weight contributions
        to each of the 7 dimensions.
        """
        resp = await self.client.get(
            f"{self.base_url}/api/v1/scoring/{ticker.upper()}/matrix"
        )
        resp.raise_for_status()
        return resp.json().get("rows", [])

    async def get_dimension_scores_raw(self, ticker: str) -> List[Dict[str, Any]]:
        """
        Get raw 7 dimension scores from Snowflake.
        GET /api/v1/scoring/{ticker}/dimensions
        """
        resp = await self.client.get(
            f"{self.base_url}/api/v1/scoring/{ticker.upper()}/dimensions"
        )
        resp.raise_for_status()
        return resp.json().get("scores", [])

    async def get_full_scoring_view(self, ticker: str) -> Dict[str, Any]:
        """
        Get the full scoring view: mapping matrix + dimension scores + coverage.
        GET /api/v1/scoring/{ticker}/full
        """
        resp = await self.client.get(
            f"{self.base_url}/api/v1/scoring/{ticker.upper()}/full"
        )
        resp.raise_for_status()
        return resp.json()

    async def get_scoring_summary(self) -> List[Dict[str, Any]]:
        """
        Get dimension scores for all scored companies.
        GET /api/v1/scoring/summary
        """
        resp = await self.client.get(f"{self.base_url}/api/v1/scoring/summary")
        resp.raise_for_status()
        return resp.json().get("companies", [])

    async def delete_scoring_data(self, ticker: str) -> Dict[str, Any]:
        """
        Delete scoring data (mapping matrix + dimension scores) for a company.
        DELETE /api/v1/scoring/{ticker}
        """
        resp = await self.client.delete(
            f"{self.base_url}/api/v1/scoring/{ticker.upper()}"
        )
        resp.raise_for_status()
        return resp.json()

    # -----------------------------------------------------------------------
    # Section 5 — CS3: Rubric & Keyword Enrichment
    # -----------------------------------------------------------------------

    async def get_rubric(
        self,
        dimension: Dimension,
        level: Optional[ScoreLevel] = None,
        ticker: Optional[str] = None,
    ) -> List[RubricCriteria]:
        """
        Return rubric criteria for a dimension (all levels, or a specific level).

        If ticker is provided, keywords are expanded via Groq (platform endpoint
        first, local Groq call as fallback).
        """
        levels = [level] if level else list(ScoreLevel)

        keywords_for_level: Dict[int, List[str]] = {}
        if ticker:
            try:
                expanded = await self.get_dimension_keywords(ticker, dimension)
                for lv in levels:
                    keywords_for_level[lv.value] = expanded
            except Exception:
                pass

        rubrics = []
        for lv in levels:
            kws = keywords_for_level.get(lv.value, _BASE_KEYWORDS.get(dimension, []))
            rubrics.append(RubricCriteria(
                dimension=dimension,
                level=lv,
                criteria_text=_RUBRIC_TEXT.get(dimension, {}).get(lv.value, ""),
                keywords=kws,
                quantitative_thresholds={
                    "min_score": float(lv.score_range[0]),
                    "max_score": float(lv.score_range[1]),
                },
            ))
        return rubrics

    # -----------------------------------------------------------------------
    # Section 6 — CS3: Groq Gap-Filling
    # -----------------------------------------------------------------------

    async def get_assessment(self, ticker: str, fill_gaps: bool = True) -> CompanyAssessment:
        """
        Fetch a complete CompanyAssessment by ticker.

        Calls GET /api/v1/scoring/{ticker}/full and maps the response.
        When fill_gaps=True (default), any dimension with score=0 and
        evidence_count=0 is filled with a Groq-estimated score so that
        the IC package is never empty.
        """
        full_data = await self.get_full_scoring_view(ticker)

        dim_scores: Dict[Dimension, DimensionScore] = {}
        for row in full_data.get("dimension_scores", []):
            result = _build_dimension_score(row)
            if result:
                mapped, ds = result
                dim_scores[mapped] = ds

        # Gap-fill with Groq for missing or zero-evidence dimensions
        if fill_gaps:
            company_name = full_data.get("company_name", "")
            for dim in Dimension:
                ds = dim_scores.get(dim)
                if ds is None or (ds.score == 0.0 and ds.evidence_count == 0):
                    estimate = await estimate_missing_score(ticker, dim, company_name)
                    dim_scores[dim] = DimensionScore(
                        dimension=dim,
                        score=estimate.estimated_score,
                        level=estimate.level,
                        confidence_interval=(
                            max(0.0, estimate.estimated_score * (1 - estimate.confidence)),
                            min(100.0, estimate.estimated_score * (1 + estimate.confidence * 0.3)),
                        ),
                        evidence_count=0,
                        last_updated=datetime.now(timezone.utc).isoformat(),
                        groq_estimated=True,
                        groq_rationale=estimate.rationale,
                    )

        vr_score = _weighted_vr(dim_scores)
        ci_half = vr_score * 0.1
        pf_raw = full_data.get("coverage", {})
        pf = float(pf_raw.get("position_factor", 0.0)) if isinstance(pf_raw, dict) else 0.0

        return CompanyAssessment(
            company_id=full_data.get("company_id", ticker),
            assessment_date=str(full_data.get("last_scored") or datetime.now(timezone.utc).date().isoformat()),
            vr_score=vr_score,
            hr_score=0.0,       # not returned by scoring/full; computed by composite_scoring_service
            synergy_score=0.0,
            org_air_score=vr_score,
            confidence_interval=(max(0.0, vr_score - ci_half), min(100.0, vr_score + ci_half)),
            dimension_scores=dim_scores,
            talent_concentration=0.0,
            position_factor=pf,
        )

    async def get_dimension_score(
        self,
        ticker: str,
        dimension: Dimension,
        fill_gap: bool = True,
    ) -> DimensionScore:
        """
        Fetch a single dimension score for a ticker.
        GET /api/v1/scoring/{ticker}/dimensions — filter for the requested dimension.

        When fill_gap=True and the dimension has score=0 / no evidence,
        Groq estimates a plausible score from public knowledge.
        """
        try:
            rows = await self.get_dimension_scores_raw(ticker)
        except Exception:
            rows = []

        for row in rows:
            result = _build_dimension_score(row)
            if result:
                mapped, ds = result
                if mapped == dimension:
                    if fill_gap and ds.score == 0.0 and ds.evidence_count == 0:
                        break   # fall through to Groq estimate below
                    return ds

        if fill_gap:
            try:
                company = await self.get_company(ticker)
                company_name = company.name
            except Exception:
                company_name = ""
            estimate = await estimate_missing_score(ticker, dimension, company_name)
            return DimensionScore(
                dimension=dimension,
                score=estimate.estimated_score,
                level=estimate.level,
                confidence_interval=(
                    max(0.0, estimate.estimated_score * (1 - estimate.confidence)),
                    min(100.0, estimate.estimated_score * (1 + estimate.confidence * 0.3)),
                ),
                evidence_count=0,
                last_updated=datetime.now(timezone.utc).isoformat(),
                groq_estimated=True,
                groq_rationale=estimate.rationale,
            )

        # No fill — return zero default
        return DimensionScore(
            dimension=dimension,
            score=0.0,
            level=ScoreLevel.LEVEL_1,
            confidence_interval=(0.0, 0.0),
            evidence_count=0,
            last_updated=datetime.now(timezone.utc).isoformat(),
        )

    async def get_all_dimension_estimates(self, ticker: str) -> Dict[Dimension, GroqScoreEstimate]:
        """
        Use Groq to estimate scores for ALL 7 dimensions for a ticker.
        Useful for companies with no CS2/CS3 data yet — gives a baseline IC view.
        """
        try:
            company = await self.get_company(ticker)
            company_name = company.name
        except Exception:
            company_name = ""

        estimates: Dict[Dimension, GroqScoreEstimate] = {}
        for dim in Dimension:
            estimates[dim] = await estimate_missing_score(ticker, dim, company_name)
        return estimates

    async def get_enriched_company(self, ticker: str) -> Company:
        """
        Get a company, and if any metadata fields (sector, revenue, etc.) are missing,
        use Groq to fill them in.

        Does NOT write back to the platform — returns the enriched object only.
        """
        company = await self.get_company(ticker)

        missing = not any([
            company.sector,
            company.revenue_millions,
            company.employee_count,
        ])
        if missing:
            enriched = await enrich_company_fields(ticker, company.name)
            company.sector = enriched.get("sector") or company.sector
            company.sub_sector = enriched.get("sub_sector") or company.sub_sector
            company.revenue_millions = enriched.get("revenue_millions") or company.revenue_millions
            company.employee_count = enriched.get("employee_count") or company.employee_count
            company.fiscal_year_end = enriched.get("fiscal_year_end") or company.fiscal_year_end

        return company

    async def close(self):
        await self.client.aclose()
