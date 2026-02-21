
"""
Rubric-Based Scorer — Task 5.0b (CS3)
app/scoring/rubric_scorer.py

Converts evidence text + quantitative metrics into rubric-aligned 0-100 scores
for each of the 7 V^R dimensions.

Each dimension has a 5-level rubric (CS3 p.11-13):
  Level 5: 80-100 (Excellent)
  Level 4: 60-79  (Good)
  Level 3: 40-59  (Adequate)
  Level 2: 20-39  (Developing)
  Level 1:  0-19  (Nascent)

Keywords are expanded to match actual SEC filing language (10-K Items 1, 1A, 7)
while preserving the CS3 rubric structure and intent.
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from enum import Enum
from decimal import Decimal, ROUND_HALF_UP
import re


# ---------------------------------------------------------------------------
# Score Levels
# ---------------------------------------------------------------------------

class ScoreLevel(Enum):
    LEVEL_5 = (80, 100, "Excellent")
    LEVEL_4 = (60, 79, "Good")
    LEVEL_3 = (40, 59, "Adequate")
    LEVEL_2 = (20, 39, "Developing")
    LEVEL_1 = (0, 19, "Nascent")

    @property
    def min_score(self) -> int:
        return self.value[0]

    @property
    def max_score(self) -> int:
        return self.value[1]

    @property
    def label(self) -> str:
        return self.value[2]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RubricCriteria:
    level: ScoreLevel
    keywords: List[str]
    min_keyword_matches: int
    quantitative_threshold: float


@dataclass
class RubricResult:
    dimension: str
    level: ScoreLevel
    score: Decimal
    matched_keywords: List[str]
    keyword_match_count: int
    confidence: Decimal
    rationale: str


# ---------------------------------------------------------------------------
# THE 7 DIMENSION RUBRICS — EXPANDED FOR REAL SEC FILING LANGUAGE
#
# Changes from original:
# - Keywords expanded to include terms that actually appear in 10-K filings
# - min_keyword_matches lowered where appropriate (SEC text is verbose,
#   even 2 strong matches indicates real capability)
# - Added industry-specific terms (financial services, retail, manufacturing)
# ---------------------------------------------------------------------------

DIMENSION_RUBRICS: Dict[str, Dict[ScoreLevel, RubricCriteria]] = {

    # ── Data Infrastructure ───────────────────────────────────────────
    "data_infrastructure": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                "snowflake", "databricks", "lakehouse", "real-time",
                "data quality", "api-first", "data mesh", "streaming",
                "cloud platform", "data lake", "delta lake",
                "real-time pipeline", "data fabric", "data platform",
            ],
            min_keyword_matches=3,
            quantitative_threshold=0.40,
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            keywords=[
                "azure", "aws", "warehouse", "etl", "data catalog",
                "hybrid cloud", "batch pipeline", "redshift", "bigquery",
                "data governance", "cloud infrastructure", "data center",
                "cloud computing", "cloud services", "data processing",
                "cloud-based", "scalable infrastructure",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.25,
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            keywords=[
                "migration", "hybrid", "modernizing", "cloud adoption",
                "data warehouse", "sql server", "oracle", "infrastructure",
                "technology infrastructure", "systems modernization",
                "information technology", "data management",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.15,
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "legacy", "silos", "on-premise", "on premise",
                "fragmented", "data quality issues", "legacy systems",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.05,
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "mainframe", "spreadsheets", "manual", "no infrastructure",
                "paper-based",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
        ),
    },

    # ── AI Governance ─────────────────────────────────────────────────
    "ai_governance": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                "caio", "cdo", "board committee", "model risk",
                "chief ai officer", "ai ethics", "responsible ai",
                "ai governance framework", "model governance",
                "ai risk management", "ethical ai", "ai oversight",
                "ai principles", "trustworthy ai",
            ],
            min_keyword_matches=3,
            quantitative_threshold=0.40,
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            keywords=[
                "vp data", "ai policy", "risk framework",
                "chief data officer", "ai risk", "data governance",
                "model validation", "ai principles",
                "risk management framework", "technology risk",
                "cybersecurity", "data privacy", "information security",
                "regulatory compliance", "model risk management",
                "operational risk", "compliance framework",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.25,
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            keywords=[
                "director", "guidelines", "it governance",
                "compliance", "risk management", "data privacy",
                "risk factors", "regulatory", "internal controls",
                "governance", "oversight", "risk assessment",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.15,
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "informal", "no policy", "ad-hoc", "ad hoc",
                "limited oversight",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.05,
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "none", "no oversight", "unmanaged", "no governance",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
        ),
    },

    # ── Technology Stack ──────────────────────────────────────────────
    "technology_stack": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                "sagemaker", "mlops", "feature store", "vertex ai",
                "model registry", "automated pipeline", "kubeflow",
                "ml platform", "mlflow", "model serving",
                "inference", "training infrastructure", "gpu cluster",
                "ai infrastructure", "accelerated computing",
                "tensorrt", "triton", "cuda",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.40,
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            keywords=[
                "mlflow", "kubeflow", "databricks ml", "experiment tracking",
                "pytorch", "tensorflow", "model deployment",
                "containerized", "docker", "kubernetes",
                "deep learning", "neural network", "machine learning",
                "ai platform", "cloud ml", "compute platform",
                "gpu", "ai workload", "model training",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.25,
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            keywords=[
                "jupyter", "notebooks", "manual deploy", "scikit-learn",
                "python", "r studio", "basic ml",
                "analytics platform", "data analytics", "algorithms",
                "predictive", "automation", "digital platform",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.15,
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "excel", "tableau only", "no ml", "basic bi",
                "spreadsheet", "power bi only",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.05,
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "manual", "no tools", "no analytics", "manual reporting",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
        ),
    },

    # ── Talent (talent_skills) ────────────────────────────────────────
    "talent_skills": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                "ml platform", "ai research", "large team", ">20 specialists",
                "ai leadership", "principal ml", "staff ml",
                "research scientist", "ai lab",
                "world-class talent", "ai talent", "engineering talent",
                "top talent", "ai researchers",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.40,
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            # keywords=[
            #     "data science team", "ml engineers", "10-20",
            #     "active hiring", "retention", "senior data scientist",
            #     "ml engineer", "data engineering team",
            #     "skilled workforce", "technical talent", "engineers",
            #     "scientists", "researchers", "specialized talent",
            #     "talent acquisition", "employee development",
            # ],
             keywords=[
                "data science team", "ml engineers", "10-20",
                "active hiring", "retention", "senior data scientist",
                "ml engineer", "data engineering team",
                "skilled workforce", "technical talent", "engineers",
                "scientists", "researchers", "specialized talent",
                "talent acquisition", "employee development",
                # 10-K workforce language
                "engineering team", "technology team",
                "research and development", "r&d",
                "technical expertise", "software engineers",
                "technology professionals", "it professionals",
                "talent pipeline", "technical training",
                "employee training", "upskilling",
                "professional development", "learning programs",
                "stem", "university recruiting",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.25,
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            # keywords=[
            #     "data scientist", "growing team", "3-10",
            #     "hiring data", "analytics team", "small team",
            #     "workforce", "employees", "human capital",
            #     "training programs", "talent development",
            # ],
              keywords=[
                "data scientist", "growing team", "3-10",
                "hiring data", "analytics team", "small team",
                "workforce", "employees", "human capital",
                "training programs", "talent development",
                # Generic 10-K workforce language
                "headcount", "staffing", "recruiting",
                "compensation", "benefits", "retention programs",
                "employee engagement", "workforce development",
                "full-time employees", "team members", "associates",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.15,
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "junior", "contractor", "turnover", "1-2 data",
                "limited technical", "outsourced",
                "labor shortage", "staffing challenges",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.05,
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "no data scientist", "vendor only", "no ai talent",
                "no ml", "fully outsourced",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
        ),
    },

    # ── Leadership (leadership_vision) ────────────────────────────────
    # THIS IS THE MOST CRITICAL FIX — was scoring "Nascent" for all
    # companies because keywords like "ceo ai" and "board committee"
    # never appear verbatim in MD&A text. SEC filings use different
    # language: "artificial intelligence", "strategic priority",
    # "growth strategy", "investment in technology", etc.
    "leadership_vision": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                # CS3 original
                "ceo ai", "board committee", "ai strategy",
                "ai strategic plan", "ceo champions",
                "multi-year ai", "ai transformation",
                # Real SEC filing language
                "artificial intelligence", "generative ai",
                "ai-driven", "ai infrastructure",
                "accelerated computing", "data center",
                "deep learning", "machine learning",
                "strategic investment", "growth strategy",
                "technology leadership", "innovation",
                "next-generation", "industry leader",
                "competitive advantage", "market leadership",
            ],
            min_keyword_matches=3,
            quantitative_threshold=0.40,
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            keywords=[
                # CS3 original
                "cto ai", "strategic priority", "c-suite",
                "cdo", "ai in strategy", "executive sponsor",
                "digital transformation",
                # Real SEC filing language
                "technology", "digital", "automation",
                "investment in technology", "technology investment",
                "strategic initiative", "modernization",
                "cloud", "platform", "transformation",
                "data-driven", "analytics",
                "competitive position", "market position",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.25,
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            keywords=[
                "vp sponsor", "department initiative", "vp-level",
                "departmental ai", "pilot program",
                "operational efficiency", "process improvement",
                "cost reduction", "technology systems",
                "information technology", "it investment",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.15,
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "it led", "limited awareness", "it-driven",
                "no executive", "limited sponsorship",
                "cost management", "maintaining systems",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.05,
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "no sponsor", "not discussed", "no ai discussion",
                "no strategy", "no technology mention",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
        ),
    },

    # ── Use Case Portfolio ────────────────────────────────────────────
    "use_case_portfolio": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                # CS3 original
                "production ai", "3x roi", "ai product",
                "5+ use cases", "revenue-generating ai",
                "ai at scale", "production deployment",
                # Real SEC filing language
                "ai-powered", "ai-enabled", "ai solutions",
                "ai products", "ai services", "ai platform",
                "ai applications", "ai capabilities",
                "deployed", "production", "revenue",
                "customers use", "ai-driven revenue",
                "generative ai", "large language model",
                "inference", "training",
            ],
            min_keyword_matches=3,
            quantitative_threshold=0.40,
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            # keywords=[
            #     "production", "measured roi", "scaling",
            #     "2-4 use cases", "deployed ai", "ai in production",
            #     "machine learning", "deep learning", "automation",
            #     "predictive analytics", "computer vision",
            #     "natural language", "recommendation",
            #     "supply chain optimization", "fraud detection",
            #     "customer experience", "operational efficiency",
            # ],
             keywords=[
                "production", "measured roi", "scaling",
                "2-4 use cases", "deployed ai", "ai in production",
                "machine learning", "deep learning", "automation",
                "predictive analytics", "computer vision",
                "natural language", "recommendation",
                "supply chain optimization", "fraud detection",
                "customer experience", "operational efficiency",
                # Industry-specific AI use cases
                "predictive maintenance", "demand forecasting",
                "inventory optimization", "route optimization",
                "credit risk model", "underwriting model",
                "claims processing", "chatbot", "virtual assistant",
                "anomaly detection", "quality control",
                "process automation", "robotic process",
                "intelligent automation", "sensor data",
                "iot", "connected", "additive manufacturing",
                "condition monitoring", "fleet management",
                "supply chain", "logistics",
                "pricing optimization", "dynamic pricing",
                "ecommerce", "omnichannel", "e-commerce",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.25,
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            # keywords=[
            #     "pilot", "early production", "proof of concept moving",
            #     "poc to production", "initial deployment",
            #     "exploring ai", "evaluating", "implementing",
            #     "digital transformation", "technology initiative",
            #     "analytics", "data analytics",
            # ],
             keywords=[
                "pilot", "early production", "proof of concept moving",
                "poc to production", "initial deployment",
                "exploring ai", "evaluating", "implementing",
                "digital transformation", "technology initiative",
                "analytics", "data analytics",
                # Industry language
                "modernization", "digitization", "digital strategy",
                "technology roadmap", "innovation program",
                "enterprise software", "erp", "crm",
                "mobile app", "digital services",
                "operational technology", "industrial internet",
                "smart factory", "industry 4.0",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.15,
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "poc", "proof of concept", "prototype",
                "testing", "experimental", "considering ai",
                "early stage", "investigating",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.05,
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "exploring", "no use cases", "no ai use",
                "considering", "no deployment", "no ai",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
        ),
    },

    # ── Culture (culture_change) ──────────────────────────────────────
    "culture_change": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                "innovative", "data-driven", "fail-fast",
                "experimentation culture", "innovation rewarded",
                "data culture", "growth mindset",
                "culture of innovation", "entrepreneurial",
                "cutting-edge", "forward-thinking",
            ],
            min_keyword_matches=3,
            quantitative_threshold=0.40,
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            keywords=[
                "experimental", "learning culture", "data literacy",
                "encourages experimentation", "open to innovation",
                "continuous improvement", "agile", "adaptive",
                "collaborative", "empowered", "dynamic",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.25,
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            keywords=[
                "open to change", "some resistance", "mixed",
                "evolving", "transitioning", "changing",
                "transformation", "culture change",
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.15,
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "bureaucratic", "resistant", "slow",
                "hierarchical", "traditional", "risk-averse",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.05,
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "hostile", "siloed", "no data culture",
                "change hostile", "completely resistant",
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
        ),
    },
}


# ---------------------------------------------------------------------------
# RubricScorer
# ---------------------------------------------------------------------------

class RubricScorer:
    """Score evidence against PE Org-AI-R rubrics."""

    def __init__(self):
        self.rubrics = DIMENSION_RUBRICS

    def score_dimension(
        self,
        dimension: str,
        evidence_text: str,
        quantitative_metrics: Optional[Dict[str, float]] = None,
    ) -> RubricResult:
        """
        Score a dimension using rubric matching.

        Algorithm:
          1. Normalize evidence text (lowercase)
          2. For each level (5 down to 1):
             a. Count keyword matches
             b. Check quantitative threshold
             c. If criteria met → interpolate score within level range
          3. Use keyword density to interpolate within range
        """
        if quantitative_metrics is None:
            quantitative_metrics = {}

        text = evidence_text.lower()
        rubric = self.rubrics.get(dimension, {})

        if not rubric:
            return RubricResult(
                dimension=dimension,
                level=ScoreLevel.LEVEL_3,
                score=Decimal("50.00"),
                matched_keywords=[],
                keyword_match_count=0,
                confidence=Decimal("0.300"),
                rationale=f"No rubric defined for dimension '{dimension}'",
            )

        primary_metric = self._get_primary_metric(dimension, quantitative_metrics)

        for level in [ScoreLevel.LEVEL_5, ScoreLevel.LEVEL_4, ScoreLevel.LEVEL_3,
                      ScoreLevel.LEVEL_2, ScoreLevel.LEVEL_1]:
            criteria = rubric.get(level)
            if not criteria:
                continue

            matches = [kw for kw in criteria.keywords if kw in text]
            match_count = len(matches)

            quant_met = primary_metric >= criteria.quantitative_threshold if primary_metric is not None else True

            if match_count >= criteria.min_keyword_matches and quant_met:
                score = self._interpolate_score(level, match_count, criteria)
                confidence = self._calculate_confidence(
                    match_count, criteria.min_keyword_matches,
                    len(criteria.keywords), primary_metric is not None
                )
                rationale = (
                    f"Matched {match_count}/{criteria.min_keyword_matches} required keywords "
                    f"for {level.label} ({level.min_score}-{level.max_score}). "
                    f"Keywords: {', '.join(matches[:5])}"
                )

                return RubricResult(
                    dimension=dimension,
                    level=level,
                    score=score,
                    matched_keywords=matches,
                    keyword_match_count=match_count,
                    confidence=confidence,
                    rationale=rationale,
                )

        return RubricResult(
            dimension=dimension,
            level=ScoreLevel.LEVEL_1,
            score=Decimal("10.00"),
            matched_keywords=[],
            keyword_match_count=0,
            confidence=Decimal("0.300"),
            rationale="No rubric criteria matched; defaulting to Level 1",
        )

    def score_all_dimensions(
        self,
        evidence_by_dimension: Dict[str, str],
        metrics_by_dimension: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> Dict[str, RubricResult]:
        """Score all 7 dimensions."""
        if metrics_by_dimension is None:
            metrics_by_dimension = {}

        results = {}
        for dim_name in self.rubrics:
            text = evidence_by_dimension.get(dim_name, "")
            metrics = metrics_by_dimension.get(dim_name, {})
            results[dim_name] = self.score_dimension(dim_name, text, metrics)
        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _interpolate_score(
        self,
        level: ScoreLevel,
        match_count: int,
        criteria: RubricCriteria,
    ) -> Decimal:
        """Interpolate score within a level's range based on keyword density."""
        min_s = level.min_score
        max_s = level.max_score
        range_size = max_s - min_s

        extra = match_count - criteria.min_keyword_matches
        max_extra = max(len(criteria.keywords) - criteria.min_keyword_matches, 1)

        ratio = min(extra / max_extra, 1.0)
        score = min_s + ratio * range_size
        score = max(min_s, min(max_s, score))

        return Decimal(str(round(score, 2)))

    def _calculate_confidence(
        self,
        match_count: int,
        min_matches: int,
        total_keywords: int,
        has_quant: bool,
    ) -> Decimal:
        """Calculate confidence of the rubric match."""
        base = 0.50
        match_ratio = match_count / max(total_keywords, 1)
        match_bonus = min(match_ratio * 0.30, 0.30)
        quant_bonus = 0.10 if has_quant else 0.0
        excess_bonus = min((match_count - min_matches) * 0.02, 0.10) if match_count > min_matches else 0.0

        conf = min(base + match_bonus + quant_bonus + excess_bonus, 0.95)
        return Decimal(str(round(conf, 3)))

    def _get_primary_metric(
        self,
        dimension: str,
        metrics: Dict[str, float],
    ) -> Optional[float]:
        """Get the primary quantitative metric for a dimension."""
        metric_map = {
            "talent_skills": ["ai_job_ratio", "ai_jobs", "team_size"],
            "technology_stack": ["ai_tech_ratio", "tech_count", "ai_tools"],
            "data_infrastructure": ["data_tech_count", "cloud_adoption"],
            "ai_governance": ["governance_score", "risk_mentions"],
            "leadership_vision": ["leadership_score", "exec_ai_mentions"],
            "use_case_portfolio": ["use_case_count", "production_ai_count"],
            "culture_change": ["culture_score", "innovation_mentions"],
        }

        preferred_keys = metric_map.get(dimension, [])
        for key in preferred_keys:
            if key in metrics:
                return metrics[key]

        if metrics:
            return next(iter(metrics.values()))
        return None