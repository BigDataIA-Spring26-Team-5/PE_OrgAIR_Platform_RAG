# app/routers/rag.py
"""RAG Router — FastAPI endpoints for CS4 RAG search and justification."""
from __future__ import annotations

import asyncio
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services.integration.cs1_client import CS1Client
from app.services.integration.cs2_client import CS2Client
from app.services.integration.cs3_client import CS3Client
from app.services.search.vector_store import VectorStore, EMBEDDING_MODEL
from app.services.retrieval.hybrid import HybridRetriever
from app.services.retrieval.dimension_mapper import DimensionMapper
from app.services.retrieval.hyde import HyDERetriever
from app.services.justification.generator import JustificationGenerator
from app.services.workflows.ic_prep import ICPrepWorkflow
from app.services.llm.router import ModelRouter
from app.prompts.rag_prompts import (
    DIM_DETECTION_SYSTEM,
    DIM_DETECTION_USER,
    CHATBOT_SYSTEM,
    CHATBOT_USER,
)
import structlog

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/rag", tags=["CS4 RAG"])

_vector_store: Optional[VectorStore] = None
_retriever: Optional[HybridRetriever] = None
_router_llm: Optional[ModelRouter] = None
_mapper: Optional[DimensionMapper] = None


def _get_vector_store() -> VectorStore:
    global _vector_store
    if _vector_store is None:
        _vector_store = VectorStore()
    return _vector_store


def _get_retriever() -> HybridRetriever:
    global _retriever
    if _retriever is None:
        _retriever = HybridRetriever()
    return _retriever


def _get_router() -> ModelRouter:
    global _router_llm
    if _router_llm is None:
        _router_llm = ModelRouter()
    return _router_llm


def _get_mapper() -> DimensionMapper:
    global _mapper
    if _mapper is None:
        _mapper = DimensionMapper()
    return _mapper


# ── Dimension → keyword query map ─────────────────────────────────────────────
DIMENSION_QUERY_MAP: Dict[str, str] = {
    "data_infrastructure": (
        "data platform cloud infrastructure pipeline snowflake databricks "
        "data quality lakehouse real-time API data catalog"
    ),
    "ai_governance": (
        "AI policy governance risk management compliance board committee "
        "CAIO CDO chief data officer model risk oversight"
    ),
    "technology_stack": (
        "machine learning MLOps GPU generative AI SageMaker MLflow "
        "deep learning PyTorch TensorFlow feature store model registry"
    ),
    "talent": (
        "AI engineers hiring machine learning data scientists talent "
        "ML platform team AI research staff retention"
    ),
    "leadership": (
        "CEO AI strategy executive AI investment roadmap CTO CDO "
        "board AI committee strategic priorities digital transformation"
    ),
    "use_case_portfolio": (
        "AI use cases production deployment ROI revenue AI products "
        "pilots proof of concept automation predictive analytics"
    ),
    "culture": (
        "innovation data-driven culture experimentation fail-fast "
        "agile learning change readiness digital culture"
    ),
}

DIMENSION_SOURCE_AFFINITY: Dict[str, List[str]] = {
    "data_infrastructure": ["sec_10k_item_1", "sec_10k_item_7"],
    "ai_governance":       ["sec_10k_item_1a", "board_proxy_def14a"],
    "technology_stack":    ["sec_10k_item_1", "sec_10k_item_7", "patent_uspto"],
    "talent":              ["job_posting_indeed", "job_posting_linkedin", "glassdoor_review"],
    "leadership":          ["sec_10k_item_7", "sec_10k_item_1a", "sec_10k_item_1", "board_proxy_def14a"],
    "use_case_portfolio":  ["sec_10k_item_1", "sec_10k_item_7"],
    "culture":             ["glassdoor_review", "sec_10k_item_1"],
}

# ── Dimension detection ────────────────────────────────────────────────────────
_DIMENSION_DISCRIMINATORS: Dict[str, set] = {
    "data_infrastructure": {"snowflake", "databricks", "lakehouse", "pipeline", "catalog", "ingestion"},
    "ai_governance":       {"governance", "compliance", "oversight", "policy", "caio", "cdo"},
    "technology_stack":    {"gpu", "mlops", "sagemaker", "mlflow", "pytorch", "tensorflow"},
    "talent":              {"hiring", "engineers", "scientists", "recruitment", "retention", "headcount"},
    "leadership":          {"ceo", "cto", "executive", "roadmap", "strategy"},
    "use_case_portfolio":  {"revenue", "roi", "pilots", "production", "automation"},
    "culture":             {"culture", "agile", "experimentation", "fail-fast"},
}

_COMMON_WORDS = {"ai", "the", "and", "for", "with", "model", "data", "digital", "learning", "board"}

_DIM_CONFIDENCE_THRESHOLD = 0.12

# ── LLM dimension detection ────────────────────────────────────────────────────
# Human-readable descriptions used as context for the LLM classifier.
# These are intentionally technology-agnostic so they work for any company —
# the LLM maps company-specific terms (CUDA, Hopper, H100, DGX) to dimensions
# without needing those terms in any keyword list.
_DIMENSION_DESCRIPTIONS: Dict[str, str] = {
    "data_infrastructure": (
        "Data platforms, cloud pipelines, databases, data lakes, ETL/ELT workflows, "
        "data quality, real-time streaming, data catalogs, storage architecture, "
        "lakehouse (e.g. Snowflake, Databricks, BigQuery, Redshift)"
    ),
    "ai_governance": (
        "AI policy and ethics, regulatory compliance, export controls, risk management "
        "frameworks, board AI committee, model risk oversight, responsible AI, CAIO/CDO roles, "
        "government regulations affecting AI products"
    ),
    "technology_stack": (
        "ML/AI frameworks and tools, GPU/hardware for AI, software SDKs/APIs/libraries, "
        "developer platforms, MLOps tooling, CUDA/PyTorch/TensorFlow, model training "
        "infrastructure, proprietary AI platforms, technology architecture"
    ),
    "talent": (
        "AI/ML hiring and job postings, data scientist recruitment, engineer headcount, "
        "talent pipeline, workforce skills, employee retention for technical roles, "
        "internship programs, AI research staff"
    ),
    "leadership": (
        "CEO/CTO/CDO statements on AI strategy, board-level AI priorities, executive "
        "investment roadmap, digital transformation direction, strategic AI commitments, "
        "management discussion of AI direction (MD&A)"
    ),
    "use_case_portfolio": (
        "Specific AI products deployed in production, commercial AI applications, "
        "AI revenue streams, business automation use cases, proof-of-concept to production "
        "deployments, named product lines or platforms generating revenue from AI"
    ),
    "culture": (
        "Innovation culture, data-driven mindset, employee reviews of work environment, "
        "agile/experimental culture, change readiness, Glassdoor feedback, "
        "internal collaboration norms, fail-fast mentality"
    ),
}


def _detect_dimension_with_llm(
    question: str,
    router: ModelRouter,
) -> tuple[Optional[str], float]:
    """LLM-assisted dimension detection for company-specific or ambiguous queries.

    Called when keyword scoring confidence is below _DIM_CONFIDENCE_THRESHOLD.
    Uses Groq (fast, cheap — "keyword_matching" task) to map any question to one
    of the 7 CS3 dimensions by semantic understanding, not keyword matching.

    This handles company-specific terminology that keyword matching misses:
      - "What CUDA products does NVDA have?" → technology_stack
      - "What AI products does NVDA have in production?" → use_case_portfolio
      - "What is GOOGL's Gemini strategy?" → use_case_portfolio
      - "How does NVDA use Hopper architecture?" → technology_stack

    Returns (dimension_key, 0.75) on success.
    Returns (None, 0.0) on any failure — caller falls back gracefully.
    The 0.75 confidence is intentionally below 1.0 to signal LLM-detected
    (vs caller-supplied dimension = 1.0) but above threshold to trigger enrichment.
    """
    valid_dims = set(_DIMENSION_DESCRIPTIONS.keys())
    dim_list = "\n".join(
        f"  {dim}: {desc}"
        for dim, desc in _DIMENSION_DESCRIPTIONS.items()
    )
    messages = [
        {
            "role": "system",
            "content": DIM_DETECTION_SYSTEM.format(
                valid_dims="\n".join(f"  {d}" for d in valid_dims)
            ),
        },
        {
            "role": "user",
            "content": DIM_DETECTION_USER.format(
                question=question,
                dim_list=dim_list,
            ),
        },
    ]
    try:
        # "keyword_matching" routes to Groq — fast and cheap, appropriate for
        # this classification step (not a quality-critical IC output)
        raw = router.complete("keyword_matching", messages)
        detected = raw.strip().lower().replace('"', "").replace("'", "").split()[0]
        if detected in valid_dims:
            logger.info("rag.llm_dim_detected", question=question[:80], dimension=detected)
            return detected, 0.75
        # LLM returned something unexpected — log and fall back
        logger.warning(
            "rag.llm_dim_invalid_response",
            question=question[:80],
            raw_response=raw[:100],
        )
        return None, 0.0
    except Exception as e:
        logger.warning("rag.llm_dim_detection_failed", question=question[:80], error=str(e))
        return None, 0.0


def _detect_dimension_scored(question: str) -> tuple[Optional[str], float]:
    """
    Weighted dimension detection.

    Priority order:
    1. Gap/weakness questions → ai_governance (Item 1A risk factors)
    2. Talent/hiring questions → talent (job postings)
    3. Score justification questions → extract dimension from question text
    4. Broad readiness/overall/strengths → use_case_portfolio
    5. Weighted keyword overlap across all 7 dimensions
    6. No strong signal → (None, 0.0) — caller should invoke LLM fallback
    """
    q_lower = question.lower()
    q_words = set(q_lower.split())

    # Priority 1: gap/weakness/risk questions → Item 1A (Risk Factors)
    _GAP_TRIGGERS = {
        "gaps", "gap", "weaknesses", "weakness", "risks", "risk",
        "missing", "lacking", "improve", "improvement", "challenge",
        "challenges", "concerns", "concern", "shortcoming", "shortcomings",
        "threats", "threat", "competitive", "competition",
    }
    if q_words & _GAP_TRIGGERS:
        return "ai_governance", 0.30

    # Priority 2: talent/hiring questions → talent dimension → job postings
    _TALENT_TRIGGERS = {
        "talent", "hiring", "hire", "recruitment", "engineers", "employees",
        "workforce", "headcount", "jobs", "job", "postings", "roles",
        "data scientists", "ml engineers", "staff", "team",
    }
    if q_words & _TALENT_TRIGGERS:
        return "talent", 0.35

    # Priority 3: score justification questions → route to correct dimension
    _SCORE_TRIGGERS = {"score", "scored", "scoring", "why", "justify", "justification"}
    if q_words & _SCORE_TRIGGERS:
        for dim in DIMENSION_QUERY_MAP:
            dim_words = set(dim.replace("_", " ").lower().split())
            if dim_words & q_words:
                return dim, 0.50

    # Priority 4: broad assessment questions → use_case_portfolio
    _BROAD_TRIGGERS = {
        "overall", "readiness", "assessment", "strengths", "strength",
        "summary", "overview", "evaluate", "evaluation", "prepare",
        "investment", "committee", "score", "rating", "general",
    }
    if q_words & _BROAD_TRIGGERS:
        return "use_case_portfolio", 0.20

    # Priority 5: weighted keyword overlap
    best_dim: Optional[str] = None
    best_score: float = 0.0

    for dim, keywords_str in DIMENSION_QUERY_MAP.items():
        kw_tokens = keywords_str.lower().split()
        discriminators = _DIMENSION_DISCRIMINATORS.get(dim, set())
        raw = 0.0
        for token in kw_tokens:
            if token not in q_words:
                continue
            if token in discriminators:
                raw += 3.0
            elif token in _COMMON_WORDS:
                raw += 0.3
            else:
                raw += 1.0
        normalised = raw / max(len(kw_tokens), 1)
        if normalised > best_score:
            best_score = normalised
            best_dim = dim

    if best_score < 0.05:
        return None, 0.0
    return best_dim, round(best_score, 4)


# ── Filter builder ─────────────────────────────────────────────────────────────

def _build_filter(
    ticker: str,
    dimension: Optional[str] = None,
    source_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build ChromaDB where-clause. ticker is always applied."""
    conditions: List[Dict] = [{"ticker": ticker}]

    if dimension and dimension not in ("string", ""):
        conditions.append({"dimension": dimension})

    if source_types:
        valid = [s for s in source_types if s and s != "string"]
        if valid:
            conditions.append({"source_type": {"$in": valid}})

    if len(conditions) == 1:
        return conditions[0]
    return {"$and": conditions}


# ── Retrieval with fallback ────────────────────────────────────────────────────

async def _retrieve_with_fallback(
    retriever: HybridRetriever,
    query: str,
    ticker: str,
    dimension: Optional[str],
    top_k: int,
    min_results: int = 3,
    dim_confidence: float = 1.0,
) -> List:
    """
    Retrieve with graceful dimension-filter fallback.

    Steps:
    1. Dimension-filtered search (enriched query only if dim_confidence is high)
    2. Source-affinity fallback if too few results — always prefers SEC sources
       for governance/leadership/use_case_portfolio dimensions
    3. Ticker-only with raw query as final fallback

    SEC source priority: for non-culture, non-talent dimensions, SEC filings
    are always tried before falling back to ticker-only. This prevents
    Glassdoor reviews from dominating broad/gap questions.
    """
    # Talent dimension — always force job postings, bypass dimension filter
    if dimension == "talent":
        talent_query = (
            "machine learning AI engineer data scientist MLOps deep learning "
            "generative AI LLM hiring " + query
        )
        filter_jobs = _build_filter(
            ticker,
            source_types=["job_posting_indeed", "job_posting_linkedin"],
        )
        job_results = retriever.retrieve(talent_query, k=top_k, filter_metadata=filter_jobs)
        if len(job_results) >= min_results:
            return job_results
        filter_jobs_gd = _build_filter(
            ticker,
            source_types=["job_posting_indeed", "job_posting_linkedin", "glassdoor_review"],
        )
        job_results = retriever.retrieve(talent_query, k=top_k, filter_metadata=filter_jobs_gd)
        if len(job_results) >= min_results:
            return job_results

    # Culture dimension — always go to Glassdoor first
    if dimension == "culture":
        filter_culture = _build_filter(ticker, source_types=["glassdoor_review"])
        culture_results = retriever.retrieve(query, k=top_k, filter_metadata=filter_culture)
        if len(culture_results) >= min_results:
            return culture_results

    results = []

    # SEC-authoritative dimensions — always try SEC sources first
    _SEC_PRIMARY_DIMS = {
        "data_infrastructure", "ai_governance", "technology_stack",
        "leadership", "use_case_portfolio",
    }

    if dimension:
        if dim_confidence >= _DIM_CONFIDENCE_THRESHOLD:
            dim_keywords = DIMENSION_QUERY_MAP.get(dimension, "")
            enriched_query = (dim_keywords + " " + query).strip()
        else:
            enriched_query = query

        filter_with_dim = _build_filter(ticker, dimension=dimension)
        results = retriever.retrieve(enriched_query, k=top_k, filter_metadata=filter_with_dim)

        if len(results) >= min_results:
            return results

        logger.info(
            "rag.fallback_triggered",
            ticker=ticker,
            dimension=dimension,
            dim_confidence=dim_confidence,
            dim_results=len(results),
            reason="too_few_dim_results",
        )

        # Step 2a: For SEC-primary dimensions, force SEC source affinity
        if dimension in _SEC_PRIMARY_DIMS:
            sec_sources = [
                "sec_10k_item_1", "sec_10k_item_1a",
                "sec_10k_item_7", "board_proxy_def14a",
            ]
            filter_sec = _build_filter(ticker, source_types=sec_sources)
            sec_results = retriever.retrieve(enriched_query, k=top_k, filter_metadata=filter_sec)
            if len(sec_results) > len(results):
                results = sec_results
            if len(results) >= min_results:
                return results

        # Step 2b: General source affinity fallback
        affinity_sources = DIMENSION_SOURCE_AFFINITY.get(dimension, [])
        if affinity_sources:
            filter_src = _build_filter(ticker, source_types=affinity_sources)
            src_results = retriever.retrieve(enriched_query, k=top_k, filter_metadata=filter_src)
            if len(src_results) > len(results):
                results = src_results

        if len(results) >= min_results:
            return results

    # Step 3: ticker-only — use raw query, no dimension pollution
    filter_ticker_only = _build_filter(ticker)
    fallback = retriever.retrieve(query, k=top_k, filter_metadata=filter_ticker_only)

    # If dimension is SEC-primary and fallback returns non-SEC, force SEC
    if dimension in _SEC_PRIMARY_DIMS and fallback:
        sec_fallback = [
            r for r in fallback
            if r.metadata.get("source_type", "").startswith("sec_")
            or r.metadata.get("source_type", "") == "board_proxy_def14a"
        ]
        if len(sec_fallback) >= min_results:
            return sec_fallback

    return fallback if len(fallback) > len(results) else results


# ── Request / Response Models ─────────────────────────────────────────────────

class IndexRequest(BaseModel):
    source_types: Optional[List[str]] = None
    signal_categories: Optional[List[str]] = None
    min_confidence: float = 0.0


class IndexResponse(BaseModel):
    indexed_count: int
    ticker: str
    source_counts: Dict[str, int] = {}


class BulkIndexRequest(BaseModel):
    tickers: List[str]
    source_types: Optional[List[str]] = None
    signal_categories: Optional[List[str]] = None
    min_confidence: float = 0.0
    force: bool = False


class BulkIndexResponse(BaseModel):
    results: Dict[str, IndexResponse]
    total_indexed: int
    failed: Dict[str, str]


class SearchRequest(BaseModel):
    query: str
    ticker: Optional[str] = None
    source_types: Optional[List[str]] = None
    dimension: Optional[str] = None
    top_k: int = 10
    use_hyde: bool = False


class SearchResult(BaseModel):
    doc_id: str
    content: str
    metadata: Dict[str, Any]
    score: float
    retrieval_method: str


class JustifyResponse(BaseModel):
    ticker: str
    dimension: str
    score: float
    level: int
    level_name: str
    generated_summary: str
    evidence_strength: str
    supporting_evidence: List[Dict[str, Any]]
    gaps_identified: List[str]


class ICPrepResponse(BaseModel):
    company_id: str
    ticker: str
    executive_summary: str
    recommendation: str
    key_strengths: List[str]
    key_gaps: List[str]
    risk_factors: List[str]
    dimension_scores: Dict[str, float]
    total_evidence_count: int
    generated_at: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/index/{ticker}", response_model=IndexResponse)
async def index_company_evidence(
    ticker: str,
    source_types: Optional[str] = Query(None),
    signal_categories: Optional[str] = Query(None),
    min_confidence: float = Query(0.0),
    force: bool = Query(False),
):
    """Fetch CS2 evidence for a company and index into ChromaDB."""
    logger.info("rag.index_start", ticker=ticker, force=force)
    cs2 = CS2Client()
    vs = _get_vector_store()
    mapper = _get_mapper()

    if force:
        st_list = [s.strip() for s in source_types.split(",")] if source_types else None
        where: dict = {"ticker": {"$eq": ticker}}
        if st_list:
            where = {"$and": [where, {"source_type": {"$in": st_list}}]}
        vs.delete_by_filter(where)

    evidence = cs2.get_evidence(
        ticker=ticker,
        source_types=[s.strip() for s in source_types.split(",")] if source_types else None,
        signal_categories=[s.strip() for s in signal_categories.split(",")] if signal_categories else None,
        min_confidence=min_confidence,
    )

    from collections import defaultdict
    source_counts: Dict[str, int] = defaultdict(int)
    for e in evidence:
        source_counts[e.signal_category] += 1

    count = vs.index_cs2_evidence(evidence, mapper)
    if evidence:
        cs2.mark_indexed([e.evidence_id for e in evidence])

    _get_retriever().refresh_sparse_index()
    _get_retriever().seed_from_evidence(evidence)

    logger.info("rag.index_complete", ticker=ticker, indexed_count=count)
    return IndexResponse(indexed_count=count, ticker=ticker, source_counts=dict(source_counts))


@router.post("/index", response_model=BulkIndexResponse)
async def bulk_index_evidence(req: BulkIndexRequest):
    """Index CS2 evidence for multiple tickers in a single call."""
    logger.info("rag.bulk_index_start", tickers=req.tickers)
    cs2 = CS2Client()
    vs = _get_vector_store()
    mapper = _get_mapper()

    from collections import defaultdict
    results: Dict[str, IndexResponse] = {}
    failed: Dict[str, str] = {}
    all_evidence = []

    for ticker in req.tickers:
        try:
            if req.force:
                where: dict = {"ticker": {"$eq": ticker}}
                if req.source_types:
                    where = {"$and": [where, {"source_type": {"$in": req.source_types}}]}
                vs.delete_by_filter(where)

            evidence = cs2.get_evidence(
                ticker=ticker,
                source_types=req.source_types,
                signal_categories=req.signal_categories,
                min_confidence=req.min_confidence,
            )

            source_counts: Dict[str, int] = defaultdict(int)
            for e in evidence:
                source_counts[e.signal_category] += 1

            count = vs.index_cs2_evidence(evidence, mapper)
            if evidence:
                cs2.mark_indexed([e.evidence_id for e in evidence])
                all_evidence.extend(evidence)

            results[ticker] = IndexResponse(
                indexed_count=count, ticker=ticker,
                source_counts=dict(source_counts),
            )
        except Exception as e:
            failed[ticker] = str(e)
            logger.warning("rag.bulk_index_ticker_error", ticker=ticker, error=str(e))

    _get_retriever().refresh_sparse_index()
    if all_evidence:
        _get_retriever().seed_from_evidence(all_evidence)

    total_indexed = sum(r.indexed_count for r in results.values())
    return BulkIndexResponse(results=results, total_indexed=total_indexed, failed=failed)


@router.delete("/index")
async def wipe_index(ticker: Optional[str] = Query(None)):
    """Delete documents from the ChromaDB index."""
    vs = _get_vector_store()
    if ticker:
        wiped = vs.delete_by_filter({"ticker": {"$eq": ticker}})
    else:
        wiped = vs.wipe()
        _get_retriever().refresh_sparse_index()
    return {"wiped_count": wiped, "scope": ticker if ticker else "all"}


@router.post("/search", response_model=List[SearchResult])
async def search_evidence(req: SearchRequest):
    """Hybrid dense + sparse search with optional HyDE enhancement."""
    logger.info("rag.search_start", query_len=len(req.query), ticker=req.ticker)
    retriever = _get_retriever()

    source_types = None
    if req.source_types:
        source_types = [s for s in req.source_types if s and s != "string"]

    if req.use_hyde and req.dimension:
        filter_meta = _build_filter(
            req.ticker or "",
            dimension=req.dimension,
            source_types=source_types,
        ) if req.ticker else {}
        llm_router = _get_router()
        hyde = HyDERetriever(retriever, llm_router)
        results = hyde.retrieve(
            req.query, k=req.top_k,
            filters=filter_meta or None,
            dimension=req.dimension or "",
        )
    elif req.ticker:
        results = await _retrieve_with_fallback(
            retriever=retriever,
            query=req.query,
            ticker=req.ticker,
            dimension=req.dimension if req.dimension and req.dimension != "string" else None,
            top_k=req.top_k,
        )
    else:
        filter_meta: Dict[str, Any] = {}
        if source_types:
            filter_meta["source_type"] = {"$in": source_types}
        if req.dimension and req.dimension != "string":
            filter_meta["dimension"] = req.dimension
        results = retriever.retrieve(
            req.query, k=req.top_k,
            filter_metadata=filter_meta or None,
        )

    logger.info("rag.search_complete", result_count=len(results))
    return [
        SearchResult(
            doc_id=r.doc_id,
            content=r.content[:500],
            metadata=r.metadata,
            score=r.score,
            retrieval_method=r.retrieval_method,
        )
        for r in results
    ]


@router.get("/justify/{ticker}/{dimension}", response_model=JustifyResponse)
async def justify_score(ticker: str, dimension: str):
    """Generate IC-ready justification for a dimension score with cited evidence."""
    logger.info("rag.justify_start", ticker=ticker, dimension=dimension)
    retriever = _get_retriever()
    llm_router = _get_router()
    gen = JustificationGenerator(retriever=retriever, router=llm_router)

    try:
        j = await asyncio.to_thread(gen.generate_justification, ticker, dimension)
    except Exception as e:
        logger.error("rag.justify_error", ticker=ticker, dimension=dimension, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

    return JustifyResponse(
        ticker=j.company_id,
        dimension=j.dimension,
        score=j.score,
        level=j.level,
        level_name=j.level_name,
        generated_summary=j.generated_summary,
        evidence_strength=j.evidence_strength,
        supporting_evidence=[
            {
                "evidence_id": e.evidence_id,
                "content": e.content,
                "source_type": e.source_type,
                "source_url": e.source_url,
                "confidence": e.confidence,
                "matched_keywords": e.matched_keywords,
                "relevance_score": e.relevance_score,
            }
            for e in j.supporting_evidence[:5]
        ],
        gaps_identified=j.gaps_identified,
    )


@router.get("/ic-prep/{ticker}", response_model=ICPrepResponse)
async def ic_prep(ticker: str, dimensions: Optional[str] = Query(None)):
    """Generate full 7-dimension IC meeting package with recommendation."""
    focus = [d.strip() for d in dimensions.split(",")] if dimensions else None
    logger.info("rag.ic_prep_start", ticker=ticker, focus_dimensions=focus)
    workflow = ICPrepWorkflow()
    try:
        pkg = await workflow.prepare_meeting(ticker, focus_dimensions=focus)
    except Exception as e:
        logger.error("rag.ic_prep_error", ticker=ticker, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

    dim_scores = {dim: j.score for dim, j in pkg.dimension_justifications.items()}
    return ICPrepResponse(
        company_id=pkg.company.company_id,
        ticker=pkg.company.ticker,
        executive_summary=pkg.executive_summary,
        recommendation=pkg.recommendation,
        key_strengths=pkg.key_strengths,
        key_gaps=pkg.key_gaps,
        risk_factors=pkg.risk_factors,
        dimension_scores=dim_scores,
        total_evidence_count=pkg.total_evidence_count,
        generated_at=pkg.generated_at,
    )


@router.get("/status")
async def rag_status():
    """Returns ChromaDB index stats and system status."""
    vs = _get_vector_store()
    indexed = vs.count()
    return {
        "status": "operational",
        "indexed_documents": indexed,
        "vector_store": "ChromaDB",
        "embedding_model": EMBEDDING_MODEL,
        "llm_providers": ["groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"],
    }


@router.get("/debug")
async def rag_debug(
    ticker: Optional[str] = Query(None),
    limit: int = Query(10, le=100),
):
    """Show ChromaDB contents via search."""
    vs = _get_vector_store()
    total = vs.count()

    if total == 0:
        return {"total": 0, "by_ticker": {}, "by_source_type": {}, "sample": []}

    try:
        results = vs.search(
            query="AI machine learning data infrastructure technology",
            top_k=limit,
            ticker=ticker,
        )
        docs = [
            {
                "id": r.doc_id,
                "ticker": r.metadata.get("ticker"),
                "source_type": r.metadata.get("source_type"),
                "signal_category": r.metadata.get("signal_category"),
                "dimension": r.metadata.get("dimension"),
                "confidence": r.metadata.get("confidence"),
                "content_preview": r.content[:200],
            }
            for r in results
        ]
        from collections import Counter
        ticker_counts = Counter(r.metadata.get("ticker", "unknown") for r in results)
        source_counts = Counter(r.metadata.get("source_type", "unknown") for r in results)
        return {
            "total": total,
            "by_ticker": dict(ticker_counts),
            "by_source_type": dict(source_counts),
            "sample": docs,
        }
    except Exception as e:
        return {"total": total, "error": str(e), "sample": []}


@router.get("/debug/evidence/{ticker}")
async def debug_evidence(ticker: str):
    """Debug what cs2_client.get_evidence() returns — verify SEC chunks loading."""
    cs2 = CS2Client()
    evidence = cs2.get_evidence(ticker=ticker)
    from collections import Counter
    by_cat = Counter(e.signal_category for e in evidence)
    by_source = Counter(e.source_type for e in evidence)
    return {
        "total": len(evidence),
        "by_signal_category": dict(by_cat),
        "by_source_type": dict(by_source),
        "sample_sec": [
            {
                "id": e.evidence_id,
                "source": e.source_type,
                "content": e.content[:150],
            }
            for e in evidence
            if "sec" in e.source_type or "proxy" in e.source_type
        ][:5],
    }


@router.get("/chatbot/{ticker}")
async def chatbot_query(
    ticker: str,
    question: str = Query(...),
    dimension: Optional[str] = Query(None),
    use_hyde: bool = Query(False),
):
    """
    Answer a question about a company using RAG.

    Dimension detection pipeline (in order):
    1. Caller-supplied dimension param → used directly (confidence=1.0)
    2. Keyword scoring via _detect_dimension_scored() — fast, zero latency
    3. LLM fallback via _detect_dimension_with_llm() — if keyword confidence
       is below threshold OR returns None. Handles company-specific terms
       (CUDA, Hopper, H100, DGX, Gemini, etc.) that keyword matching misses.
    4. No dimension → ticker-only retrieval with raw query
    """
    logger.info("rag.chatbot_query", ticker=ticker, question_len=len(question))
    retriever = _get_retriever()
    llm_router = _get_router()

    detected_dimension = dimension
    dim_confidence = 1.0

    if not detected_dimension:
        detected_dimension, dim_confidence = _detect_dimension_scored(question)

        # LLM fallback: keyword scoring returned no result or low-confidence result
        # This handles company-specific terminology (CUDA, H100, DGX, Gemini, etc.)
        # that doesn't appear in generic keyword lists
        if detected_dimension is None or dim_confidence < _DIM_CONFIDENCE_THRESHOLD:
            llm_dim, llm_conf = _detect_dimension_with_llm(question, llm_router)
            if llm_dim is not None:
                detected_dimension = llm_dim
                dim_confidence = llm_conf
                logger.info(
                    "rag.chatbot_used_llm_dim",
                    ticker=ticker,
                    dimension=detected_dimension,
                    confidence=dim_confidence,
                )

    logger.info(
        "rag.chatbot_dim_final",
        ticker=ticker,
        dimension=detected_dimension,
        confidence=dim_confidence,
    )

    results = await _retrieve_with_fallback(
        retriever=retriever,
        query=question,
        ticker=ticker,
        dimension=detected_dimension,
        top_k=8,
        min_results=3,
        dim_confidence=dim_confidence,
    )

    if not results:
        return {
            "answer": (
                f"No evidence found for {ticker}. "
                "Please run the indexing pipeline first via POST /rag/index/{ticker}."
            ),
            "evidence": [],
            "sources_used": 0,
            "ticker": ticker,
            "dimension_detected": detected_dimension,
            "dim_confidence": dim_confidence,
        }

    results_sorted = sorted(results, key=lambda r: r.score, reverse=True)
    context_parts = []
    for r in results_sorted[:6]:
        src = r.metadata.get("source_type", "unknown")
        dim = r.metadata.get("dimension", "")
        fy = r.metadata.get("fiscal_year", "")
        label = (
            f"[{src}"
            + (f", {fy}" if fy else "")
            + (f", dim={dim}" if dim else "")
            + "]"
        )
        context_parts.append(f"{label}\n{r.content[:600]}")

    context = "\n\n---\n\n".join(context_parts)

    dim_instruction = ""
    if detected_dimension and dim_confidence >= _DIM_CONFIDENCE_THRESHOLD:
        dim_label = detected_dimension.replace("_", " ").title()
        dim_instruction = f" Focus your answer on the {dim_label} dimension of AI readiness."

    messages = [
        {
            "role": "system",
            "content": CHATBOT_SYSTEM + dim_instruction,
        },
        {
            "role": "user",
            "content": CHATBOT_USER.format(
                ticker=ticker,
                context=context,
                question=question,
            ),
        },
    ]

    try:
        raw_answer = llm_router.complete("chat_response", messages)
        if isinstance(raw_answer, str):
            answer = raw_answer
        elif hasattr(raw_answer, "choices"):
            answer = raw_answer.choices[0].message.content
        else:
            answer = str(raw_answer)
    except Exception as e:
        logger.error("rag.chatbot_llm_error", ticker=ticker, error=str(e))
        answer = f"Evidence retrieved but could not generate answer: {e}"

    return {
        "answer": answer,
        "evidence": [
            {
                "source_type": r.metadata.get("source_type"),
                "dimension": r.metadata.get("dimension"),
                "fiscal_year": r.metadata.get("fiscal_year"),
                "content": r.content[:300],
                "score": round(r.score, 4),
            }
            for r in results_sorted[:4]
        ],
        "sources_used": len(results),
        "dimension_detected": detected_dimension,
        "dim_confidence": dim_confidence,
        "ticker": ticker,
    }