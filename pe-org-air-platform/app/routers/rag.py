"""RAG Router — FastAPI endpoints for CS4 RAG search and justification."""
from __future__ import annotations

import asyncio
from typing import List, Optional, Dict, Any, Union
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

router = APIRouter(prefix="/rag", tags=["CS4 RAG"])

# Singletons (lazy-initialized per request for simplicity)
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
    failed: Dict[str, str]  # ticker → error message


class SearchRequest(BaseModel):
    query: str
    ticker: Optional[str] = None
    source_types: Optional[List[str]] = None  # e.g. ["patent_uspto"]
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

@router.post("/index/{ticker}", response_model=IndexResponse, summary="Index company evidence into ChromaDB")
async def index_company_evidence(
    ticker: str,
    source_types: Optional[str] = Query(None, description="Comma-separated source types to filter (e.g. job_posting_indeed,patent_uspto)"),
    signal_categories: Optional[str] = Query(None, description="Comma-separated signal categories to filter (e.g. technology_hiring,innovation_activity)"),
    min_confidence: float = Query(0.0, description="Minimum confidence score (0.0–1.0)"),
    force: bool = Query(False, description="Delete existing docs for this ticker+source_types before re-indexing"),
):
    """Fetch CS2 evidence for a company and index into ChromaDB. All filters are optional."""
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
    return IndexResponse(indexed_count=count, ticker=ticker, source_counts=dict(source_counts))


@router.post("/index", response_model=BulkIndexResponse, summary="Bulk index multiple tickers into ChromaDB")
async def bulk_index_evidence(req: BulkIndexRequest):
    """Index CS2 evidence for multiple tickers in a single call. Continues on per-ticker errors."""
    cs2 = CS2Client()
    vs = _get_vector_store()
    mapper = _get_mapper()

    from collections import defaultdict
    results: Dict[str, IndexResponse] = {}
    failed: Dict[str, str] = {}

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

            results[ticker] = IndexResponse(
                indexed_count=count,
                ticker=ticker,
                source_counts=dict(source_counts),
            )
        except Exception as e:
            failed[ticker] = str(e)

    _get_retriever().refresh_sparse_index()

    total_indexed = sum(r.indexed_count for r in results.values())
    return BulkIndexResponse(results=results, total_indexed=total_indexed, failed=failed)


@router.delete("/index", summary="Wipe ChromaDB index (all or single ticker)")
async def wipe_index(
    ticker: Optional[str] = Query(None, description="If set, wipe only this ticker's documents; otherwise wipe all"),
):
    """Delete documents from the ChromaDB index. Omit ticker to wipe everything."""
    vs = _get_vector_store()

    if ticker:
        wiped = vs.delete_by_filter({"ticker": {"$eq": ticker}})
        scope = ticker
    else:
        wiped = vs.wipe()
        scope = "all"
        # Reset sparse index since all documents are gone
        _get_retriever().refresh_sparse_index()

    return {"wiped_count": wiped, "scope": scope}


@router.post("/search", response_model=List[SearchResult], summary="Hybrid search over indexed evidence")
async def search_evidence(req: SearchRequest):
    """Hybrid dense + sparse search with optional HyDE enhancement."""
    retriever = _get_retriever()

    filter_meta: Dict[str, Any] = {}
    if req.ticker:
        filter_meta["ticker"] = req.ticker
    if req.source_types:
        valid_types = [s for s in req.source_types if s and s != "string"]
        if valid_types:
            filter_meta["source_type"] = valid_types

    if req.dimension and req.dimension != "string":
        filter_meta["dimension"] = req.dimension

    if req.use_hyde and req.dimension:
        llm_router = _get_router()
        hyde = HyDERetriever(retriever, llm_router)
        results = hyde.retrieve(
            req.query,
            k=req.top_k,
            filters=filter_meta or None,
            dimension=req.dimension or "",
        )
    else:
        results = retriever.retrieve(
            req.query,
            k=req.top_k,
            filter_metadata=filter_meta or None,
        )

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


@router.get(
    "/justify/{ticker}/{dimension}",
    response_model=JustifyResponse,
    summary="Generate cited score justification",
)
async def justify_score(ticker: str, dimension: str):
    """Generate IC-ready justification for a dimension score with cited evidence."""
    retriever = _get_retriever()
    llm_router = _get_router()
    gen = JustificationGenerator(retriever=retriever, router=llm_router)

    try:
        j = await asyncio.to_thread(gen.generate_justification, ticker, dimension)
    except Exception as e:
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


@router.get(
    "/ic-prep/{ticker}",
    response_model=ICPrepResponse,
    summary="Generate full IC meeting preparation package",
)
async def ic_prep(
    ticker: str,
    dimensions: Optional[str] = Query(
        None,
        description="Comma-separated list of dimensions to include (default: all 7)",
    ),
):
    """Generate full 7-dimension IC meeting package with recommendation."""
    focus = [d.strip() for d in dimensions.split(",")] if dimensions else None
    workflow = ICPrepWorkflow()
    try:
        pkg = await asyncio.to_thread(workflow.prepare_meeting, ticker, focus_dimensions=focus)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    dim_scores = {
        dim: j.score for dim, j in pkg.dimension_justifications.items()
    }

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


@router.get("/status", summary="RAG system status")
async def rag_status():
    """Returns ChromaDB index stats and system status."""
    vs = _get_vector_store()
    return {
        "status": "operational",
        "indexed_documents": vs.count(),
        "vector_store": "ChromaDB",
        "embedding_model": EMBEDDING_MODEL,
        "llm_providers": ["groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"],
    }


@router.get("/debug", summary="Inspect ChromaDB contents")
async def rag_debug(
    ticker: Optional[str] = Query(None, description="Filter by ticker"),
    limit: int = Query(10, description="Max documents to return", le=100),
):
    """Show raw ChromaDB documents with metadata — useful for verifying indexing."""
    vs = _get_vector_store()
    if vs._collection is None:
        return {"total": 0, "documents": []}

    total = vs._collection.count()
    if total == 0:
        return {"total": 0, "documents": []}

    where = {"ticker": {"$eq": ticker}} if ticker else None
    kwargs: Dict[str, Any] = {
        "limit": limit,
        "include": ["documents", "metadatas"],
    }
    if where:
        kwargs["where"] = where

    try:
        result = vs._collection.get(**kwargs)
    except Exception as e:
        return {"total": total, "error": str(e), "documents": []}

    docs = []
    for doc_id, doc, meta in zip(result["ids"], result["documents"], result["metadatas"]):
        docs.append({
            "id": doc_id,
            "ticker": meta.get("ticker"),
            "source_type": meta.get("source_type"),
            "signal_category": meta.get("signal_category"),
            "dimension": meta.get("dimension"),
            "confidence": meta.get("confidence"),
            "content_preview": doc[:200],
        })

    # Breakdown by ticker and source_type
    from collections import Counter
    all_meta = vs._collection.get(include=["metadatas"])["metadatas"]
    ticker_counts = Counter(m.get("ticker", "unknown") for m in all_meta)
    source_counts = Counter(m.get("source_type", "unknown") for m in all_meta)

    return {
        "total": total,
        "by_ticker": dict(ticker_counts),
        "by_source_type": dict(source_counts),
        "sample": docs,
    }
