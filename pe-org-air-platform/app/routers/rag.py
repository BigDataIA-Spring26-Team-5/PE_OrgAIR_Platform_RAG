# """RAG Router — FastAPI endpoints for CS4 RAG search and justification."""
# from __future__ import annotations

# import asyncio
# from typing import List, Optional, Dict, Any, Union
# from fastapi import APIRouter, HTTPException, Query
# from pydantic import BaseModel

# from app.services.integration.cs1_client import CS1Client
# from app.services.integration.cs2_client import CS2Client
# from app.services.integration.cs3_client import CS3Client
# from app.services.search.vector_store import VectorStore, EMBEDDING_MODEL
# from app.services.retrieval.hybrid import HybridRetriever
# from app.services.retrieval.dimension_mapper import DimensionMapper
# from app.services.retrieval.hyde import HyDERetriever
# from app.services.justification.generator import JustificationGenerator
# from app.services.workflows.ic_prep import ICPrepWorkflow
# from app.services.llm.router import ModelRouter
# import structlog

# logger = structlog.get_logger(__name__)

# router = APIRouter(prefix="/rag", tags=["CS4 RAG"])

# # Singletons (lazy-initialized per request for simplicity)
# _vector_store: Optional[VectorStore] = None
# _retriever: Optional[HybridRetriever] = None
# _router_llm: Optional[ModelRouter] = None
# _mapper: Optional[DimensionMapper] = None


# def _get_vector_store() -> VectorStore:
#     global _vector_store
#     if _vector_store is None:
#         _vector_store = VectorStore()
#     return _vector_store


# def _get_retriever() -> HybridRetriever:
#     global _retriever
#     if _retriever is None:
#         _retriever = HybridRetriever()
#     return _retriever


# def _get_router() -> ModelRouter:
#     global _router_llm
#     if _router_llm is None:
#         _router_llm = ModelRouter()
#     return _router_llm


# def _get_mapper() -> DimensionMapper:
#     global _mapper
#     if _mapper is None:
#         _mapper = DimensionMapper()
#     return _mapper


# # ── Request / Response Models ─────────────────────────────────────────────────

# class IndexRequest(BaseModel):
#     source_types: Optional[List[str]] = None
#     signal_categories: Optional[List[str]] = None
#     min_confidence: float = 0.0


# class IndexResponse(BaseModel):
#     indexed_count: int
#     ticker: str
#     source_counts: Dict[str, int] = {}


# class BulkIndexRequest(BaseModel):
#     tickers: List[str]
#     source_types: Optional[List[str]] = None
#     signal_categories: Optional[List[str]] = None
#     min_confidence: float = 0.0
#     force: bool = False


# class BulkIndexResponse(BaseModel):
#     results: Dict[str, IndexResponse]
#     total_indexed: int
#     failed: Dict[str, str]  # ticker → error message


# class SearchRequest(BaseModel):
#     query: str
#     ticker: Optional[str] = None
#     source_types: Optional[List[str]] = None  # e.g. ["patent_uspto"]
#     dimension: Optional[str] = None
#     top_k: int = 10
#     use_hyde: bool = False


# class SearchResult(BaseModel):
#     doc_id: str
#     content: str
#     metadata: Dict[str, Any]
#     score: float
#     retrieval_method: str


# class JustifyResponse(BaseModel):
#     ticker: str
#     dimension: str
#     score: float
#     level: int
#     level_name: str
#     generated_summary: str
#     evidence_strength: str
#     supporting_evidence: List[Dict[str, Any]]
#     gaps_identified: List[str]


# class ICPrepResponse(BaseModel):
#     company_id: str
#     ticker: str
#     executive_summary: str
#     recommendation: str
#     key_strengths: List[str]
#     key_gaps: List[str]
#     risk_factors: List[str]
#     dimension_scores: Dict[str, float]
#     total_evidence_count: int
#     generated_at: str


# # ── Endpoints ─────────────────────────────────────────────────────────────────

# @router.post("/index/{ticker}", response_model=IndexResponse, summary="Index company evidence into ChromaDB")
# async def index_company_evidence(
#     ticker: str,
#     source_types: Optional[str] = Query(None, description="Comma-separated source types to filter (e.g. job_posting_indeed,patent_uspto)"),
#     signal_categories: Optional[str] = Query(None, description="Comma-separated signal categories to filter (e.g. technology_hiring,innovation_activity)"),
#     min_confidence: float = Query(0.0, description="Minimum confidence score (0.0–1.0)"),
#     force: bool = Query(False, description="Delete existing docs for this ticker+source_types before re-indexing"),
# ):
#     """Fetch CS2 evidence for a company and index into ChromaDB. All filters are optional."""
#     logger.info(
#         "rag.index_start",
#         ticker=ticker,
#         force=force,
#         source_types=source_types,
#         signal_categories=signal_categories,
#     )
#     cs2 = CS2Client()
#     vs = _get_vector_store()
#     mapper = _get_mapper()

#     if force:
#         st_list = [s.strip() for s in source_types.split(",")] if source_types else None
#         where: dict = {"ticker": {"$eq": ticker}}
#         if st_list:
#             where = {"$and": [where, {"source_type": {"$in": st_list}}]}
#         vs.delete_by_filter(where)

#     evidence = cs2.get_evidence(
#         ticker=ticker,
#         source_types=[s.strip() for s in source_types.split(",")] if source_types else None,
#         signal_categories=[s.strip() for s in signal_categories.split(",")] if signal_categories else None,
#         min_confidence=min_confidence,
#     )

#     from collections import defaultdict
#     source_counts: Dict[str, int] = defaultdict(int)
#     for e in evidence:
#         source_counts[e.signal_category] += 1

#     count = vs.index_cs2_evidence(evidence, mapper)
#     if evidence:
#         cs2.mark_indexed([e.evidence_id for e in evidence])
#     _get_retriever().refresh_sparse_index()
#     logger.info("rag.index_complete", ticker=ticker, indexed_count=count, source_counts=dict(source_counts))
#     return IndexResponse(indexed_count=count, ticker=ticker, source_counts=dict(source_counts))


# @router.post("/index", response_model=BulkIndexResponse, summary="Bulk index multiple tickers into ChromaDB")
# async def bulk_index_evidence(req: BulkIndexRequest):
#     """Index CS2 evidence for multiple tickers in a single call. Continues on per-ticker errors."""
#     logger.info("rag.bulk_index_start", tickers=req.tickers, force=req.force)
#     cs2 = CS2Client()
#     vs = _get_vector_store()
#     mapper = _get_mapper()

#     from collections import defaultdict
#     results: Dict[str, IndexResponse] = {}
#     failed: Dict[str, str] = {}

#     for ticker in req.tickers:
#         try:
#             if req.force:
#                 where: dict = {"ticker": {"$eq": ticker}}
#                 if req.source_types:
#                     where = {"$and": [where, {"source_type": {"$in": req.source_types}}]}
#                 vs.delete_by_filter(where)

#             evidence = cs2.get_evidence(
#                 ticker=ticker,
#                 source_types=req.source_types,
#                 signal_categories=req.signal_categories,
#                 min_confidence=req.min_confidence,
#             )

#             source_counts: Dict[str, int] = defaultdict(int)
#             for e in evidence:
#                 source_counts[e.signal_category] += 1

#             count = vs.index_cs2_evidence(evidence, mapper)
#             if evidence:
#                 cs2.mark_indexed([e.evidence_id for e in evidence])

#             results[ticker] = IndexResponse(
#                 indexed_count=count,
#                 ticker=ticker,
#                 source_counts=dict(source_counts),
#             )
#             logger.info("rag.bulk_index_ticker_complete", ticker=ticker, indexed_count=count)
#         except Exception as e:
#             failed[ticker] = str(e)
#             logger.warning("rag.bulk_index_ticker_error", ticker=ticker, error=str(e))

#     _get_retriever().refresh_sparse_index()

#     total_indexed = sum(r.indexed_count for r in results.values())
#     logger.info("rag.bulk_index_complete", total_indexed=total_indexed, failed_count=len(failed))
#     return BulkIndexResponse(results=results, total_indexed=total_indexed, failed=failed)


# @router.delete("/index", summary="Wipe ChromaDB index (all or single ticker)")
# async def wipe_index(
#     ticker: Optional[str] = Query(None, description="If set, wipe only this ticker's documents; otherwise wipe all"),
# ):
#     """Delete documents from the ChromaDB index. Omit ticker to wipe everything."""
#     scope = ticker if ticker else "all"
#     logger.info("rag.wipe_start", scope=scope)
#     vs = _get_vector_store()

#     if ticker:
#         wiped = vs.delete_by_filter({"ticker": {"$eq": ticker}})
#     else:
#         wiped = vs.wipe()
#         # Reset sparse index since all documents are gone
#         _get_retriever().refresh_sparse_index()

#     logger.info("rag.wipe_complete", scope=scope, wiped_count=wiped)
#     return {"wiped_count": wiped, "scope": scope}


# @router.post("/search", response_model=List[SearchResult], summary="Hybrid search over indexed evidence")
# async def search_evidence(req: SearchRequest):
#     """Hybrid dense + sparse search with optional HyDE enhancement."""
#     logger.info(
#         "rag.search_start",
#         query_len=len(req.query),
#         ticker=req.ticker,
#         use_hyde=req.use_hyde,
#         dimension=req.dimension,
#         top_k=req.top_k,
#     )
#     retriever = _get_retriever()

#     filter_meta: Dict[str, Any] = {}
#     if req.ticker:
#         filter_meta["ticker"] = req.ticker
#     if req.source_types:
#         valid_types = [s for s in req.source_types if s and s != "string"]
#         if valid_types:
#             filter_meta["source_type"] = valid_types

#     if req.dimension and req.dimension != "string":
#         filter_meta["dimension"] = req.dimension

#     if req.use_hyde and req.dimension:
#         llm_router = _get_router()
#         hyde = HyDERetriever(retriever, llm_router)
#         results = hyde.retrieve(
#             req.query,
#             k=req.top_k,
#             filters=filter_meta or None,
#             dimension=req.dimension or "",
#         )
#     else:
#         results = retriever.retrieve(
#             req.query,
#             k=req.top_k,
#             filter_metadata=filter_meta or None,
#         )

#     top_score = results[0].score if results else 0.0
#     logger.info("rag.search_complete", result_count=len(results), top_score=top_score)
#     return [
#         SearchResult(
#             doc_id=r.doc_id,
#             content=r.content[:500],
#             metadata=r.metadata,
#             score=r.score,
#             retrieval_method=r.retrieval_method,
#         )
#         for r in results
#     ]


# @router.get(
#     "/justify/{ticker}/{dimension}",
#     response_model=JustifyResponse,
#     summary="Generate cited score justification",
# )
# async def justify_score(ticker: str, dimension: str):
#     """Generate IC-ready justification for a dimension score with cited evidence."""
#     logger.info("rag.justify_start", ticker=ticker, dimension=dimension)
#     retriever = _get_retriever()
#     llm_router = _get_router()
#     gen = JustificationGenerator(retriever=retriever, router=llm_router)

#     try:
#         j = await asyncio.to_thread(gen.generate_justification, ticker, dimension)
#     except Exception as e:
#         logger.error("rag.justify_error", ticker=ticker, dimension=dimension, error=str(e))
#         raise HTTPException(status_code=500, detail=str(e))

#     logger.info(
#         "rag.justify_complete",
#         ticker=ticker,
#         dimension=dimension,
#         score=j.score,
#         level=j.level,
#         evidence_count=len(j.supporting_evidence),
#         gaps_count=len(j.gaps_identified),
#     )
#     return JustifyResponse(
#         ticker=j.company_id,
#         dimension=j.dimension,
#         score=j.score,
#         level=j.level,
#         level_name=j.level_name,
#         generated_summary=j.generated_summary,
#         evidence_strength=j.evidence_strength,
#         supporting_evidence=[
#             {
#                 "evidence_id": e.evidence_id,
#                 "content": e.content,
#                 "source_type": e.source_type,
#                 "source_url": e.source_url,
#                 "confidence": e.confidence,
#                 "matched_keywords": e.matched_keywords,
#                 "relevance_score": e.relevance_score,
#             }
#             for e in j.supporting_evidence[:5]
#         ],
#         gaps_identified=j.gaps_identified,
#     )


# @router.get(
#     "/ic-prep/{ticker}",
#     response_model=ICPrepResponse,
#     summary="Generate full IC meeting preparation package",
# )
# async def ic_prep(
#     ticker: str,
#     dimensions: Optional[str] = Query(
#         None,
#         description="Comma-separated list of dimensions to include (default: all 7)",
#     ),
# ):
#     """Generate full 7-dimension IC meeting package with recommendation."""
#     focus = [d.strip() for d in dimensions.split(",")] if dimensions else None
#     logger.info("rag.ic_prep_start", ticker=ticker, focus_dimensions=focus)
#     workflow = ICPrepWorkflow()
#     try:
#         # pkg = await asyncio.to_thread(workflow.prepare_meeting, ticker, focus_dimensions=focus)
#         pkg = await workflow.prepare_meeting(ticker, focus_dimensions=focus)
#     except Exception as e:
#         logger.error("rag.ic_prep_error", ticker=ticker, error=str(e))
#         raise HTTPException(status_code=500, detail=str(e))

#     dim_scores = {
#         dim: j.score for dim, j in pkg.dimension_justifications.items()
#     }
#     logger.info(
#         "rag.ic_prep_complete",
#         ticker=ticker,
#         recommendation=pkg.recommendation,
#         dim_count=len(dim_scores),
#         evidence_count=pkg.total_evidence_count,
#     )
#     return ICPrepResponse(
#         company_id=pkg.company.company_id,
#         ticker=pkg.company.ticker,
#         executive_summary=pkg.executive_summary,
#         recommendation=pkg.recommendation,
#         key_strengths=pkg.key_strengths,
#         key_gaps=pkg.key_gaps,
#         risk_factors=pkg.risk_factors,
#         dimension_scores=dim_scores,
#         total_evidence_count=pkg.total_evidence_count,
#         generated_at=pkg.generated_at,
#     )


# @router.get("/status", summary="RAG system status")
# async def rag_status():
#     """Returns ChromaDB index stats and system status."""
#     vs = _get_vector_store()
#     indexed = vs.count()
#     logger.info("rag.status_checked", indexed_documents=indexed)
#     return {
#         "status": "operational",
#         "indexed_documents": indexed,
#         "vector_store": "ChromaDB",
#         "embedding_model": EMBEDDING_MODEL,
#         "llm_providers": ["groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"],
#     }


# @router.get("/debug", summary="Inspect ChromaDB contents")
# async def rag_debug(
#     ticker: Optional[str] = Query(None, description="Filter by ticker"),
#     limit: int = Query(10, description="Max documents to return", le=100),
# ):
#     """Show raw ChromaDB documents with metadata — useful for verifying indexing."""
#     vs = _get_vector_store()
#     if vs._collection is None:
#         return {"total": 0, "documents": []}

#     total = vs._collection.count()
#     if total == 0:
#         return {"total": 0, "documents": []}

#     where = {"ticker": {"$eq": ticker}} if ticker else None
#     kwargs: Dict[str, Any] = {
#         "limit": limit,
#         "include": ["documents", "metadatas"],
#     }
#     if where:
#         kwargs["where"] = where

#     try:
#         result = vs._collection.get(**kwargs)
#     except Exception as e:
#         return {"total": total, "error": str(e), "documents": []}

#     docs = []
#     for doc_id, doc, meta in zip(result["ids"], result["documents"], result["metadatas"]):
#         docs.append({
#             "id": doc_id,
#             "ticker": meta.get("ticker"),
#             "source_type": meta.get("source_type"),
#             "signal_category": meta.get("signal_category"),
#             "dimension": meta.get("dimension"),
#             "confidence": meta.get("confidence"),
#             "content_preview": doc[:200],
#         })

#     # Breakdown by ticker and source_type
#     from collections import Counter
#     all_meta = vs._collection.get(include=["metadatas"])["metadatas"]
#     ticker_counts = Counter(m.get("ticker", "unknown") for m in all_meta)
#     source_counts = Counter(m.get("source_type", "unknown") for m in all_meta)

#     logger.info("rag.debug_queried", ticker=ticker, limit=limit, total=total, returned_count=len(docs))
#     return {
#         "total": total,
#         "by_ticker": dict(ticker_counts),
#         "by_source_type": dict(source_counts),
#         "sample": docs,
#     }

# """RAG Router — FastAPI endpoints for CS4 RAG search and justification."""
# from __future__ import annotations

# import asyncio
# from typing import List, Optional, Dict, Any, Union
# from fastapi import APIRouter, HTTPException, Query
# from pydantic import BaseModel

# from app.services.integration.cs1_client import CS1Client
# from app.services.integration.cs2_client import CS2Client
# from app.services.integration.cs3_client import CS3Client
# from app.services.search.vector_store import VectorStore, EMBEDDING_MODEL
# from app.services.retrieval.hybrid import HybridRetriever
# from app.services.retrieval.dimension_mapper import DimensionMapper
# from app.services.retrieval.hyde import HyDERetriever
# from app.services.justification.generator import JustificationGenerator
# from app.services.workflows.ic_prep import ICPrepWorkflow
# from app.services.llm.router import ModelRouter
# import structlog

# logger = structlog.get_logger(__name__)

# router = APIRouter(prefix="/rag", tags=["CS4 RAG"])

# # Singletons (lazy-initialized per request for simplicity)
# _vector_store: Optional[VectorStore] = None
# _retriever: Optional[HybridRetriever] = None
# _router_llm: Optional[ModelRouter] = None
# _mapper: Optional[DimensionMapper] = None


# def _get_vector_store() -> VectorStore:
#     global _vector_store
#     if _vector_store is None:
#         _vector_store = VectorStore()
#     return _vector_store


# def _get_retriever() -> HybridRetriever:
#     global _retriever
#     if _retriever is None:
#         _retriever = HybridRetriever()
#     return _retriever


# def _get_router() -> ModelRouter:
#     global _router_llm
#     if _router_llm is None:
#         _router_llm = ModelRouter()
#     return _router_llm


# def _get_mapper() -> DimensionMapper:
#     global _mapper
#     if _mapper is None:
#         _mapper = DimensionMapper()
#     return _mapper


# # ── Request / Response Models ─────────────────────────────────────────────────

# class IndexRequest(BaseModel):
#     source_types: Optional[List[str]] = None
#     signal_categories: Optional[List[str]] = None
#     min_confidence: float = 0.0


# class IndexResponse(BaseModel):
#     indexed_count: int
#     ticker: str
#     source_counts: Dict[str, int] = {}


# class BulkIndexRequest(BaseModel):
#     tickers: List[str]
#     source_types: Optional[List[str]] = None
#     signal_categories: Optional[List[str]] = None
#     min_confidence: float = 0.0
#     force: bool = False


# class BulkIndexResponse(BaseModel):
#     results: Dict[str, IndexResponse]
#     total_indexed: int
#     failed: Dict[str, str]


# class SearchRequest(BaseModel):
#     query: str
#     ticker: Optional[str] = None
#     source_types: Optional[List[str]] = None
#     dimension: Optional[str] = None
#     top_k: int = 10
#     use_hyde: bool = False


# class SearchResult(BaseModel):
#     doc_id: str
#     content: str
#     metadata: Dict[str, Any]
#     score: float
#     retrieval_method: str


# class JustifyResponse(BaseModel):
#     ticker: str
#     dimension: str
#     score: float
#     level: int
#     level_name: str
#     generated_summary: str
#     evidence_strength: str
#     supporting_evidence: List[Dict[str, Any]]
#     gaps_identified: List[str]


# class ICPrepResponse(BaseModel):
#     company_id: str
#     ticker: str
#     executive_summary: str
#     recommendation: str
#     key_strengths: List[str]
#     key_gaps: List[str]
#     risk_factors: List[str]
#     dimension_scores: Dict[str, float]
#     total_evidence_count: int
#     generated_at: str


# # ── Endpoints ─────────────────────────────────────────────────────────────────

# @router.post("/index/{ticker}", response_model=IndexResponse, summary="Index company evidence into ChromaDB")
# async def index_company_evidence(
#     ticker: str,
#     source_types: Optional[str] = Query(None),
#     signal_categories: Optional[str] = Query(None),
#     min_confidence: float = Query(0.0),
#     force: bool = Query(False),
# ):
#     """Fetch CS2 evidence for a company and index into ChromaDB."""
#     logger.info("rag.index_start", ticker=ticker, force=force)
#     cs2 = CS2Client()
#     vs = _get_vector_store()
#     mapper = _get_mapper()

#     if force:
#         st_list = [s.strip() for s in source_types.split(",")] if source_types else None
#         where: dict = {"ticker": {"$eq": ticker}}
#         if st_list:
#             where = {"$and": [where, {"source_type": {"$in": st_list}}]}
#         vs.delete_by_filter(where)

#     evidence = cs2.get_evidence(
#         ticker=ticker,
#         source_types=[s.strip() for s in source_types.split(",")] if source_types else None,
#         signal_categories=[s.strip() for s in signal_categories.split(",")] if signal_categories else None,
#         min_confidence=min_confidence,
#     )

#     from collections import defaultdict
#     source_counts: Dict[str, int] = defaultdict(int)
#     for e in evidence:
#         source_counts[e.signal_category] += 1

#     count = vs.index_cs2_evidence(evidence, mapper)
#     if evidence:
#         cs2.mark_indexed([e.evidence_id for e in evidence])
#     _get_retriever().refresh_sparse_index()

#     logger.info("rag.index_complete", ticker=ticker, indexed_count=count)
#     return IndexResponse(indexed_count=count, ticker=ticker, source_counts=dict(source_counts))


# @router.post("/index", response_model=BulkIndexResponse, summary="Bulk index multiple tickers")
# async def bulk_index_evidence(req: BulkIndexRequest):
#     """Index CS2 evidence for multiple tickers in a single call."""
#     logger.info("rag.bulk_index_start", tickers=req.tickers)
#     cs2 = CS2Client()
#     vs = _get_vector_store()
#     mapper = _get_mapper()

#     from collections import defaultdict
#     results: Dict[str, IndexResponse] = {}
#     failed: Dict[str, str] = {}

#     for ticker in req.tickers:
#         try:
#             if req.force:
#                 where: dict = {"ticker": {"$eq": ticker}}
#                 if req.source_types:
#                     where = {"$and": [where, {"source_type": {"$in": req.source_types}}]}
#                 vs.delete_by_filter(where)

#             evidence = cs2.get_evidence(
#                 ticker=ticker,
#                 source_types=req.source_types,
#                 signal_categories=req.signal_categories,
#                 min_confidence=req.min_confidence,
#             )

#             source_counts: Dict[str, int] = defaultdict(int)
#             for e in evidence:
#                 source_counts[e.signal_category] += 1

#             count = vs.index_cs2_evidence(evidence, mapper)
#             if evidence:
#                 cs2.mark_indexed([e.evidence_id for e in evidence])

#             results[ticker] = IndexResponse(
#                 indexed_count=count, ticker=ticker,
#                 source_counts=dict(source_counts),
#             )
#         except Exception as e:
#             failed[ticker] = str(e)
#             logger.warning("rag.bulk_index_ticker_error", ticker=ticker, error=str(e))

#     _get_retriever().refresh_sparse_index()
#     total_indexed = sum(r.indexed_count for r in results.values())
#     return BulkIndexResponse(results=results, total_indexed=total_indexed, failed=failed)


# @router.delete("/index", summary="Wipe ChromaDB index")
# async def wipe_index(
#     ticker: Optional[str] = Query(None),
# ):
#     """Delete documents from the ChromaDB index."""
#     vs = _get_vector_store()
#     if ticker:
#         wiped = vs.delete_by_filter({"ticker": {"$eq": ticker}})
#     else:
#         wiped = vs.wipe()
#         _get_retriever().refresh_sparse_index()
#     return {"wiped_count": wiped, "scope": ticker if ticker else "all"}


# @router.post("/search", response_model=List[SearchResult], summary="Hybrid search over indexed evidence")
# async def search_evidence(req: SearchRequest):
#     """Hybrid dense + sparse search with optional HyDE enhancement."""
#     logger.info("rag.search_start", query_len=len(req.query), ticker=req.ticker)
#     retriever = _get_retriever()

#     filter_meta: Dict[str, Any] = {}
#     if req.ticker:
#         filter_meta["ticker"] = req.ticker
#     if req.source_types:
#         valid_types = [s for s in req.source_types if s and s != "string"]
#         if valid_types:
#             filter_meta["source_type"] = valid_types
#     if req.dimension and req.dimension != "string":
#         filter_meta["dimension"] = req.dimension

#     if req.use_hyde and req.dimension:
#         llm_router = _get_router()
#         hyde = HyDERetriever(retriever, llm_router)
#         results = hyde.retrieve(
#             req.query, k=req.top_k,
#             filters=filter_meta or None,
#             dimension=req.dimension or "",
#         )
#     else:
#         results = retriever.retrieve(
#             req.query, k=req.top_k,
#             filter_metadata=filter_meta or None,
#         )

#     logger.info("rag.search_complete", result_count=len(results))
#     return [
#         SearchResult(
#             doc_id=r.doc_id,
#             content=r.content[:500],
#             metadata=r.metadata,
#             score=r.score,
#             retrieval_method=r.retrieval_method,
#         )
#         for r in results
#     ]


# @router.get("/justify/{ticker}/{dimension}", response_model=JustifyResponse,
#             summary="Generate cited score justification")
# async def justify_score(ticker: str, dimension: str):
#     """Generate IC-ready justification for a dimension score with cited evidence."""
#     logger.info("rag.justify_start", ticker=ticker, dimension=dimension)
#     retriever = _get_retriever()
#     llm_router = _get_router()
#     gen = JustificationGenerator(retriever=retriever, router=llm_router)

#     try:
#         j = await asyncio.to_thread(gen.generate_justification, ticker, dimension)
#     except Exception as e:
#         logger.error("rag.justify_error", ticker=ticker, dimension=dimension, error=str(e))
#         raise HTTPException(status_code=500, detail=str(e))

#     return JustifyResponse(
#         ticker=j.company_id,
#         dimension=j.dimension,
#         score=j.score,
#         level=j.level,
#         level_name=j.level_name,
#         generated_summary=j.generated_summary,
#         evidence_strength=j.evidence_strength,
#         supporting_evidence=[
#             {
#                 "evidence_id": e.evidence_id,
#                 "content": e.content,
#                 "source_type": e.source_type,
#                 "source_url": e.source_url,
#                 "confidence": e.confidence,
#                 "matched_keywords": e.matched_keywords,
#                 "relevance_score": e.relevance_score,
#             }
#             for e in j.supporting_evidence[:5]
#         ],
#         gaps_identified=j.gaps_identified,
#     )


# @router.get("/ic-prep/{ticker}", response_model=ICPrepResponse,
#             summary="Generate full IC meeting preparation package")
# async def ic_prep(
#     ticker: str,
#     dimensions: Optional[str] = Query(None),
# ):
#     """Generate full 7-dimension IC meeting package with recommendation."""
#     focus = [d.strip() for d in dimensions.split(",")] if dimensions else None
#     logger.info("rag.ic_prep_start", ticker=ticker, focus_dimensions=focus)
#     workflow = ICPrepWorkflow()
#     try:
#         pkg = await workflow.prepare_meeting(ticker, focus_dimensions=focus)
#     except Exception as e:
#         logger.error("rag.ic_prep_error", ticker=ticker, error=str(e))
#         raise HTTPException(status_code=500, detail=str(e))

#     dim_scores = {dim: j.score for dim, j in pkg.dimension_justifications.items()}
#     return ICPrepResponse(
#         company_id=pkg.company.company_id,
#         ticker=pkg.company.ticker,
#         executive_summary=pkg.executive_summary,
#         recommendation=pkg.recommendation,
#         key_strengths=pkg.key_strengths,
#         key_gaps=pkg.key_gaps,
#         risk_factors=pkg.risk_factors,
#         dimension_scores=dim_scores,
#         total_evidence_count=pkg.total_evidence_count,
#         generated_at=pkg.generated_at,
#     )


# @router.get("/status", summary="RAG system status")
# async def rag_status():
#     """Returns ChromaDB index stats and system status."""
#     vs = _get_vector_store()
#     indexed = vs.count()
#     return {
#         "status": "operational",
#         "indexed_documents": indexed,
#         "vector_store": "ChromaDB",
#         "embedding_model": EMBEDDING_MODEL,
#         "llm_providers": ["groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"],
#     }


# @router.get("/debug", summary="Inspect ChromaDB contents")
# async def rag_debug(
#     ticker: Optional[str] = Query(None, description="Filter by ticker"),
#     limit: int = Query(10, description="Max documents to return", le=100),
# ):
#     """
#     Show ChromaDB contents via search.
#     Uses search endpoint instead of direct collection access
#     since VectorStore now uses HTTP API (no _collection attribute).
#     """
#     vs = _get_vector_store()
#     total = vs.count()

#     if total == 0:
#         return {"total": 0, "by_ticker": {}, "by_source_type": {}, "sample": []}

#     # Use search to get sample documents for the ticker
#     try:
#         results = vs.search(
#             query="AI machine learning data infrastructure technology",
#             top_k=limit,
#             ticker=ticker,
#         )

#         docs = [
#             {
#                 "id": r.doc_id,
#                 "ticker": r.metadata.get("ticker"),
#                 "source_type": r.metadata.get("source_type"),
#                 "signal_category": r.metadata.get("signal_category"),
#                 "dimension": r.metadata.get("dimension"),
#                 "confidence": r.metadata.get("confidence"),
#                 "content_preview": r.content[:200],
#             }
#             for r in results
#         ]

#         # Count by ticker and source type from results
#         from collections import Counter
#         ticker_counts = Counter(r.metadata.get("ticker", "unknown") for r in results)
#         source_counts = Counter(r.metadata.get("source_type", "unknown") for r in results)

#         return {
#             "total": total,
#             "by_ticker": dict(ticker_counts),
#             "by_source_type": dict(source_counts),
#             "sample": docs,
#         }
#     except Exception as e:
#         return {"total": total, "error": str(e), "sample": []}


# @router.get("/chatbot/{ticker}", summary="Simple chatbot Q&A for a company")
# async def chatbot_query(
#     ticker: str,
#     question: str = Query(..., description="Question to ask about the company"),
#     use_hyde: bool = Query(False, description="Use HyDE query enhancement"),
# ):
#     """
#     Answer a question about a company using RAG.
#     Used by the Streamlit chatbot interface.
#     """
#     logger.info("rag.chatbot_query", ticker=ticker, question_len=len(question))
#     retriever = _get_retriever()
#     llm_router = _get_router()

#     # Search for relevant evidence
#     filter_meta = {"ticker": ticker}
#     if use_hyde:
#         hyde = HyDERetriever(retriever, llm_router)
#         results = hyde.retrieve(question, k=5, filters=filter_meta)
#     else:
#         results = retriever.retrieve(question, k=5, filter_metadata=filter_meta)

#     if not results:
#         return {
#             "answer": f"No evidence found for {ticker}. Please run the pipeline first.",
#             "evidence": [],
#             "ticker": ticker,
#         }

#     # Build context from results
#     context = "\n\n".join([
#         f"[{r.metadata.get('source_type', 'unknown')}] {r.content[:500]}"
#         for r in results[:5]
#     ])

#     # Generate answer using LLM
#     messages = [
#         {
#             "role": "system",
#             "content": (
#                 "You are a PE investment analyst. Answer questions about companies "
#                 "based ONLY on the provided evidence. Be specific and cite sources. "
#                 "If evidence is insufficient, say so explicitly."
#             ),
#         },
#         {
#             "role": "user",
#             "content": (
#                 f"Company: {ticker}\n\n"
#                 f"Evidence:\n{context}\n\n"
#                 f"Question: {question}\n\n"
#                 "Answer in 2-3 sentences with specific citations:"
#             ),
#         },
#     ]

#     try:
#         answer = llm_router.complete("chat_response", messages)
#     except Exception as e:
#         answer = f"Could not generate answer: {e}"

#     return {
#         "answer": answer,
#         "evidence": [
#             {
#                 "source_type": r.metadata.get("source_type"),
#                 "content": r.content[:300],
#                 "score": round(r.score, 3),
#                 "dimension": r.metadata.get("dimension"),
#             }
#             for r in results[:3]
#         ],
#         "ticker": ticker,
#     }

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
import structlog

logger = structlog.get_logger(__name__)

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


# ── Dimension → keyword query map ─────────────────────────────────────────────
# Used to build better chatbot queries when the user asks a vague question.
# Maps CS3 dimension names to high-signal search terms from the CS3 rubrics.
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

# Source types that are most useful for each dimension — used for fallback
# retrieval when dimension-filtered search returns too few results.
DIMENSION_SOURCE_AFFINITY: Dict[str, List[str]] = {
    "data_infrastructure": ["sec_10k_item_1", "sec_10k_item_7"],
    "ai_governance":       ["board_proxy_def14a", "sec_10k_item_1a"],
    "technology_stack":    ["sec_10k_item_1", "sec_10k_item_7", "patent_uspto"],
    "talent":              ["job_posting_linkedin", "job_posting_indeed", "sec_10k_item_1"],
    "leadership":          ["sec_10k_item_7", "sec_10k_item_1", "board_proxy_def14a"],
    "use_case_portfolio":  ["sec_10k_item_1", "sec_10k_item_7"],
    "culture":             ["glassdoor_review", "sec_10k_item_1"],
}


def _build_filter(
    ticker: str,
    dimension: Optional[str] = None,
    source_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Build a ChromaDB where-clause that works regardless of whether chunks were
    indexed with 'ticker' or 'company_id' as the key.

    Strategy:
    - Always include ticker filter (stored as 'ticker' in our VectorStore)
    - Optionally add dimension filter
    - Optionally add source_type filter
    - Never apply dimension filter if it would return an empty result set
      (caller must check and fall back)
    """
    # Our VectorStore.index_cs2_evidence stores metadata with key 'ticker'
    # (set from evidence.company_id which is the ticker symbol in our CS2Client)
    where: Dict[str, Any] = {"ticker": ticker}

    conditions = [where]

    if dimension and dimension not in ("string", ""):
        conditions.append({"dimension": dimension})

    if source_types:
        valid = [s for s in source_types if s and s != "string"]
        if valid:
            conditions.append({"source_type": {"$in": valid}})

    if len(conditions) == 1:
        return conditions[0]
    return {"$and": conditions}


async def _retrieve_with_fallback(
    retriever: HybridRetriever,
    query: str,
    ticker: str,
    dimension: Optional[str],
    top_k: int,
    min_results: int = 3,
) -> List:
    """
    Retrieve with graceful dimension-filter fallback.

    1. Try dimension-filtered search first
    2. If results < min_results, try source-type-affinity search (no dim filter)
    3. If still < min_results, try ticker-only search with enriched query
    """
    # Step 1: dimension-filtered search
    if dimension:
        enriched_query = DIMENSION_QUERY_MAP.get(dimension, query) + " " + query
        filter_with_dim = _build_filter(ticker, dimension=dimension)
        results = retriever.retrieve(enriched_query, k=top_k, filter_metadata=filter_with_dim)
        if len(results) >= min_results:
            return results

        logger.info(
            "rag.fallback_triggered",
            ticker=ticker,
            dimension=dimension,
            dim_results=len(results),
            reason="too_few_dim_results",
        )

        # Step 2: source-affinity fallback (no dim filter, filter by source types)
        affinity_sources = DIMENSION_SOURCE_AFFINITY.get(dimension, [])
        if affinity_sources:
            filter_src = _build_filter(ticker, source_types=affinity_sources)
            src_results = retriever.retrieve(enriched_query, k=top_k, filter_metadata=filter_src)
            if len(src_results) > len(results):
                results = src_results
        if len(results) >= min_results:
            return results

    # Step 3: ticker-only with enriched query
    enriched = (DIMENSION_QUERY_MAP.get(dimension, "") + " " + query).strip() if dimension else query
    filter_ticker_only = _build_filter(ticker)
    fallback = retriever.retrieve(enriched, k=top_k, filter_metadata=filter_ticker_only)
    # Return whichever set was larger
    return fallback if len(fallback) > len(results if dimension else []) else (results if dimension else fallback)


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

@router.post("/index/{ticker}", response_model=IndexResponse, summary="Index company evidence into ChromaDB")
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

    logger.info("rag.index_complete", ticker=ticker, indexed_count=count)
    return IndexResponse(indexed_count=count, ticker=ticker, source_counts=dict(source_counts))


@router.post("/index", response_model=BulkIndexResponse, summary="Bulk index multiple tickers")
async def bulk_index_evidence(req: BulkIndexRequest):
    """Index CS2 evidence for multiple tickers in a single call."""
    logger.info("rag.bulk_index_start", tickers=req.tickers)
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
                indexed_count=count, ticker=ticker,
                source_counts=dict(source_counts),
            )
        except Exception as e:
            failed[ticker] = str(e)
            logger.warning("rag.bulk_index_ticker_error", ticker=ticker, error=str(e))

    _get_retriever().refresh_sparse_index()
    total_indexed = sum(r.indexed_count for r in results.values())
    return BulkIndexResponse(results=results, total_indexed=total_indexed, failed=failed)


@router.delete("/index", summary="Wipe ChromaDB index")
async def wipe_index(
    ticker: Optional[str] = Query(None),
):
    """Delete documents from the ChromaDB index."""
    vs = _get_vector_store()
    if ticker:
        wiped = vs.delete_by_filter({"ticker": {"$eq": ticker}})
    else:
        wiped = vs.wipe()
        _get_retriever().refresh_sparse_index()
    return {"wiped_count": wiped, "scope": ticker if ticker else "all"}


@router.post("/search", response_model=List[SearchResult], summary="Hybrid search over indexed evidence")
async def search_evidence(req: SearchRequest):
    """
    Hybrid dense + sparse search with optional HyDE enhancement.

    FIX: dimension filter now uses graceful fallback so searches never return
    empty when documents exist for that ticker.
    """
    logger.info("rag.search_start", query_len=len(req.query), ticker=req.ticker)
    retriever = _get_retriever()

    source_types = None
    if req.source_types:
        source_types = [s for s in req.source_types if s and s != "string"]

    if req.use_hyde and req.dimension:
        # HyDE path: build filter manually and pass to HyDE retriever
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
        # Use fallback-aware retrieval
        results = await _retrieve_with_fallback(
            retriever=retriever,
            query=req.query,
            ticker=req.ticker,
            dimension=req.dimension if req.dimension and req.dimension != "string" else None,
            top_k=req.top_k,
        )
    else:
        # No ticker — bare search (cross-company)
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


@router.get("/justify/{ticker}/{dimension}", response_model=JustifyResponse,
            summary="Generate cited score justification")
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


@router.get("/ic-prep/{ticker}", response_model=ICPrepResponse,
            summary="Generate full IC meeting preparation package")
async def ic_prep(
    ticker: str,
    dimensions: Optional[str] = Query(None),
):
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


@router.get("/status", summary="RAG system status")
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


@router.get("/debug", summary="Inspect ChromaDB contents")
async def rag_debug(
    ticker: Optional[str] = Query(None, description="Filter by ticker"),
    limit: int = Query(10, description="Max documents to return", le=100),
):
    """
    Show ChromaDB contents via search.
    Uses search endpoint instead of direct collection access
    since VectorStore now uses HTTP API (no _collection attribute).
    """
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


@router.get("/chatbot/{ticker}", summary="Simple chatbot Q&A for a company")
async def chatbot_query(
    ticker: str,
    question: str = Query(..., description="Question to ask about the company"),
    dimension: Optional[str] = Query(None, description="Optionally focus on a specific dimension"),
    use_hyde: bool = Query(False, description="Use HyDE query enhancement"),
):
    """
    Answer a question about a company using RAG.
    Used by the Streamlit chatbot interface.

    FIX SUMMARY vs previous version:
    1. Filter uses ticker key (not company_id) — matches how VectorStore indexes
    2. Uses _retrieve_with_fallback so dimension filter degrades gracefully
    3. Enriches the query with dimension keywords when a dimension is detected
       from the question text
    4. sources_used count is now correct (len of actual results returned)
    5. LLM answer extraction handles both string and object response formats
    """
    logger.info("rag.chatbot_query", ticker=ticker, question_len=len(question))
    retriever = _get_retriever()
    llm_router = _get_router()

    # Auto-detect dimension from question text if not explicitly provided
    detected_dimension = dimension
    if not detected_dimension:
        q_lower = question.lower()
        for dim_key, keywords in DIMENSION_QUERY_MAP.items():
            # Check if question contains any word from the dimension's keyword set
            kw_words = set(keywords.lower().split())
            q_words = set(q_lower.split())
            if len(kw_words & q_words) >= 2:
                detected_dimension = dim_key
                break

    # Retrieve with fallback — never returns 0 if any docs exist for ticker
    results = await _retrieve_with_fallback(
        retriever=retriever,
        query=question,
        ticker=ticker,
        dimension=detected_dimension,
        top_k=8,
        min_results=3,
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
        }

    # Build context — weight higher-scoring results first
    results_sorted = sorted(results, key=lambda r: r.score, reverse=True)
    context_parts = []
    for i, r in enumerate(results_sorted[:6]):
        src = r.metadata.get("source_type", "unknown")
        dim = r.metadata.get("dimension", "")
        fy = r.metadata.get("fiscal_year", "")
        label = f"[{src}" + (f", {fy}" if fy else "") + (f", dim={dim}" if dim else "") + "]"
        context_parts.append(f"{label}\n{r.content[:600]}")

    context = "\n\n---\n\n".join(context_parts)

    # Build dimension-aware system prompt
    dim_instruction = ""
    if detected_dimension:
        dim_label = detected_dimension.replace("_", " ").title()
        dim_instruction = (
            f" Focus your answer on the {dim_label} dimension of AI readiness."
        )

    messages = [
        {
            "role": "system",
            "content": (
                "You are a PE investment analyst preparing materials for an investment committee. "
                "Answer questions about companies based ONLY on the provided evidence excerpts. "
                "Be specific: cite the source type and fiscal year when referencing evidence. "
                "If the evidence does not contain enough information to answer, say so clearly "
                "and describe what evidence IS present."
                + dim_instruction
            ),
        },
        {
            "role": "user",
            "content": (
                f"Company: {ticker}\n\n"
                f"Evidence excerpts:\n{context}\n\n"
                f"Question: {question}\n\n"
                "Provide a 3-4 sentence answer with specific citations to the evidence above:"
            ),
        },
    ]

    try:
        raw_answer = llm_router.complete("chat_response", messages)
        # Handle both string response and object with .choices[0].message.content
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
        "ticker": ticker,
    }