"""Hybrid Retriever — Dense (Chroma Cloud HTTP) + Sparse (BM25) + RRF fusion.

Uses VectorStore for dense search (which uses Chroma Cloud HTTP API directly)
to avoid onnxruntime/chromadb DLL issues on Windows.

FIX: BM25 corpus now seeded from ChromaDB on startup via broad sampling search.
     Previously _load_bm25_from_store() was a no-op (pass), meaning BM25 never
     fired and every retrieval was dense-only. Now we fetch up to BM25_SEED_LIMIT
     docs from the cloud store at init time to populate the sparse index.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

try:
    from rank_bm25 import BM25Okapi
    _BM25_AVAILABLE = True
except ImportError:
    _BM25_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    _ST_AVAILABLE = True
except ImportError:
    _ST_AVAILABLE = False

from app.services.search.vector_store import VectorStore, SearchResult

logger = logging.getLogger(__name__)

# How many docs to pull from ChromaDB to seed BM25 at startup.
# 500 gives good sparse coverage without being slow at init.
BM25_SEED_LIMIT = 500

# Broad seed queries — rotated to maximise vocabulary coverage when seeding BM25.
_SEED_QUERIES = [
    "AI machine learning data infrastructure technology governance talent",
    "revenue growth strategy investment risk compliance board",
    "cloud platform engineering pipeline data quality analytics",
    "leadership executive officer director management team culture",
    "patent innovation research development product deployment",
]


@dataclass
class RetrievedDocument:
    doc_id: str
    content: str
    metadata: Dict[str, Any]
    score: float
    retrieval_method: str  # "dense", "sparse", or "hybrid"


class HybridRetriever:
    """Combines dense (Chroma Cloud) + sparse (BM25) retrieval with RRF fusion.

    Dense search uses VectorStore which calls Chroma Cloud HTTP API directly.
    No chromadb Python package required.
    """

    def __init__(
        self,
        dense_weight: float = 0.6,
        sparse_weight: float = 0.4,
        rrf_k: int = 60,
        persist_dir: str = "./chroma_data",
    ):
        self.dense_weight = dense_weight
        self.sparse_weight = sparse_weight
        self.rrf_k = rrf_k
        self.persist_dir = persist_dir

        # Dense index via VectorStore (uses Chroma Cloud HTTP API)
        self._vector_store = VectorStore(persist_dir=persist_dir)

        # Sparse index
        self._bm25: Optional[Any] = None
        self._doc_store: List[RetrievedDocument] = []
        self._tokenized_corpus: List[List[str]] = []

        # Seed BM25 from existing ChromaDB docs on startup
        self._load_bm25_from_store()

    def _load_bm25_from_store(self):
        """
        Seed BM25 corpus from ChromaDB at startup.

        Runs multiple broad queries and unions the results to maximise
        vocabulary coverage. Capped at BM25_SEED_LIMIT total unique docs.

        Previously this was `pass` — meaning BM25 was always empty and
        _sparse_search() always returned []. Fixed here.
        """
        if not _BM25_AVAILABLE:
            logger.warning("bm25_unavailable rank_bm25 not installed")
            return

        total = self._vector_store.count()
        if total == 0:
            logger.info("bm25_seed_skipped reason=empty_vector_store")
            return

        seen_ids: set = set()
        docs: List[RetrievedDocument] = []

        # How many docs to fetch per seed query
        per_query_k = max(BM25_SEED_LIMIT // len(_SEED_QUERIES), 50)

        for seed_query in _SEED_QUERIES:
            if len(docs) >= BM25_SEED_LIMIT:
                break
            try:
                results = self._vector_store.search(
                    query=seed_query,
                    top_k=per_query_k,
                )
                for r in results:
                    if r.doc_id not in seen_ids:
                        seen_ids.add(r.doc_id)
                        docs.append(RetrievedDocument(
                            doc_id=r.doc_id,
                            content=r.content,
                            metadata=r.metadata,
                            score=r.score,
                            retrieval_method="dense",
                        ))
                        if len(docs) >= BM25_SEED_LIMIT:
                            break
            except Exception as e:
                logger.warning("bm25_seed_query_failed query=%s error=%s", seed_query[:30], e)

        if docs:
            self._doc_store = docs
            self._tokenized_corpus = [d.content.lower().split() for d in docs]
            self._bm25 = BM25Okapi(self._tokenized_corpus)
            logger.info("bm25_seeded doc_count=%d", len(docs))
        else:
            logger.warning("bm25_seed_empty no_docs_fetched")

    def refresh_sparse_index(self):
        """
        Rebuild BM25 — call after indexing new documents.

        Re-seeds from ChromaDB so the sparse index reflects the latest state.
        """
        logger.info("bm25_refresh_start")
        self._doc_store = []
        self._bm25 = None
        self._tokenized_corpus = []
        self._load_bm25_from_store()
        logger.info("bm25_refresh_complete doc_count=%d", len(self._doc_store))

    def _encode(self, texts: List[str]) -> List[List[float]]:
        return self._vector_store._encode(texts)

    def index_documents(self, documents: List[RetrievedDocument]) -> int:
        """Index documents into both dense and sparse indices."""
        if not documents:
            return 0

        # Sparse: rebuild BM25 corpus
        self._doc_store.extend(documents)
        self._tokenized_corpus = [
            doc.content.lower().split() for doc in self._doc_store
        ]
        if _BM25_AVAILABLE and self._tokenized_corpus:
            self._bm25 = BM25Okapi(self._tokenized_corpus)

        # Dense: upsert to Chroma Cloud via VectorStore
        if self._vector_store._use_cloud and self._vector_store._collection_id:
            texts = [d.content[:2000] for d in documents]
            ids = [d.doc_id for d in documents]
            metas = [d.metadata for d in documents]
            embeddings = self._encode(texts)
            batch = 100
            for i in range(0, len(texts), batch):
                self._vector_store._cloud_upsert(
                    ids[i:i+batch],
                    texts[i:i+batch],
                    embeddings[i:i+batch],
                    metas[i:i+batch],
                )

        return len(documents)

    def retrieve(
        self,
        query: str,
        k: int = 10,
        filter_metadata: Optional[Dict[str, Any]] = None,
    ) -> List[RetrievedDocument]:
        """Retrieve top-k documents using RRF-fused hybrid search."""
        n_candidates = k * 5
        dense_results = self._dense_search(query, n_candidates, filter_metadata)
        sparse_results = self._sparse_search(query, n_candidates, filter_metadata)
        return self._rrf_fusion(dense_results, sparse_results, k)

    def _dense_search(
        self,
        query: str,
        k: int,
        filter_metadata: Optional[Dict[str, Any]],
    ) -> List[RetrievedDocument]:
        """Dense search via VectorStore (Chroma Cloud HTTP API)."""
        ticker = filter_metadata.get("ticker") if filter_metadata else None
        dimension = filter_metadata.get("dimension") if filter_metadata else None
        source_types = filter_metadata.get("source_type") if filter_metadata else None
        if isinstance(source_types, str):
            source_types = [source_types]

        # Handle $and / $in filter structures from evidence.py _build_filter()
        if filter_metadata and "$and" in filter_metadata:
            for clause in filter_metadata["$and"]:
                if "ticker" in clause:
                    ticker = ticker or clause["ticker"]
                if "dimension" in clause:
                    dimension = dimension or clause["dimension"]
                if "source_type" in clause:
                    st = clause["source_type"]
                    source_types = source_types or (st.get("$in") if isinstance(st, dict) else [st])

        results = self._vector_store.search(
            query=query,
            top_k=k,
            ticker=ticker,
            dimension=dimension,
            source_types=source_types,
        )

        return [
            RetrievedDocument(
                doc_id=r.doc_id,
                content=r.content,
                metadata=r.metadata,
                score=r.score,
                retrieval_method="dense",
            )
            for r in results
        ]

    def _sparse_search(
        self,
        query: str,
        k: int,
        filter_metadata: Optional[Dict[str, Any]],
    ) -> List[RetrievedDocument]:
        """BM25 sparse search over in-memory doc store."""
        if self._bm25 is None or not self._doc_store:
            return []

        tokens = query.lower().split()
        scores = self._bm25.get_scores(tokens)
        ranked_idx = sorted(
            range(len(scores)), key=lambda i: scores[i], reverse=True
        )

        # Build a flat filter for metadata matching
        flat_filter = self._flatten_filter(filter_metadata)

        results = []
        for idx in ranked_idx:
            if len(results) >= k:
                break
            if scores[idx] <= 0:
                break  # BM25 gives 0 for no-match — stop early
            doc = self._doc_store[idx]
            if flat_filter and not self._matches_filter(doc.metadata, flat_filter):
                continue
            results.append(RetrievedDocument(
                doc_id=doc.doc_id,
                content=doc.content,
                metadata=doc.metadata,
                score=float(scores[idx]),
                retrieval_method="sparse",
            ))
        return results

    def _rrf_fusion(
        self,
        dense: List[RetrievedDocument],
        sparse: List[RetrievedDocument],
        k: int,
    ) -> List[RetrievedDocument]:
        """Reciprocal Rank Fusion."""
        rrf_scores: Dict[str, float] = {}
        doc_map: Dict[str, RetrievedDocument] = {}

        for rank, doc in enumerate(dense):
            rrf_scores[doc.doc_id] = rrf_scores.get(doc.doc_id, 0.0) + (
                self.dense_weight / (self.rrf_k + rank + 1)
            )
            doc_map[doc.doc_id] = doc

        for rank, doc in enumerate(sparse):
            rrf_scores[doc.doc_id] = rrf_scores.get(doc.doc_id, 0.0) + (
                self.sparse_weight / (self.rrf_k + rank + 1)
            )
            if doc.doc_id not in doc_map:
                doc_map[doc.doc_id] = doc

        ranked = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)[:k]
        return [
            RetrievedDocument(
                doc_id=did,
                content=doc_map[did].content,
                metadata=doc_map[did].metadata,
                score=score,
                retrieval_method="hybrid",
            )
            for did, score in ranked
        ]

    @staticmethod
    def _flatten_filter(filter_metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Flatten a nested $and filter into a simple key→value dict for
        _matches_filter(). Handles both plain dicts and $and/$in structures.
        """
        if not filter_metadata:
            return {}
        if "$and" not in filter_metadata:
            return filter_metadata
        flat: Dict[str, Any] = {}
        for clause in filter_metadata["$and"]:
            for k, v in clause.items():
                if k.startswith("$"):
                    continue
                if isinstance(v, dict) and "$in" in v:
                    flat[k] = v["$in"]  # list
                elif isinstance(v, dict) and "$eq" in v:
                    flat[k] = v["$eq"]
                else:
                    flat[k] = v
        return flat

    @staticmethod
    def _build_where(filter_metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not filter_metadata:
            return {}
        conditions = []
        for k, v in filter_metadata.items():
            if isinstance(v, list):
                conditions.append({k: {"$in": v}})
            else:
                conditions.append({k: {"$eq": v}})
        if len(conditions) == 1:
            return conditions[0]
        return {"$and": conditions}

    @staticmethod
    def _matches_filter(metadata: Dict[str, Any], filter_metadata: Dict[str, Any]) -> bool:
        for k, v in filter_metadata.items():
            val = metadata.get(k)
            if isinstance(v, list):
                if val not in v:
                    return False
            elif val != v:
                return False
        return True