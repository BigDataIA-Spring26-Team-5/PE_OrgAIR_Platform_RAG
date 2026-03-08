"""Hybrid Retriever — Dense (ChromaDB) + Sparse (BM25) + RRF fusion."""
from __future__ import annotations

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

try:
    import chromadb
    from chromadb.config import Settings as ChromaSettings
    _CHROMA_AVAILABLE = True
except ImportError:
    _CHROMA_AVAILABLE = False


@dataclass
class RetrievedDocument:
    doc_id: str
    content: str
    metadata: Dict[str, Any]
    score: float
    retrieval_method: str  # "dense", "sparse", or "hybrid"


class HybridRetriever:
    """Combines dense (ChromaDB) + sparse (BM25) retrieval with RRF fusion."""

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

        # Dense index
        self._encoder: Optional[Any] = None
        self._collection: Optional[Any] = None

        # Sparse index
        self._bm25: Optional[Any] = None
        self._doc_store: List[RetrievedDocument] = []
        self._tokenized_corpus: List[List[str]] = []

        self._init_dense()

    def _init_dense(self):
        if _ST_AVAILABLE:
            self._encoder = SentenceTransformer("all-MiniLM-L6-v2")
        if _CHROMA_AVAILABLE:
            client = chromadb.PersistentClient(
                path=self.persist_dir,
                settings=ChromaSettings(anonymized_telemetry=False),
            )
            self._collection = client.get_or_create_collection(
                name="pe_evidence",
                metadata={"hnsw:space": "cosine"},
            )
            self._load_bm25_from_chroma()

    def _load_bm25_from_chroma(self):
        """Seed BM25 sparse index from existing ChromaDB documents."""
        if self._collection is None or not _BM25_AVAILABLE:
            return
        count = self._collection.count()
        if count == 0:
            return
        result = self._collection.get(include=["documents", "metadatas"])
        self._doc_store = []
        for doc_id, doc, meta in zip(result["ids"], result["documents"], result["metadatas"]):
            self._doc_store.append(
                RetrievedDocument(
                    doc_id=doc_id,
                    content=doc,
                    metadata=meta,
                    score=0.0,
                    retrieval_method="sparse",
                )
            )
        self._tokenized_corpus = [d.content.lower().split() for d in self._doc_store]
        self._bm25 = BM25Okapi(self._tokenized_corpus)

    def refresh_sparse_index(self):
        """Rebuild BM25 from ChromaDB — call after indexing new documents."""
        self._load_bm25_from_chroma()

    def _encode(self, texts: List[str]) -> List[List[float]]:
        if self._encoder is None:
            return [[0.0] * 384 for _ in texts]
        return self._encoder.encode(texts, show_progress_bar=False).tolist()

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

        # Dense: upsert to ChromaDB
        if self._collection is not None:
            texts = [d.content[:2000] for d in documents]
            ids = [d.doc_id for d in documents]
            metas = [d.metadata for d in documents]
            embeddings = self._encode(texts)
            batch = 100
            for i in range(0, len(texts), batch):
                self._collection.upsert(
                    ids=ids[i:i+batch],
                    documents=texts[i:i+batch],
                    embeddings=embeddings[i:i+batch],
                    metadatas=metas[i:i+batch],
                )

        return len(documents)

    def retrieve(
        self,
        query: str,
        k: int = 10,
        filter_metadata: Optional[Dict[str, Any]] = None,
    ) -> List[RetrievedDocument]:
        """Retrieve top-k documents using RRF-fused hybrid search."""
        n_candidates = k * 3
        dense_results = self._dense_search(query, n_candidates, filter_metadata)
        sparse_results = self._sparse_search(query, n_candidates, filter_metadata)
        fused = self._rrf_fusion(dense_results, sparse_results, k)
        return fused

    def _dense_search(
        self,
        query: str,
        k: int,
        filter_metadata: Optional[Dict[str, Any]],
    ) -> List[RetrievedDocument]:
        if self._collection is None:
            return []
        count = self._collection.count()
        if count == 0:
            return []
        qemb = self._encode([query])[0]
        where = self._build_where(filter_metadata)
        kwargs: Dict[str, Any] = {
            "query_embeddings": [qemb],
            "n_results": min(k, count),
            "include": ["documents", "metadatas", "distances"],
        }
        if where:
            kwargs["where"] = where
        try:
            res = self._collection.query(**kwargs)
        except Exception:
            return []
        results = []
        for doc_id, doc, meta, dist in zip(
            res["ids"][0], res["documents"][0], res["metadatas"][0], res["distances"][0]
        ):
            results.append(
                RetrievedDocument(
                    doc_id=doc_id,
                    content=doc,
                    metadata=meta,
                    score=1.0 - dist,
                    retrieval_method="dense",
                )
            )
        return results

    def _sparse_search(
        self,
        query: str,
        k: int,
        filter_metadata: Optional[Dict[str, Any]],
    ) -> List[RetrievedDocument]:
        if self._bm25 is None or not self._doc_store:
            return []
        tokens = query.lower().split()
        scores = self._bm25.get_scores(tokens)
        ranked_idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)
        results = []
        for idx in ranked_idx[:k]:
            doc = self._doc_store[idx]
            if filter_metadata:
                if not self._matches_filter(doc.metadata, filter_metadata):
                    continue
            results.append(
                RetrievedDocument(
                    doc_id=doc.doc_id,
                    content=doc.content,
                    metadata=doc.metadata,
                    score=float(scores[idx]),
                    retrieval_method="sparse",
                )
            )
        return results

    def _rrf_fusion(
        self,
        dense: List[RetrievedDocument],
        sparse: List[RetrievedDocument],
        k: int,
    ) -> List[RetrievedDocument]:
        """Reciprocal Rank Fusion: score = Σ w_r / (rrf_k + rank_r(d))"""
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
        result = []
        for doc_id, score in ranked:
            doc = doc_map[doc_id]
            result.append(
                RetrievedDocument(
                    doc_id=doc.doc_id,
                    content=doc.content,
                    metadata=doc.metadata,
                    score=score,
                    retrieval_method="hybrid",
                )
            )
        return result

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
