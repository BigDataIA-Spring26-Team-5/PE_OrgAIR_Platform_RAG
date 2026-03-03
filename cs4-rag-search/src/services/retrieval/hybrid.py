"""
hybrid.py — CS4 RAG Search
src/services/retrieval/hybrid.py

Hybrid BM25 + dense vector retrieval over ChromaDB.
BM25 uses rank_bm25; dense embeddings use sentence-transformers + ChromaDB.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import chromadb
from rank_bm25 import BM25Okapi
from sentence_transformers import SentenceTransformer


@dataclass
class RetrievedChunk:
    chunk_id: str
    text: str
    score: float
    source: str = ""
    ticker: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


class HybridRetriever:
    """
    Two-stage retrieval:
      1. BM25 keyword search over a local corpus
      2. Dense vector search via ChromaDB
    Results are fused with Reciprocal Rank Fusion (RRF).
    """

    def __init__(
        self,
        collection_name: str = "evidence",
        chroma_host: str = "localhost",
        chroma_port: int = 8001,
        embed_model: str = "all-MiniLM-L6-v2",
        bm25_weight: float = 0.3,
        dense_weight: float = 0.7,
    ) -> None:
        self._collection_name = collection_name
        self._bm25_weight = bm25_weight
        self._dense_weight = dense_weight

        self._encoder = SentenceTransformer(embed_model)
        self._chroma = chromadb.HttpClient(host=chroma_host, port=chroma_port)
        self._collection = self._chroma.get_or_create_collection(collection_name)

        # BM25 index is built lazily on first search or after add_documents()
        self._bm25: Optional[BM25Okapi] = None
        self._bm25_corpus: List[str] = []
        self._bm25_ids: List[str] = []

    # ------------------------------------------------------------------
    # Indexing
    # ------------------------------------------------------------------

    def add_documents(
        self,
        texts: List[str],
        ids: List[str],
        metadatas: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Embed and store documents; rebuild BM25 index."""
        embeddings = self._encoder.encode(texts, show_progress_bar=False).tolist()
        self._collection.upsert(
            documents=texts,
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas or [{} for _ in texts],
        )
        self._bm25_corpus = texts
        self._bm25_ids = ids
        tokenized = [t.lower().split() for t in texts]
        self._bm25 = BM25Okapi(tokenized)

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def search(self, query: str, top_k: int = 10) -> List[RetrievedChunk]:
        """Return top-k chunks using RRF-fused BM25 + dense scores."""
        dense_results = self._dense_search(query, top_k)
        bm25_results = self._bm25_search(query, top_k)
        return self._rrf_fuse(dense_results, bm25_results, top_k)

    def _dense_search(self, query: str, top_k: int) -> List[RetrievedChunk]:
        embedding = self._encoder.encode([query], show_progress_bar=False).tolist()[0]
        results = self._collection.query(
            query_embeddings=[embedding],
            n_results=min(top_k, self._collection.count() or 1),
            include=["documents", "metadatas", "distances"],
        )
        chunks = []
        for i, doc_id in enumerate(results["ids"][0]):
            text = results["documents"][0][i]
            dist = results["distances"][0][i]
            meta = results["metadatas"][0][i] if results["metadatas"] else {}
            chunks.append(RetrievedChunk(
                chunk_id=doc_id,
                text=text,
                score=1.0 / (1.0 + dist),  # convert distance to similarity
                source=meta.get("source", ""),
                ticker=meta.get("ticker", ""),
                metadata=meta,
            ))
        return chunks

    def _bm25_search(self, query: str, top_k: int) -> List[RetrievedChunk]:
        if self._bm25 is None or not self._bm25_corpus:
            return []
        tokenized_query = query.lower().split()
        scores = self._bm25.get_scores(tokenized_query)
        ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)[:top_k]
        chunks = []
        for idx, score in ranked:
            chunks.append(RetrievedChunk(
                chunk_id=self._bm25_ids[idx],
                text=self._bm25_corpus[idx],
                score=float(score),
            ))
        return chunks

    @staticmethod
    def _rrf_fuse(
        dense: List[RetrievedChunk],
        bm25: List[RetrievedChunk],
        top_k: int,
        k: int = 60,
    ) -> List[RetrievedChunk]:
        """Reciprocal Rank Fusion."""
        scores: Dict[str, float] = {}
        index: Dict[str, RetrievedChunk] = {}

        for rank, chunk in enumerate(dense):
            scores[chunk.chunk_id] = scores.get(chunk.chunk_id, 0.0) + 1.0 / (k + rank + 1)
            index[chunk.chunk_id] = chunk

        for rank, chunk in enumerate(bm25):
            scores[chunk.chunk_id] = scores.get(chunk.chunk_id, 0.0) + 1.0 / (k + rank + 1)
            if chunk.chunk_id not in index:
                index[chunk.chunk_id] = chunk

        sorted_ids = sorted(scores, key=lambda cid: scores[cid], reverse=True)[:top_k]
        results = []
        for cid in sorted_ids:
            chunk = index[cid]
            chunk.score = scores[cid]
            results.append(chunk)
        return results
