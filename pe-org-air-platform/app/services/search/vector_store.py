"""ChromaDB vector store for PE evidence indexing."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

try:
    import chromadb
    from chromadb.config import Settings as ChromaSettings
    _CHROMA_AVAILABLE = True
except ImportError:
    _CHROMA_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    _ST_AVAILABLE = True
except ImportError:
    _ST_AVAILABLE = False


COLLECTION_NAME = "pe_evidence"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"


@dataclass
class SearchResult:
    doc_id: str
    content: str
    metadata: Dict[str, Any]
    score: float  # cosine similarity (0–1)
    distance: float


class VectorStore:
    """ChromaDB persistent vector store with sentence-transformer embeddings."""

    def __init__(self, persist_dir: str = "./chroma_data"):
        self.persist_dir = persist_dir
        self._encoder: Optional[Any] = None
        self._client: Optional[Any] = None
        self._collection: Optional[Any] = None
        self._init()

    def _init(self):
        if _ST_AVAILABLE:
            self._encoder = SentenceTransformer(EMBEDDING_MODEL)
        if _CHROMA_AVAILABLE:
            self._client = chromadb.PersistentClient(
                path=self.persist_dir,
                settings=ChromaSettings(anonymized_telemetry=False),
            )
            self._collection = self._client.get_or_create_collection(
                name=COLLECTION_NAME,
                metadata={"hnsw:space": "cosine"},
            )

    def _encode(self, texts: List[str]) -> List[List[float]]:
        if self._encoder is None:
            # Fallback: zero vectors
            return [[0.0] * 384 for _ in texts]
        return self._encoder.encode(texts, show_progress_bar=False).tolist()

    def index_cs2_evidence(self, evidence_list: list, dimension_mapper: Any) -> int:
        """Index a list of CS2Evidence objects into ChromaDB."""
        if not evidence_list:
            return 0
        if self._collection is None:
            return 0

        documents, embeddings, metadatas, ids = [], [], [], []
        seen_content_hashes: set = set()

        for ev in evidence_list:
            if not ev.content:
                continue
            content_hash = hash(ev.content[:2000])
            if content_hash in seen_content_hashes:
                continue
            seen_content_hashes.add(content_hash)
            dim_weights = dimension_mapper.get_dimension_weights(ev.signal_category)
            primary_dim = dimension_mapper.get_primary_dimension(ev.signal_category)

            meta = {
                "evidence_id": ev.evidence_id or "",
                "ticker": ev.company_id,
                "source_type": ev.source_type,
                "signal_category": ev.signal_category,
                "dimension": primary_dim,
                "dimension_weights": json.dumps({k: v for k, v in dim_weights.items()}),
                "confidence": float(ev.confidence),
                "fiscal_year": ev.fiscal_year or "",
                "source_url": ev.source_url or "",
                "page_number": str(ev.page_number or ""),
            }
            documents.append(ev.content[:2000])  # Chroma has content limits
            ids.append(ev.evidence_id or f"ev_{hash(ev.content)}")
            metadatas.append(meta)

        if documents:
            embeddings = self._encode(documents)
            # Upsert in batches of 100
            batch_size = 100
            for i in range(0, len(documents), batch_size):
                self._collection.upsert(
                    ids=ids[i:i+batch_size],
                    documents=documents[i:i+batch_size],
                    embeddings=embeddings[i:i+batch_size],
                    metadatas=metadatas[i:i+batch_size],
                )

        return len(documents)

    def search(
        self,
        query: str,
        top_k: int = 10,
        ticker: Optional[str] = None,
        dimension: Optional[str] = None,
        source_types: Optional[List[str]] = None,
        min_confidence: float = 0.0,
    ) -> List[SearchResult]:
        """Dense vector search with optional metadata filters."""
        if self._collection is None:
            return []

        where: Dict[str, Any] = {}
        conditions = []
        if ticker:
            conditions.append({"ticker": {"$eq": ticker}})
        if dimension:
            conditions.append({"dimension": {"$eq": dimension}})
        if source_types:
            conditions.append({"source_type": {"$in": source_types}})
        if min_confidence > 0:
            conditions.append({"confidence": {"$gte": min_confidence}})

        if len(conditions) > 1:
            where = {"$and": conditions}
        elif len(conditions) == 1:
            where = conditions[0]

        query_emb = self._encode([query])[0]
        kwargs: Dict[str, Any] = {
            "query_embeddings": [query_emb],
            "n_results": min(top_k, max(1, self._collection.count())),
            "include": ["documents", "metadatas", "distances"],
        }
        if where:
            kwargs["where"] = where

        try:
            results = self._collection.query(**kwargs)
        except Exception:
            return []

        output = []
        for doc, meta, dist in zip(
            results["documents"][0],
            results["metadatas"][0],
            results["distances"][0],
        ):
            score = 1.0 - dist  # cosine distance → similarity
            output.append(
                SearchResult(
                    doc_id=meta.get("source_url", "") or doc[:50],
                    content=doc,
                    metadata=meta,
                    score=score,
                    distance=dist,
                )
            )
        return output

    def count(self) -> int:
        if self._collection is None:
            return 0
        return self._collection.count()
