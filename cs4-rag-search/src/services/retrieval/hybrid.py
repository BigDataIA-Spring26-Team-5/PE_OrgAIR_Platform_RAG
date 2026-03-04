"""Hybrid retrieval with RRF fusion."""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from collections import defaultdict
from rank_bm25 import BM25Okapi
from sentence_transformers import SentenceTransformer
import chromadb

@dataclass
class RetrievedDocument:
    doc_id: str
    content: str
    metadata: Dict[str, Any]
    score: float
    retrieval_method: str

class HybridRetriever:
    """Hybrid retrieval combining dense and sparse search."""

    def __init__(self, dense_weight: float = 0.6, sparse_weight: float = 0.4, rrf_k: int = 60):
        self.dense_weight = dense_weight
        self.sparse_weight = sparse_weight
        self.rrf_k = rrf_k
        self.encoder = SentenceTransformer("all-MiniLM-L6-v2")
        self.chroma = chromadb.PersistentClient(path="./chroma_data")
        self.collection = self.chroma.get_or_create_collection(
            name="pe_evidence", metadata={"hnsw:space": "cosine"}
        )
        self._bm25, self._corpus, self._doc_ids, self._metadata = None, [], [], []

    def index_documents(self, documents: List[Dict[str, Any]]) -> int:
        ids = [d["doc_id"] for d in documents]
        contents = [d["content"] for d in documents]
        metadatas = [d.get("metadata", {}) for d in documents]

        # Dense indexing
        embeddings = self.encoder.encode(contents).tolist()
        self.collection.add(ids=ids, embeddings=embeddings, documents=contents, metadatas=metadatas)

        # Sparse indexing
        self._corpus.extend(contents)
        self._doc_ids.extend(ids)
        self._metadata.extend(metadatas)
        self._bm25 = BM25Okapi([c.lower().split() for c in self._corpus])
        return len(documents)

    async def retrieve(
        self,
        query: str,
        k: int = 10,
        filter_metadata: Optional[Dict] = None,
    ) -> List[RetrievedDocument]:
        n = k * 3

        # Dense retrieval
        qe = self.encoder.encode(query).tolist()
        dr = self.collection.query(query_embeddings=[qe], n_results=n, where=filter_metadata)
        dense = [
            RetrievedDocument(
                doc_id=dr["ids"][0][i], content=dr["documents"][0][i],
                metadata=dr["metadatas"][0][i], score=1 - dr["distances"][0][i],
                retrieval_method="dense"
            )
            for i in range(len(dr["ids"][0]))
        ]

        # Sparse retrieval
        scores = self._bm25.get_scores(query.lower().split())
        top_idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:n]
        sparse = [
            RetrievedDocument(
                doc_id=self._doc_ids[i], content=self._corpus[i],
                metadata=self._metadata[i], score=scores[i],
                retrieval_method="sparse"
            )
            for i in top_idx
        ]

        # RRF Fusion
        return self._rrf_fusion(dense, sparse, k)

    def _rrf_fusion(self, dense: List[RetrievedDocument], sparse: List[RetrievedDocument], k: int):
        scores, doc_map = defaultdict(float), {}
        for rank, doc in enumerate(dense):
            scores[doc.doc_id] += self.dense_weight / (self.rrf_k + rank + 1)
            doc_map[doc.doc_id] = doc
        for rank, doc in enumerate(sparse):
            scores[doc.doc_id] += self.sparse_weight / (self.rrf_k + rank + 1)
            if doc.doc_id not in doc_map:
                doc_map[doc.doc_id] = doc

        sorted_ids = sorted(scores.keys(), key=lambda x: scores[x], reverse=True)[:k]
        return [
            RetrievedDocument(
                doc_id=did, content=doc_map[did].content, metadata=doc_map[did].metadata,
                score=scores[did], retrieval_method="hybrid"
            )
            for did in sorted_ids
        ]
