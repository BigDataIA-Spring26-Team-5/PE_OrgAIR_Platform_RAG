"""ChromaDB vector store with CS2 evidence metadata."""
import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class SearchResult:
    doc_id: str
    content: str
    score: float
    metadata: Dict[str, Any]

class VectorStore:
    """Vector store preserving CS2 evidence metadata."""

    def __init__(self, persist_dir: str = "./chroma_data"):
        self.client = chromadb.PersistentClient(
            path=persist_dir,
            settings=Settings(anonymized_telemetry=False)
        )
        self.collection = self.client.get_or_create_collection(
            name="pe_evidence",
            metadata={"hnsw:space": "cosine"}
        )
        self.encoder = SentenceTransformer("all-MiniLM-L6-v2")

    def index_cs2_evidence(
        self,
        evidence_list: List['CS2Evidence'],
        dimension_mapper: 'DimensionMapper'
    ) -> int:
        """
        Index CS2 evidence with dimension mapping.

        Preserves: source_type, signal_category, confidence,
                   company_id, dimension (from mapper)
        """
        ids, contents, metadatas = [], [], []

        for e in evidence_list:
            # Get dimension weights from CS3's mapping
            dim_weights = dimension_mapper.get_dimension_weights(e.signal_category)
            primary_dim = dimension_mapper.get_primary_dimension(e.signal_category)

            ids.append(e.evidence_id)
            contents.append(e.content)
            metadatas.append({
                "company_id": e.company_id,
                "source_type": e.source_type.value,
                "signal_category": e.signal_category.value,
                "dimension": primary_dim.value,
                "dimension_weights": str(dim_weights),  # JSON string
                "confidence": e.confidence,
                "fiscal_year": e.fiscal_year or 0,
                "source_url": e.source_url or "",
            })

        embeddings = self.encoder.encode(contents).tolist()
        self.collection.add(
            ids=ids, embeddings=embeddings,
            documents=contents, metadatas=metadatas
        )
        return len(ids)

    def search(
        self,
        query: str,
        top_k: int = 10,
        company_id: Optional[str] = None,
        dimension: Optional[str] = None,
        source_types: Optional[List[str]] = None,
        min_confidence: float = 0.0,
    ) -> List[SearchResult]:
        """Search with metadata filters."""
        # Build ChromaDB where clause
        where = {}
        if company_id:
            where["company_id"] = company_id
        if dimension:
            where["dimension"] = dimension
        if min_confidence > 0:
            where["confidence"] = {"$gte": min_confidence}

        query_embedding = self.encoder.encode(query).tolist()
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=top_k,
            where=where if where else None
        )

        return [
            SearchResult(
                doc_id=results["ids"][0][i],
                content=results["documents"][0][i],
                score=1 - results["distances"][0][i],
                metadata=results["metadatas"][0][i]
            )
            for i in range(len(results["ids"][0]))
        ]
