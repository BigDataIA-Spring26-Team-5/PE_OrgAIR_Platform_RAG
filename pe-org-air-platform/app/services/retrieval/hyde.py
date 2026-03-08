"""HyDE — Hypothetical Document Embeddings query enhancement."""
from __future__ import annotations

from typing import List, Dict, Any, Optional

from app.services.retrieval.hybrid import HybridRetriever, RetrievedDocument
from app.services.llm.router import ModelRouter

_HYDE_PROMPT_TEMPLATE = """You are an expert private equity analyst specializing in AI readiness assessment.

Write a detailed passage (150–200 words) that would appear in a {source_type} for a company
scoring at Level 4 ("Good", 60–79) on the '{dimension}' dimension of AI readiness.

Company context: {company_context}

The passage should use specific, concrete language about {dimension} capabilities,
technologies, and practices. Include specific metrics, tools, or initiatives where appropriate.
Do NOT include headers or bullet points — write as flowing prose.

Passage:"""


class HyDERetriever:
    """Enhances retrieval quality by generating hypothetical answer documents."""

    def __init__(
        self,
        retriever: HybridRetriever,
        router: ModelRouter,
    ):
        self.retriever = retriever
        self.router = router

    def enhance_query(
        self,
        query: str,
        dimension: str = "",
        company_context: str = "",
    ) -> str:
        """Generate a hypothetical document that answers the query."""
        prompt = _HYDE_PROMPT_TEMPLATE.format(
            source_type="SEC 10-K filing or analyst report",
            dimension=dimension or "AI readiness",
            company_context=company_context or "a large-cap technology company",
        )
        messages = [
            {"role": "system", "content": "You write realistic financial document excerpts."},
            {"role": "user", "content": prompt},
        ]
        try:
            hypothetical_doc = self.router.complete("hyde_generation", messages)
            return hypothetical_doc.strip()
        except Exception:
            return query  # Fallback to original query

    def retrieve(
        self,
        query: str,
        k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        dimension: str = "",
        company_context: str = "",
    ) -> List[RetrievedDocument]:
        """Retrieve using HyDE: generate hypothetical doc, then search with it."""
        # Step 1: Generate hypothetical answer document
        hypothetical_doc = self.enhance_query(query, dimension, company_context)

        # Step 2: Use hypothetical doc as search query
        try:
            results = self.retriever.retrieve(hypothetical_doc, k=k, filter_metadata=filters)
            if results:
                return results
        except Exception:
            pass

        # Step 3: Fallback to original query
        return self.retriever.retrieve(query, k=k, filter_metadata=filters)
