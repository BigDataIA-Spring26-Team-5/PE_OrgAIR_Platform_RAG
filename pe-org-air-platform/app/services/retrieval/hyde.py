"""HyDE — Hypothetical Document Embeddings query enhancement.

NOTE: router.complete() takes a plain string task key, NOT a TaskType enum.
Valid task keys defined in _TASK_ROUTING in app/services/llm/router.py:
"hyde_generation", "chat_response", "justification_generation", etc.
"""
from __future__ import annotations

from typing import List, Dict, Any, Optional

from app.services.retrieval.hybrid import HybridRetriever, RetrievedDocument
from app.services.llm.router import ModelRouter
from app.prompts.rag_prompts import HYDE_SYSTEM, HYDE_TEMPLATE


class HyDERetriever:
    """Enhances retrieval quality by generating hypothetical answer documents.

    HyDE works by:
    1. Generating a hypothetical document that *would* answer the query well
       (e.g. a realistic SEC 10-K excerpt about data infrastructure)
    2. Using that hypothetical document as the search query instead of the
       raw user question
    3. Falling back to the original query if generation fails

    This bridges the vocabulary gap between short user questions and the
    dense, domain-specific language of SEC filings and analyst reports.
    """

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
        """Generate a hypothetical document that answers the query.

        Uses "hyde_generation" task key which routes to Groq (fast, cheap)
        since this is an intermediate retrieval step, not a final IC output.
        Falls back to the original query on any failure.
        """
        prompt = HYDE_TEMPLATE.format(
            source_type="SEC 10-K filing or analyst report",
            dimension=dimension or "AI readiness",
            company_context=company_context or "a large-cap technology company",
        )
        messages = [
            {"role": "system", "content": HYDE_SYSTEM},
            {"role": "user", "content": prompt},
        ]
        try:
            # router.complete() takes a plain string task key and returns str directly
            result = self.router.complete("hyde_generation", messages)
            if isinstance(result, str):
                return result.strip()
            # Defensive: handle unexpected response shape
            if hasattr(result, "choices") and result.choices:
                return result.choices[0].message.content.strip()
            return query
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("hyde_enhance_failed error=%s falling_back_to_original_query", e)
            return query

    def retrieve(
        self,
        query: str,
        k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        dimension: str = "",
        company_context: str = "",
    ) -> List[RetrievedDocument]:
        """Retrieve using HyDE: generate hypothetical doc, then search with it.

        Steps:
        1. Generate a hypothetical answer document via LLM
        2. Search ChromaDB using the hypothetical doc as the query embedding
           (it's in the same semantic space as real documents, unlike the
           short raw question)
        3. Fall back to original query if generation or retrieval fails
        """
        # Step 1: Generate hypothetical answer document
        hypothetical_doc = self.enhance_query(query, dimension, company_context)

        # Step 2: Use hypothetical doc as search query
        try:
            results = self.retriever.retrieve(hypothetical_doc, k=k, filter_metadata=filters)
            if results:
                return results
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("hyde_retrieve_failed error=%s", e)

        # Step 3: Fallback to original query
        return self.retriever.retrieve(query, k=k, filter_metadata=filters)