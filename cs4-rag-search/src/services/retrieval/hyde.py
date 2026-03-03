"""
hyde.py — CS4 RAG Search
src/services/retrieval/hyde.py

HyDE (Hypothetical Document Embedding) retrieval.
Generates a hypothetical answer via LLM, then uses its embedding for dense search.
Reference: Gao et al. 2022 — "Precise Zero-Shot Dense Retrieval without Relevance Labels"
"""

from __future__ import annotations

from typing import List, Optional

import litellm
from sentence_transformers import SentenceTransformer

from .hybrid import HybridRetriever, RetrievedChunk


class HyDERetriever:
    """
    Wraps HybridRetriever with a HyDE pre-step:
      query → LLM → hypothetical answer → dense search with answer embedding.
    """

    _SYSTEM_PROMPT = (
        "You are a financial due diligence analyst. "
        "Write a concise hypothetical passage (3–5 sentences) that would answer "
        "the following question as if it appeared in a 10-K filing, analyst note, "
        "or company document. Do not explain — just write the passage."
    )

    def __init__(
        self,
        retriever: HybridRetriever,
        llm_model: str = "gpt-4o-mini",
        temperature: float = 0.3,
    ) -> None:
        self._retriever = retriever
        self._llm_model = llm_model
        self._temperature = temperature

    async def search(self, query: str, top_k: int = 10) -> List[RetrievedChunk]:
        """Generate hypothetical document, then retrieve using its embedding."""
        hypothetical_doc = await self._generate_hypothetical(query)
        # Use the hypothetical doc as the dense search query
        return self._retriever._dense_search(hypothetical_doc, top_k)

    async def _generate_hypothetical(self, query: str) -> str:
        response = await litellm.acompletion(
            model=self._llm_model,
            messages=[
                {"role": "system", "content": self._SYSTEM_PROMPT},
                {"role": "user", "content": query},
            ],
            temperature=self._temperature,
            max_tokens=300,
        )
        return response.choices[0].message.content.strip()
