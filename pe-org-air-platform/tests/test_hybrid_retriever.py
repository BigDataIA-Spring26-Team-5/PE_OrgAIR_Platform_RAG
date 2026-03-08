"""Tests for HybridRetriever — RRF fusion logic."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from app.services.retrieval.hybrid import HybridRetriever, RetrievedDocument


def make_doc(doc_id: str, content: str = "test content", score: float = 0.5) -> RetrievedDocument:
    return RetrievedDocument(
        doc_id=doc_id,
        content=content,
        metadata={"company_id": "test"},
        score=score,
        retrieval_method="test",
    )


def test_rrf_fusion_combines_results():
    retriever = HybridRetriever.__new__(HybridRetriever)
    retriever.dense_weight = 0.6
    retriever.sparse_weight = 0.4
    retriever.rrf_k = 60

    dense = [make_doc("a", score=0.9), make_doc("b", score=0.8), make_doc("c", score=0.7)]
    sparse = [make_doc("b", score=5.0), make_doc("d", score=4.0), make_doc("a", score=3.0)]

    result = retriever._rrf_fusion(dense, sparse, k=4)
    assert len(result) <= 4
    doc_ids = [r.doc_id for r in result]
    # "a" and "b" appear in both — should rank high
    assert "a" in doc_ids
    assert "b" in doc_ids


def test_rrf_fusion_respects_weights():
    retriever = HybridRetriever.__new__(HybridRetriever)
    retriever.dense_weight = 1.0
    retriever.sparse_weight = 0.0
    retriever.rrf_k = 60

    dense = [make_doc("x", score=0.95), make_doc("y", score=0.5)]
    sparse = [make_doc("z", score=9.0), make_doc("w", score=8.0)]

    result = retriever._rrf_fusion(dense, sparse, k=4)
    # With dense_weight=1.0, dense results should dominate
    assert result[0].doc_id == "x"


def test_rrf_fusion_empty_inputs():
    retriever = HybridRetriever.__new__(HybridRetriever)
    retriever.dense_weight = 0.6
    retriever.sparse_weight = 0.4
    retriever.rrf_k = 60

    result = retriever._rrf_fusion([], [], k=5)
    assert result == []


def test_rrf_fusion_single_source():
    retriever = HybridRetriever.__new__(HybridRetriever)
    retriever.dense_weight = 0.6
    retriever.sparse_weight = 0.4
    retriever.rrf_k = 60

    dense = [make_doc(f"doc{i}", score=1.0 - i * 0.1) for i in range(5)]
    result = retriever._rrf_fusion(dense, [], k=3)
    assert len(result) == 3


def test_rrf_scores_decrease_with_rank():
    retriever = HybridRetriever.__new__(HybridRetriever)
    retriever.dense_weight = 0.6
    retriever.sparse_weight = 0.4
    retriever.rrf_k = 60

    dense = [make_doc(f"d{i}") for i in range(5)]
    sparse = [make_doc(f"s{i}") for i in range(5)]
    result = retriever._rrf_fusion(dense, sparse, k=10)

    scores = [r.score for r in result]
    assert scores == sorted(scores, reverse=True)


def test_matches_filter():
    assert HybridRetriever._matches_filter(
        {"company_id": "nvda", "dimension": "talent"},
        {"company_id": "nvda"},
    )
    assert not HybridRetriever._matches_filter(
        {"company_id": "aapl"},
        {"company_id": "nvda"},
    )


def test_build_where_single():
    where = HybridRetriever._build_where({"company_id": "nvda"})
    assert where == {"company_id": {"$eq": "nvda"}}


def test_build_where_multiple():
    where = HybridRetriever._build_where({"company_id": "nvda", "dimension": "talent"})
    assert "$and" in where
    assert len(where["$and"]) == 2
