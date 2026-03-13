"""Unit tests for HyDERetriever — all LLM and retriever calls mocked."""
from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from app.services.retrieval.hyde import HyDERetriever
from app.services.retrieval.hybrid import RetrievedDocument


def make_doc(doc_id: str = "d1", score: float = 0.9) -> RetrievedDocument:
    return RetrievedDocument(
        doc_id=doc_id,
        content="Relevant content about AI infrastructure.",
        metadata={"ticker": "nvda"},
        score=score,
        retrieval_method="dense",
    )


@pytest.fixture
def mock_router():
    r = MagicMock()
    r.complete.return_value = "Hypothetical document about data infrastructure..."
    return r


@pytest.fixture
def mock_retriever():
    r = MagicMock()
    r.retrieve.return_value = [make_doc("d1", 0.9)]
    return r


@pytest.fixture
def hyde(mock_retriever, mock_router) -> HyDERetriever:
    return HyDERetriever(retriever=mock_retriever, router=mock_router)


# ---------------------------------------------------------------------------
# enhance_query
# ---------------------------------------------------------------------------

def test_enhance_query_returns_llm_output(hyde, mock_router):
    mock_router.complete.return_value = "  Hypothetical passage about cloud pipelines.  "
    result = hyde.enhance_query("cloud pipelines?", dimension="data_infrastructure")
    assert result == "Hypothetical passage about cloud pipelines."


def test_enhance_query_falls_back_on_exception(hyde, mock_router):
    mock_router.complete.side_effect = RuntimeError("LLM unavailable")
    result = hyde.enhance_query("cloud pipelines?")
    assert result == "cloud pipelines?"


def test_enhance_query_handles_unexpected_response_type(hyde, mock_router):
    """Router returns an object without .choices — falls back to original query."""
    mock_response = MagicMock(spec=[])  # no 'choices' attribute
    mock_router.complete.return_value = mock_response
    result = hyde.enhance_query("some question")
    assert result == "some question"


def test_enhance_query_uses_hyde_generation_task(hyde, mock_router):
    hyde.enhance_query("test query")
    task_used = mock_router.complete.call_args[0][0]
    assert task_used == "hyde_generation"


def test_enhance_query_prompt_includes_dimension(hyde, mock_router):
    hyde.enhance_query("talent gaps?", dimension="talent")
    messages_used = mock_router.complete.call_args[0][1]
    full_prompt = " ".join(m["content"] for m in messages_used)
    assert "talent" in full_prompt.lower()


# ---------------------------------------------------------------------------
# retrieve
# ---------------------------------------------------------------------------

def test_retrieve_uses_hypothetical_doc_as_query(hyde, mock_retriever, mock_router):
    mock_router.complete.return_value = "Hypothetical doc text"
    hyde.retrieve("original query", k=5)
    first_call_query = mock_retriever.retrieve.call_args_list[0][0][0]
    assert first_call_query == "Hypothetical doc text"


def test_retrieve_fallback_on_empty_results(hyde, mock_retriever, mock_router):
    """If hypothetical doc retrieval returns [], fall back to original query."""
    mock_router.complete.return_value = "Hypothetical doc"
    mock_retriever.retrieve.side_effect = [[], [make_doc("fallback", 0.7)]]

    results = hyde.retrieve("original query", k=5)

    assert mock_retriever.retrieve.call_count == 2
    first_call = mock_retriever.retrieve.call_args_list[0][0][0]
    second_call = mock_retriever.retrieve.call_args_list[1][0][0]
    assert first_call == "Hypothetical doc"
    assert second_call == "original query"
    assert len(results) == 1


def test_retrieve_fallback_on_retrieve_exception(hyde, mock_retriever, mock_router):
    """If hypothetical doc retrieval raises, fall back to original query."""
    mock_router.complete.return_value = "Hypothetical doc"
    mock_retriever.retrieve.side_effect = [
        RuntimeError("Chroma down"),
        [make_doc("fallback", 0.6)],
    ]

    results = hyde.retrieve("original query", k=5)

    assert mock_retriever.retrieve.call_count == 2
    assert results[0].doc_id == "fallback"
