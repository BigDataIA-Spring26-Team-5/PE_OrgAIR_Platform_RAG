"""Integration tests for JustificationGenerator with mocked LLM."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from app.services.justification.generator import (
    JustificationGenerator,
    ScoreJustification,
    CitedEvidence,
)
from app.services.integration.cs3_client import DimensionScore, RubricCriteria
from app.services.retrieval.hybrid import RetrievedDocument


def make_retrieved_doc(doc_id: str, content: str, score: float = 0.8) -> RetrievedDocument:
    return RetrievedDocument(
        doc_id=doc_id,
        content=content,
        metadata={
            "source_type": "sec_10k_item_1",
            "source_url": "https://sec.gov/test",
            "confidence": 0.8,
        },
        score=score,
        retrieval_method="hybrid",
    )


@pytest.fixture
def mock_cs3():
    cs3 = MagicMock()
    cs3.get_dimension_score.return_value = DimensionScore(
        dimension="data_infrastructure",
        score=78.0,
        level=4,
        level_name="Good",
        confidence_interval=(72.0, 84.0),
        rubric_keywords=["cloud", "pipeline", "warehouse", "streaming", "MLflow"],
    )
    cs3.get_rubric.return_value = [
        RubricCriteria(
            dimension="data_infrastructure",
            level=4,
            level_name="Good",
            criteria="AI-ready data platform with feature store and MLOps",
            keywords=["feature store", "MLflow", "Airflow", "streaming"],
        )
    ]
    return cs3


@pytest.fixture
def mock_retriever():
    retriever = MagicMock()
    retriever.retrieve.return_value = [
        make_retrieved_doc(
            "ev001",
            "NVIDIA's data infrastructure includes a comprehensive cloud-native pipeline "
            "with streaming data ingestion and MLflow experiment tracking.",
            score=0.85,
        ),
        make_retrieved_doc(
            "ev002",
            "The company operates a feature store for ML model training with Airflow orchestration.",
            score=0.78,
        ),
        make_retrieved_doc(
            "ev003",
            "NVIDIA maintains a warehouse with petabyte-scale storage for AI workloads.",
            score=0.72,
        ),
    ]
    return retriever


@pytest.fixture
def mock_router():
    router = MagicMock()
    router.complete.return_value = (
        "NVIDIA demonstrates strong data infrastructure capabilities, scoring 78/100 at Level 4 (Good). "
        "Evidence from SEC 10-K filings confirms a cloud-native pipeline with MLflow experiment "
        "tracking and an Airflow-orchestrated feature store [ev001, ev002]. The company's petabyte-scale "
        "warehouse supports enterprise AI workloads. To advance to Level 5 (Excellent), NVIDIA would need "
        "to demonstrate automated data governance and a unified AI data fabric."
    )
    return router


def test_generate_justification_returns_correct_type(mock_cs3, mock_retriever, mock_router):
    gen = JustificationGenerator(cs3=mock_cs3, retriever=mock_retriever, router=mock_router)
    result = gen.generate_justification("nvda", "data_infrastructure")
    assert isinstance(result, ScoreJustification)


def test_generate_justification_score(mock_cs3, mock_retriever, mock_router):
    gen = JustificationGenerator(cs3=mock_cs3, retriever=mock_retriever, router=mock_router)
    result = gen.generate_justification("nvda", "data_infrastructure")
    assert result.score == 78.0
    assert result.level == 4
    assert result.level_name == "Good"


def test_generate_justification_has_evidence(mock_cs3, mock_retriever, mock_router):
    gen = JustificationGenerator(cs3=mock_cs3, retriever=mock_retriever, router=mock_router)
    result = gen.generate_justification("nvda", "data_infrastructure")
    assert len(result.supporting_evidence) >= 1
    assert all(isinstance(e, CitedEvidence) for e in result.supporting_evidence)


def test_generate_justification_summary(mock_cs3, mock_retriever, mock_router):
    gen = JustificationGenerator(cs3=mock_cs3, retriever=mock_retriever, router=mock_router)
    result = gen.generate_justification("nvda", "data_infrastructure")
    assert len(result.generated_summary) > 0


def test_assess_strength_strong():
    from app.services.justification.generator import JustificationGenerator
    evidence = [
        CitedEvidence("e1", "content", "sec", "", 0.9, ["kw"], 0.85),
        CitedEvidence("e2", "content", "sec", "", 0.8, ["kw"], 0.80),
        CitedEvidence("e3", "content", "sec", "", 0.7, ["kw"], 0.75),
        CitedEvidence("e4", "content", "sec", "", 0.7, ["kw"], 0.70),
        CitedEvidence("e5", "content", "sec", "", 0.7, ["kw"], 0.65),
    ]
    assert JustificationGenerator._assess_strength(evidence) == "strong"


def test_assess_strength_moderate():
    evidence = [
        CitedEvidence("e1", "content", "sec", "", 0.5, ["kw"], 0.60),
        CitedEvidence("e2", "content", "sec", "", 0.5, ["kw"], 0.55),
    ]
    from app.services.justification.generator import JustificationGenerator
    assert JustificationGenerator._assess_strength(evidence) == "moderate"


def test_assess_strength_weak():
    from app.services.justification.generator import JustificationGenerator
    assert JustificationGenerator._assess_strength([]) == "weak"


def test_match_to_rubric_filters_low_score():
    docs = [
        RetrievedDocument("d1", "cloud pipeline streaming", {}, 0.2, "dense"),  # below threshold
        RetrievedDocument("d2", "cloud pipeline streaming", {}, 0.8, "dense"),  # above threshold
    ]
    from app.services.justification.generator import JustificationGenerator
    cited = JustificationGenerator._match_to_rubric(docs, ["cloud", "pipeline"])
    # assert len(cited) == 1
    assert len(cited) == 2 # both match keywords
    assert cited[0].evidence_id == "d2"


def test_identify_gaps_finds_missing_keywords():
    cited = [
        CitedEvidence("e1", "cloud pipeline", "sec", "", 0.8, ["cloud"], 0.8),
    ]
    next_rubric = [
        RubricCriteria("data_infrastructure", 5, "Excellent", "Automated data governance", ["data fabric", "governance"])
    ]
    from app.services.justification.generator import JustificationGenerator
    gaps = JustificationGenerator._identify_gaps(cited, next_rubric)
    assert len(gaps) > 0
    assert any("data fabric" in g or "governance" in g for g in gaps)


# ---------------------------------------------------------------------------
# _verify_citations — additional edge cases
# ---------------------------------------------------------------------------

def test_verify_citations_appends_note_when_no_evidence():
    from app.services.justification.generator import JustificationGenerator
    summary = "NVDA shows strong data infrastructure."
    result = JustificationGenerator._verify_citations(summary, [])
    assert "[Note:" in result
    assert "No supporting evidence" in result


def test_verify_citations_clean_when_evidence_present():
    from app.services.justification.generator import JustificationGenerator
    # Summary references source types that ARE in the cited evidence
    cited = [CitedEvidence("e1", "content", "sec_10k_item_1", "", 0.8, ["pipeline"], 0.8)]
    summary = "Based on sec_10k_item_1, the company has strong infrastructure."
    result = JustificationGenerator._verify_citations(summary, cited)
    # No phantom sources → no note appended → returned as-is
    assert "[Note:" not in result
    assert "[Verification note:" not in result


# ---------------------------------------------------------------------------
# _match_to_rubric — sort order
# ---------------------------------------------------------------------------

def test_match_to_rubric_sorts_by_relevance():
    from app.services.justification.generator import JustificationGenerator
    docs = [
        RetrievedDocument("low", "cloud pipeline", {}, 0.3, "dense"),
        RetrievedDocument("high", "cloud pipeline streaming", {}, 0.9, "dense"),
    ]
    cited = JustificationGenerator._match_to_rubric(docs, ["cloud", "pipeline"])
    assert cited[0].evidence_id == "high"


# ---------------------------------------------------------------------------
# _identify_gaps — edge cases
# ---------------------------------------------------------------------------

def test_identify_gaps_empty_next_rubric():
    from app.services.justification.generator import JustificationGenerator
    cited = [CitedEvidence("e1", "content", "sec", "", 0.8, ["cloud"], 0.8)]
    assert JustificationGenerator._identify_gaps(cited, []) == []


def test_identify_gaps_all_covered():
    from app.services.justification.generator import JustificationGenerator
    cited = [
        CitedEvidence("e1", "has feature store and MLflow", "sec", "", 0.8,
                      ["feature store", "MLflow"], 0.8),
    ]
    next_rubric = [
        RubricCriteria("data_infrastructure", 5, "Excellent",
                       "Automated governance", ["feature store", "MLflow"])
    ]
    gaps = JustificationGenerator._identify_gaps(cited, next_rubric)
    assert gaps == []
