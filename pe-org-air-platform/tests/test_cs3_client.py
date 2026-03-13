"""Unit tests for CS3Client pure functions and static methods — no HTTP calls."""
from __future__ import annotations

import pytest

from app.services.integration.cs3_client import (
    CS3Client,
    CompanyAssessment,
    DimensionScore,
    RubricCriteria,
    ScoreLevel,
    score_to_level,
)


# ---------------------------------------------------------------------------
# score_to_level
# ---------------------------------------------------------------------------

def test_score_0_is_level_1_nascent():
    level, name = score_to_level(0)
    assert level == 1
    assert name == "Nascent"


def test_score_19_is_level_1():
    level, name = score_to_level(19)
    assert level == 1
    assert name == "Nascent"


def test_score_20_is_level_2_developing():
    level, name = score_to_level(20)
    assert level == 2
    assert name == "Developing"


def test_score_50_is_level_3_adequate():
    level, name = score_to_level(50)
    assert level == 3
    assert name == "Adequate"


def test_score_60_is_level_4_good():
    level, name = score_to_level(60)
    assert level == 4
    assert name == "Good"


def test_score_80_is_level_5_excellent():
    level, name = score_to_level(80)
    assert level == 5
    assert name == "Excellent"


def test_score_100_is_level_5():
    level, name = score_to_level(100)
    assert level == 5
    assert name == "Excellent"


# ---------------------------------------------------------------------------
# ScoreLevel enum
# ---------------------------------------------------------------------------

def test_score_level_name_labels():
    assert ScoreLevel.LEVEL_5.name_label == "Excellent"
    assert ScoreLevel.LEVEL_4.name_label == "Good"
    assert ScoreLevel.LEVEL_3.name_label == "Adequate"
    assert ScoreLevel.LEVEL_2.name_label == "Developing"
    assert ScoreLevel.LEVEL_1.name_label == "Nascent"


def test_score_level_ranges():
    assert ScoreLevel.LEVEL_4.score_range == (60, 79)
    assert ScoreLevel.LEVEL_5.score_range == (80, 100)
    assert ScoreLevel.LEVEL_1.score_range == (0, 19)


# ---------------------------------------------------------------------------
# _parse_dimension_score
# ---------------------------------------------------------------------------

def test_parse_dimension_score_basic():
    data = {"score": 75.0, "level": 4, "level_name": "Good"}
    ds = CS3Client._parse_dimension_score("talent", data)
    assert ds.score == 75.0
    assert ds.level == 4
    assert ds.level_name == "Good"
    assert ds.dimension == "talent"


def test_parse_dimension_score_confidence_interval_list():
    data = {"score": 70.0, "confidence_interval": [65.0, 80.0]}
    ds = CS3Client._parse_dimension_score("talent", data)
    assert ds.confidence_interval == (65.0, 80.0)


def test_parse_dimension_score_confidence_interval_dict():
    data = {"score": 70.0, "confidence_interval": {"lower": 65.0, "upper": 80.0}}
    ds = CS3Client._parse_dimension_score("talent", data)
    assert ds.confidence_interval == (65.0, 80.0)


def test_parse_dimension_score_missing_ci():
    data = {"score": 70.0}
    ds = CS3Client._parse_dimension_score("talent", data)
    assert ds.confidence_interval == (0.0, 0.0)


# ---------------------------------------------------------------------------
# _parse_rubric
# ---------------------------------------------------------------------------

def test_parse_rubric_basic():
    data = {
        "dimension": "data_infrastructure",
        "level": 4,
        "level_name": "Good",
        "criteria": "AI-ready data platform",
        "keywords": ["MLflow", "Airflow"],
    }
    rubric = CS3Client._parse_rubric(data)
    assert rubric.dimension == "data_infrastructure"
    assert rubric.level == 4
    assert rubric.level_name == "Good"
    assert rubric.criteria == "AI-ready data platform"
    assert "MLflow" in rubric.keywords


def test_parse_rubric_uses_description_fallback():
    """When 'criteria' key is missing, falls back to 'description'."""
    data = {
        "dimension": "talent",
        "level": 3,
        "description": "Adequate ML team",
        "keywords": [],
    }
    rubric = CS3Client._parse_rubric(data)
    assert rubric.criteria == "Adequate ML team"


# ---------------------------------------------------------------------------
# _default_rubric
# ---------------------------------------------------------------------------

def test_default_rubric_data_infrastructure_all_levels():
    rubrics = CS3Client._default_rubric("data_infrastructure", None)
    assert len(rubrics) == 5
    levels = {r.level for r in rubrics}
    assert levels == {1, 2, 3, 4, 5}


def test_default_rubric_data_infrastructure_level_4():
    rubrics = CS3Client._default_rubric("data_infrastructure", 4)
    assert len(rubrics) == 1
    assert rubrics[0].level == 4
    assert rubrics[0].dimension == "data_infrastructure"


def test_default_rubric_unknown_dimension():
    rubrics = CS3Client._default_rubric("nonexistent_dimension", None)
    assert rubrics == []


# ---------------------------------------------------------------------------
# _parse_assessment — list format
# ---------------------------------------------------------------------------

def _make_client() -> CS3Client:
    """Create CS3Client without calling __init__ (avoids httpx.Client creation)."""
    return CS3Client.__new__(CS3Client)


def test_parse_assessment_list_format():
    client = _make_client()
    data = {
        "ticker": "NVDA",
        "scores": [
            {"dimension": "talent", "score": 75.0},
            {"dimension": "leadership", "score": 68.0},
        ],
    }
    assessment = client._parse_assessment(data, "NVDA")
    assert "talent" in assessment.dimension_scores
    assert "leadership" in assessment.dimension_scores
    assert assessment.dimension_scores["talent"].score == 75.0


def test_parse_assessment_dict_format():
    client = _make_client()
    data = {
        "ticker": "AAPL",
        "scores": {
            "data_infrastructure": 80.0,
            "ai_governance": 65.0,
        },
    }
    assessment = client._parse_assessment(data, "AAPL")
    assert "data_infrastructure" in assessment.dimension_scores
    assert assessment.dimension_scores["data_infrastructure"].score == 80.0
    assert assessment.dimension_scores["ai_governance"].level == 4


# ---------------------------------------------------------------------------
# get_dimension_score — matching logic (mocked HTTP)
# ---------------------------------------------------------------------------

def test_get_dimension_score_exact_match():
    client = _make_client()
    client.base_url = "http://localhost:8000/api/v1"

    mock_assessment = CompanyAssessment(
        company_id="nvda",
        ticker="NVDA",
        dimension_scores={
            "talent": DimensionScore("talent", 75.0, 4, "Good"),
        },
    )
    client.get_assessment = lambda cid: mock_assessment

    result = client.get_dimension_score("nvda", "talent")
    assert result is not None
    assert result.score == 75.0


def test_get_dimension_score_prefix_match():
    """dimension='talent' should match 'talent_skills' key via prefix logic."""
    client = _make_client()
    client.base_url = "http://localhost:8000/api/v1"

    mock_assessment = CompanyAssessment(
        company_id="nvda",
        ticker="NVDA",
        dimension_scores={
            "talent_skills": DimensionScore("talent_skills", 72.0, 4, "Good"),
        },
    )
    client.get_assessment = lambda cid: mock_assessment

    result = client.get_dimension_score("nvda", "talent")
    assert result is not None
    assert result.score == 72.0
