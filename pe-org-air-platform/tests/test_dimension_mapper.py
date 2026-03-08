"""Tests for DimensionMapper — signal→dimension weight mapping."""
from __future__ import annotations

import pytest

from app.services.retrieval.dimension_mapper import DimensionMapper, SIGNAL_TO_DIMENSION_MAP


@pytest.fixture
def mapper():
    return DimensionMapper()


def test_get_dimension_weights_technology_hiring(mapper):
    weights = mapper.get_dimension_weights("technology_hiring")
    assert weights["talent"] == 0.70
    assert weights["technology_stack"] == 0.20
    assert weights["culture"] == 0.10
    assert abs(sum(weights.values()) - 1.0) < 1e-9


def test_get_dimension_weights_all_sum_to_one(mapper):
    for signal, weights in SIGNAL_TO_DIMENSION_MAP.items():
        total = sum(weights.values())
        assert abs(total - 1.0) < 1e-9, f"Weights for {signal} sum to {total}, not 1.0"


def test_get_primary_dimension_technology_hiring(mapper):
    primary = mapper.get_primary_dimension("technology_hiring")
    assert primary == "talent"


def test_get_primary_dimension_digital_presence(mapper):
    primary = mapper.get_primary_dimension("digital_presence")
    assert primary == "data_infrastructure"


def test_get_primary_dimension_governance(mapper):
    primary = mapper.get_primary_dimension("governance_signals")
    assert primary == "ai_governance"


def test_get_all_dimensions_min_weight(mapper):
    dims = mapper.get_all_dimensions_for_evidence("technology_hiring", min_weight=0.15)
    assert "talent" in dims
    assert "technology_stack" in dims
    assert "culture" not in dims  # 0.10 < 0.15


def test_get_all_dimensions_low_min_weight(mapper):
    dims = mapper.get_all_dimensions_for_evidence("technology_hiring", min_weight=0.05)
    assert len(dims) == 3


def test_fallback_for_unknown_signal(mapper):
    weights = mapper.get_dimension_weights("unknown_signal")
    assert "data_infrastructure" in weights


def test_signal_from_source(mapper):
    assert mapper.signal_from_source("job_posting_linkedin") == "technology_hiring"
    assert mapper.signal_from_source("glassdoor_review") == "culture_signals"
    assert mapper.signal_from_source("patent_uspto") == "innovation_activity"
    assert mapper.signal_from_source("board_proxy_def14a") == "governance_signals"
