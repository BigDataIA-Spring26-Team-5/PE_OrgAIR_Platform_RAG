"""Dimension Mapper — maps signal categories to CS3 dimension weights."""
from __future__ import annotations

from typing import Dict, List, Optional

# Exact weights from CS3 Task 5.0a mapping matrix
SIGNAL_TO_DIMENSION_MAP: Dict[str, Dict[str, float]] = {
    "technology_hiring": {
        "talent": 0.70,
        "technology_stack": 0.20,
        "culture": 0.10,
    },
    "innovation_activity": {
        "technology_stack": 0.50,
        "use_case_portfolio": 0.30,
        "data_infrastructure": 0.20,
    },
    "digital_presence": {
        "data_infrastructure": 0.60,
        "technology_stack": 0.40,
    },
    "leadership_signals": {
        "leadership": 0.60,
        "ai_governance": 0.25,
        "culture": 0.15,
    },
    "culture_signals": {
        "culture": 0.80,
        "talent": 0.10,
        "leadership": 0.10,
    },
    "governance_signals": {
        "ai_governance": 0.70,
        "leadership": 0.30,
    },
}

# Maps source_type → signal_category
SOURCE_TO_SIGNAL: Dict[str, str] = {
    "sec_10k_item_1": "digital_presence",
    "sec_10k_item_1a": "governance_signals",
    "sec_10k_item_7": "innovation_activity",
    "job_posting_linkedin": "technology_hiring",
    "job_posting_indeed": "technology_hiring",
    "patent_uspto": "innovation_activity",
    "glassdoor_review": "culture_signals",
    "board_proxy_def14a": "governance_signals",
    "analyst_interview": "leadership_signals",
    "dd_data_room": "digital_presence",
}

# Fallback: map unknown signal categories to a single dimension equally
_FALLBACK_DIMENSION = "data_infrastructure"


class DimensionMapper:
    """Maps evidence signal categories to CS3 dimension weights."""

    def get_dimension_weights(self, signal_category: str) -> Dict[str, float]:
        """Return dimension → weight mapping for a signal category."""
        return SIGNAL_TO_DIMENSION_MAP.get(signal_category, {_FALLBACK_DIMENSION: 1.0})

    def get_primary_dimension(self, signal_category: str) -> str:
        """Return the highest-weighted dimension for a signal category."""
        weights = self.get_dimension_weights(signal_category)
        return max(weights, key=weights.get)

    def get_all_dimensions_for_evidence(
        self, signal_category: str, min_weight: float = 0.1
    ) -> List[str]:
        """Return all dimensions with weight >= min_weight."""
        weights = self.get_dimension_weights(signal_category)
        return [dim for dim, w in weights.items() if w >= min_weight]

    def signal_from_source(self, source_type: str) -> str:
        """Infer signal category from source type."""
        return SOURCE_TO_SIGNAL.get(source_type, "digital_presence")
