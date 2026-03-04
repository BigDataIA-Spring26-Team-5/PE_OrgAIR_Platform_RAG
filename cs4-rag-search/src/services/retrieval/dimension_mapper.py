"""Map CS2 signals to CS3 dimensions for indexing."""
from typing import Dict
from services.integration.cs2_client import SignalCategory
from services.integration.cs3_client import Dimension

# From CS3 Task 5.0a - Signal-to-Dimension Mapping Matrix
SIGNAL_TO_DIMENSION_MAP: Dict[SignalCategory, Dict[Dimension, float]] = {
    SignalCategory.TECHNOLOGY_HIRING: {
        Dimension.TALENT: 0.70,
        Dimension.TECHNOLOGY_STACK: 0.20,
        Dimension.CULTURE: 0.10,
    },
    SignalCategory.INNOVATION_ACTIVITY: {
        Dimension.TECHNOLOGY_STACK: 0.50,
        Dimension.USE_CASE_PORTFOLIO: 0.30,
        Dimension.DATA_INFRASTRUCTURE: 0.20,
    },
    SignalCategory.DIGITAL_PRESENCE: {
        Dimension.DATA_INFRASTRUCTURE: 0.60,
        Dimension.TECHNOLOGY_STACK: 0.40,
    },
    SignalCategory.LEADERSHIP_SIGNALS: {
        Dimension.LEADERSHIP: 0.60,
        Dimension.AI_GOVERNANCE: 0.25,
        Dimension.CULTURE: 0.15,
    },
    SignalCategory.CULTURE_SIGNALS: {
        Dimension.CULTURE: 0.80,
        Dimension.TALENT: 0.10,
        Dimension.LEADERSHIP: 0.10,
    },
    SignalCategory.GOVERNANCE_SIGNALS: {
        Dimension.AI_GOVERNANCE: 0.70,
        Dimension.LEADERSHIP: 0.30,
    },
}

# Source type to signal category mapping for SEC filings
SOURCE_TO_SIGNAL: Dict[str, SignalCategory] = {
    "sec_10k_item_1": SignalCategory.DIGITAL_PRESENCE,
    "sec_10k_item_1a": SignalCategory.GOVERNANCE_SIGNALS,
    "sec_10k_item_7": SignalCategory.LEADERSHIP_SIGNALS,
    "job_posting_linkedin": SignalCategory.TECHNOLOGY_HIRING,
    "job_posting_indeed": SignalCategory.TECHNOLOGY_HIRING,
    "patent_uspto": SignalCategory.INNOVATION_ACTIVITY,
    "glassdoor_review": SignalCategory.CULTURE_SIGNALS,
    "board_proxy_def14a": SignalCategory.GOVERNANCE_SIGNALS,
}

class DimensionMapper:
    """Map CS2 evidence to CS3 dimensions."""

    def get_dimension_weights(
        self, signal_category: SignalCategory
    ) -> Dict[Dimension, float]:
        """Get dimension weights for a signal category."""
        return SIGNAL_TO_DIMENSION_MAP.get(signal_category, {
            Dimension.DATA_INFRASTRUCTURE: 1.0  # Default
        })

    def get_primary_dimension(
        self, signal_category: SignalCategory
    ) -> Dimension:
        """Get primary dimension (highest weight)."""
        weights = self.get_dimension_weights(signal_category)
        return max(weights.items(), key=lambda x: x[1])[0]

    def get_all_dimensions_for_evidence(
        self,
        signal_category: SignalCategory,
        min_weight: float = 0.1
    ) -> Dict[Dimension, float]:
        """Get all dimensions with weight >= threshold."""
        weights = self.get_dimension_weights(signal_category)
        return {d: w for d, w in weights.items() if w >= min_weight}
