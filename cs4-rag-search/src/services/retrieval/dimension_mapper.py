"""
dimension_mapper.py — CS4 RAG Search
src/services/retrieval/dimension_mapper.py

Extracted from pe-org-air-platform/app/scoring/evidence_mapper.py (lines 25–212).
Provides enum definitions and DimensionMapper helper for query routing.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Dimension(str, Enum):
    """The 7 V^R dimensions. Values match the platform's Dimension enum."""
    DATA_INFRASTRUCTURE = "data_infrastructure"
    AI_GOVERNANCE = "ai_governance"
    TECHNOLOGY_STACK = "technology_stack"
    TALENT_SKILLS = "talent_skills"
    LEADERSHIP_VISION = "leadership_vision"
    USE_CASE_PORTFOLIO = "use_case_portfolio"
    CULTURE_CHANGE = "culture_change"


class SignalCategory(str, Enum):
    """
    CS2 signal categories.
    Renamed from platform's SignalSource to match CS4 spec:
      GLASSDOOR_CULTURE  (was GLASSDOOR_CULTURE → CULTURE_SIGNALS)
      GOVERNANCE_SIGNALS (was BOARD_GOVERNANCE  → GOVERNANCE_SIGNALS)
    """
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"
    # SEC sections
    SEC_ITEM_1 = "sec_item_1"
    SEC_ITEM_1A = "sec_item_1a"
    SEC_ITEM_7 = "sec_item_7"
    # CS3 sources — renamed per CS4 spec
    CULTURE_SIGNALS = "culture_signals"       # was GLASSDOOR_CULTURE / glassdoor_reviews
    GOVERNANCE_SIGNALS = "governance_signals" # was BOARD_GOVERNANCE  / board_composition


# ---------------------------------------------------------------------------
# Internal mapping models (ported from EvidenceMapper internals)
# ---------------------------------------------------------------------------

@dataclass
class DimensionMapping:
    source: SignalCategory
    primary_dimension: Dimension
    primary_weight: Decimal
    secondary_mappings: Dict[Dimension, Decimal] = field(default_factory=dict)
    reliability: Decimal = Decimal("0.80")


# ---------------------------------------------------------------------------
# Signal-to-Dimension mapping table  (Table 1, CS3 p.7)
# ---------------------------------------------------------------------------

_SIGNAL_TO_DIMENSION_MAP: Dict[SignalCategory, DimensionMapping] = {

    SignalCategory.TECHNOLOGY_HIRING: DimensionMapping(
        source=SignalCategory.TECHNOLOGY_HIRING,
        primary_dimension=Dimension.TALENT_SKILLS,
        primary_weight=Decimal("0.70"),
        secondary_mappings={
            Dimension.TECHNOLOGY_STACK:    Decimal("0.20"),
            Dimension.DATA_INFRASTRUCTURE: Decimal("0.10"),
            Dimension.CULTURE_CHANGE:      Decimal("0.10"),
        },
        reliability=Decimal("0.85"),
    ),

    SignalCategory.INNOVATION_ACTIVITY: DimensionMapping(
        source=SignalCategory.INNOVATION_ACTIVITY,
        primary_dimension=Dimension.TECHNOLOGY_STACK,
        primary_weight=Decimal("0.50"),
        secondary_mappings={
            Dimension.USE_CASE_PORTFOLIO:  Decimal("0.30"),
            Dimension.DATA_INFRASTRUCTURE: Decimal("0.20"),
        },
        reliability=Decimal("0.80"),
    ),

    SignalCategory.DIGITAL_PRESENCE: DimensionMapping(
        source=SignalCategory.DIGITAL_PRESENCE,
        primary_dimension=Dimension.DATA_INFRASTRUCTURE,
        primary_weight=Decimal("0.60"),
        secondary_mappings={
            Dimension.TECHNOLOGY_STACK: Decimal("0.40"),
        },
        reliability=Decimal("0.85"),
    ),

    SignalCategory.LEADERSHIP_SIGNALS: DimensionMapping(
        source=SignalCategory.LEADERSHIP_SIGNALS,
        primary_dimension=Dimension.LEADERSHIP_VISION,
        primary_weight=Decimal("0.60"),
        secondary_mappings={
            Dimension.AI_GOVERNANCE: Decimal("0.25"),
            Dimension.CULTURE_CHANGE: Decimal("0.15"),
        },
        reliability=Decimal("0.80"),
    ),

    SignalCategory.SEC_ITEM_1: DimensionMapping(
        source=SignalCategory.SEC_ITEM_1,
        primary_dimension=Dimension.USE_CASE_PORTFOLIO,
        primary_weight=Decimal("0.70"),
        secondary_mappings={
            Dimension.TECHNOLOGY_STACK: Decimal("0.30"),
        },
        reliability=Decimal("0.75"),
    ),

    SignalCategory.SEC_ITEM_1A: DimensionMapping(
        source=SignalCategory.SEC_ITEM_1A,
        primary_dimension=Dimension.AI_GOVERNANCE,
        primary_weight=Decimal("0.80"),
        secondary_mappings={
            Dimension.DATA_INFRASTRUCTURE: Decimal("0.20"),
        },
        reliability=Decimal("0.75"),
    ),

    SignalCategory.SEC_ITEM_7: DimensionMapping(
        source=SignalCategory.SEC_ITEM_7,
        primary_dimension=Dimension.LEADERSHIP_VISION,
        primary_weight=Decimal("0.50"),
        secondary_mappings={
            Dimension.USE_CASE_PORTFOLIO:  Decimal("0.30"),
            Dimension.DATA_INFRASTRUCTURE: Decimal("0.20"),
        },
        reliability=Decimal("0.75"),
    ),

    SignalCategory.CULTURE_SIGNALS: DimensionMapping(
        source=SignalCategory.CULTURE_SIGNALS,
        primary_dimension=Dimension.CULTURE_CHANGE,
        primary_weight=Decimal("0.80"),
        secondary_mappings={
            Dimension.TALENT_SKILLS:     Decimal("0.10"),
            Dimension.LEADERSHIP_VISION: Decimal("0.10"),
        },
        reliability=Decimal("0.70"),
    ),

    SignalCategory.GOVERNANCE_SIGNALS: DimensionMapping(
        source=SignalCategory.GOVERNANCE_SIGNALS,
        primary_dimension=Dimension.AI_GOVERNANCE,
        primary_weight=Decimal("0.70"),
        secondary_mappings={
            Dimension.LEADERSHIP_VISION: Decimal("0.30"),
        },
        reliability=Decimal("0.75"),
    ),
}

# Derived reverse-lookup: source type string → signal category
SOURCE_TO_SIGNAL: Dict[str, SignalCategory] = {
    "linkedin":            SignalCategory.TECHNOLOGY_HIRING,
    "indeed":              SignalCategory.TECHNOLOGY_HIRING,
    "builtwith":           SignalCategory.INNOVATION_ACTIVITY,
    "wappalyzer":          SignalCategory.DIGITAL_PRESENCE,
    "company_website":     SignalCategory.DIGITAL_PRESENCE,
    "sec_filing":          SignalCategory.LEADERSHIP_SIGNALS,
    "press_release":       SignalCategory.LEADERSHIP_SIGNALS,
    "uspto":               SignalCategory.INNOVATION_ACTIVITY,
    "glassdoor":           SignalCategory.CULTURE_SIGNALS,
    "board_proxy":         SignalCategory.GOVERNANCE_SIGNALS,
    # CS4 new sources
    "analyst_interview":   SignalCategory.LEADERSHIP_SIGNALS,
    "dd_data_room":        SignalCategory.INNOVATION_ACTIVITY,
}


# ---------------------------------------------------------------------------
# DimensionMapper
# ---------------------------------------------------------------------------

class DimensionMapper:
    """
    Query-routing helper: given a SignalCategory, returns the dimension(s)
    it contributes to and their weights.
    """

    def get_dimension_weights(self, signal_category: SignalCategory) -> Dict[Dimension, Decimal]:
        """Return all dimension → weight pairs for this signal category."""
        mapping = _SIGNAL_TO_DIMENSION_MAP[signal_category]
        result = {mapping.primary_dimension: mapping.primary_weight}
        result.update(mapping.secondary_mappings)
        return result

    def get_primary_dimension(self, signal_category: SignalCategory) -> Dimension:
        """Return the highest-weight (primary) dimension for this signal category."""
        return _SIGNAL_TO_DIMENSION_MAP[signal_category].primary_dimension

    def get_all_dimensions_for_evidence(
        self,
        signal_category: SignalCategory,
        min_weight: float = 0.1,
    ) -> List[Dimension]:
        """
        Return all dimensions whose weight is >= min_weight for this category.
        Useful for deciding which ChromaDB collections to query.
        """
        weights = self.get_dimension_weights(signal_category)
        threshold = Decimal(str(min_weight))
        return [dim for dim, w in weights.items() if w >= threshold]
