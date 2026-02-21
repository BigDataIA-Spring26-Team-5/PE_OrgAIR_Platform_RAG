# app/scoring/vr_calculator.py
"""
V^R Calculator — CS3 Task 5.2
-------------------------------
Computes the V^R (Venture Readiness) score from 7 dimension scores and a
Talent Concentration (TC) value.

Formula:
    weighted_dim = Σ (dim_score × dim_weight) / Σ dim_weights
    TalentRiskAdj = 1 − 0.15 × max(0, TC − 0.25)
    V^R = weighted_dim × TalentRiskAdj   clamped to [0, 100]

Dimension weights from CLAUDE.md / config.py (sum = 1.0):
    data_infrastructure  0.18
    talent_skills        0.17
    ai_governance        0.15
    technology_stack     0.15
    leadership_vision    0.13
    use_case_portfolio   0.12
    culture_change       0.10
"""
import structlog
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict

from app.scoring.utils import weighted_std_dev, coefficient_of_variation, clamp

logger = structlog.get_logger(__name__)

# Dimension weights — keys match EvidenceMapper Dimension enum values
_DIM_WEIGHTS: Dict[str, Decimal] = {
    "data_infrastructure": Decimal("0.18"),
    "talent_skills":       Decimal("0.17"),
    "ai_governance":       Decimal("0.15"),
    "technology_stack":    Decimal("0.15"),
    "leadership_vision":   Decimal("0.13"),
    "use_case_portfolio":  Decimal("0.12"),
    "culture_change":      Decimal("0.10"),
}


@dataclass
class VRResult:
    """Output of VRCalculator.calculate()."""
    vr_score: Decimal            # Final V^R in [0, 100]
    weighted_dim_score: Decimal  # Weighted average before TC adjustment
    talent_risk_adj: Decimal     # TalentRiskAdj in [0, 1]
    cv_penalty: Decimal          # Non-compensatory CV penalty in [0, 1]
    talent_concentration: Decimal


class VRCalculator:
    """Calculate V^R score with Talent Concentration penalty."""

    def calculate(
        self,
        dimension_scores: Dict[str, float],
        talent_concentration: float,
        sector: str = "",
    ) -> VRResult:
        """
        Args:
            dimension_scores: Mapping of dimension name → score (0-100).
                              Keys must match _DIM_WEIGHTS (e.g. 'talent_skills').
                              Unknown keys are silently ignored (weight = 0).
            talent_concentration: TC value in [0, 1] from TalentConcentrationCalculator.
            sector: Optional sector string (reserved for future sector-specific weights).

        Returns:
            VRResult with vr_score, weighted_dim_score, talent_risk_adj, talent_concentration.
        """
        tc = Decimal(str(round(talent_concentration, 4)))

        # Weighted average of dimension scores — collect parallel lists for CV
        weighted_sum = Decimal("0")
        total_weight = Decimal("0")
        values: list[Decimal] = []
        weights: list[Decimal] = []
        for dim_val, score in dimension_scores.items():
            w = _DIM_WEIGHTS.get(dim_val, Decimal("0"))
            if w == 0:
                continue
            d_score = Decimal(str(float(score)))
            weighted_sum += d_score * w
            total_weight += w
            values.append(d_score)
            weights.append(w)

        weighted_dim = (weighted_sum / total_weight) if total_weight > 0 else Decimal("50")

        # TalentRiskAdj = 1 − 0.15 × max(0, TC − 0.25)
        adj = Decimal("1") - Decimal("0.15") * max(Decimal("0"), tc - Decimal("0.25"))
        adj = max(Decimal("0"), min(Decimal("1"), adj))

        # Non-compensatory CV penalty (λ = 0.25)
        std_dev = weighted_std_dev(values, weights, weighted_dim)
        cv_D = coefficient_of_variation(std_dev, weighted_dim)
        cv_penalty = clamp(
            Decimal("1") - Decimal("0.25") * cv_D,
            Decimal("0"),
            Decimal("1"),
        )

        vr = clamp(
            (weighted_dim * adj * cv_penalty).quantize(Decimal("0.01")),
            Decimal("0"),
            Decimal("100"),
        )

        logger.info(
            "vr_calculated",
            sector=sector,
            dimension_scores=dimension_scores,
            weighted_dim_score=float(weighted_dim),
            talent_concentration=float(tc),
            talent_risk_adj=float(adj),
            cv_D=float(cv_D),
            cv_penalty=float(cv_penalty),
            vr_score=float(vr),
        )

        return VRResult(
            vr_score=vr,
            weighted_dim_score=weighted_dim.quantize(Decimal("0.01")),
            talent_risk_adj=adj.quantize(Decimal("0.0001")),
            cv_penalty=cv_penalty.quantize(Decimal("0.0001")),
            talent_concentration=tc,
        )
