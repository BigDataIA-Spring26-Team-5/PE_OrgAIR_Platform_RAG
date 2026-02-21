"""
scoring/orgair_calculator.py — CS3 Task 6.4

Computes the Org-AI-R composite score from V^R, H^R, and Synergy.

Formula:
    Org-AI-R = (1 − β) × [α × VR + (1 − α) × HR] + β × Synergy

Parameters:
    α = 0.60  (weight of V^R vs H^R in the base blend)
    β = 0.12  (weight of Synergy in the final score)

Result clamped to [0, 100].
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from app.scoring.utils import clamp

logger = logging.getLogger(__name__)

ALPHA = Decimal("0.60")
BETA = Decimal("0.12")


@dataclass
class OrgAIRResult:
    """Output of OrgAIRCalculator.calculate()."""
    org_air_score: Decimal        # [0, 100] quantized to 0.01
    weighted_base: Decimal        # (1−β) × [α×VR + (1−α)×HR], quantized to 0.01
    synergy_contribution: Decimal # β × synergy, quantized to 0.01
    vr_weighted: Decimal          # α × VR, quantized to 0.0001
    hr_weighted: Decimal          # (1−α) × HR, quantized to 0.0001
    synergy_score_used: Decimal   # quantized to 0.01
    alpha: Decimal                # Decimal("0.60")
    beta: Decimal                 # Decimal("0.12")


class OrgAIRCalculator:
    """Calculate Org-AI-R composite score."""

    def calculate(
        self,
        vr_score: float,
        hr_score: float,
        synergy_score: Optional[float] = None,
        alignment: Optional[float] = None,
        timing_factor: float = 1.0,
    ) -> OrgAIRResult:
        """
        Calculate Org-AI-R score.

        Formula:
            Org-AI-R = (1 − β) × [α × VR + (1 − α) × HR] + β × Synergy

        Args:
            vr_score: V^R score in [0, 100]
            hr_score: H^R score in [0, 100]
            synergy_score: Pre-computed Synergy score. If None, computed internally
                           using SynergyCalculator with the provided vr/hr/alignment/timing.
            alignment: Optional alignment override passed to SynergyCalculator.
            timing_factor: Market timing factor for SynergyCalculator (default 1.0).

        Returns:
            OrgAIRResult with org_air_score and full breakdown.

        Examples:
            >>> calc = OrgAIRCalculator()
            >>> result = calc.calculate(80.0, 70.0, synergy_score=50.40)
            >>> result.org_air_score
            Decimal('72.93')
        """
        vr_d = Decimal(str(vr_score))
        hr_d = Decimal(str(hr_score))

        # Compute or accept Synergy
        if synergy_score is None:
            from app.scoring.synergy_calculator import SynergyCalculator
            syn_result = SynergyCalculator().calculate(
                vr_score, hr_score, alignment, timing_factor
            )
            syn_d = syn_result.synergy_score
        else:
            syn_d = Decimal(str(synergy_score))

        # Weighted components
        vr_weighted = ALPHA * vr_d
        hr_weighted = (Decimal("1") - ALPHA) * hr_d
        base_combined = vr_weighted + hr_weighted

        weighted_base = (Decimal("1") - BETA) * base_combined
        synergy_contribution = BETA * syn_d

        org_air = clamp(
            (weighted_base + synergy_contribution).quantize(Decimal("0.01")),
            Decimal("0"),
            Decimal("100"),
        )

        logger.info(
            "orgair_calculated",
            extra={
                "vr_score": float(vr_d),
                "hr_score": float(hr_d),
                "synergy_score": float(syn_d),
                "vr_weighted": float(vr_weighted),
                "hr_weighted": float(hr_weighted),
                "weighted_base": float(weighted_base),
                "synergy_contribution": float(synergy_contribution),
                "org_air_score": float(org_air),
            },
        )

        return OrgAIRResult(
            org_air_score=org_air,
            weighted_base=weighted_base.quantize(Decimal("0.01")),
            synergy_contribution=synergy_contribution.quantize(Decimal("0.01")),
            vr_weighted=vr_weighted.quantize(Decimal("0.0001")),
            hr_weighted=hr_weighted.quantize(Decimal("0.0001")),
            synergy_score_used=syn_d.quantize(Decimal("0.01")),
            alpha=ALPHA,
            beta=BETA,
        )
