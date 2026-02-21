"""
scoring/confidence_calculator.py — CS3 Task 6.3

Computes SEM-based confidence interval for a score using
the Spearman-Brown reliability formula.

Formula:
    ρ  = (n × r) / (1 + (n−1) × r)          — Spearman-Brown reliability
    SEM = σ × √(1 − ρ)                       — Standard error of measurement
    CI  = [score − 1.96×SEM, score + 1.96×SEM]

Fixed parameters:
    r = 0.70  (base inter-rater reliability)
    σ = 15.0  (assumed score standard deviation)
"""

import logging
import math
from dataclasses import dataclass
from decimal import Decimal

from app.scoring.utils import clamp

logger = logging.getLogger(__name__)


@dataclass
class CIResult:
    """Output of ConfidenceCalculator.calculate()."""
    ci_lower: Decimal     # clamped to [0, 100], quantized to 0.01
    ci_upper: Decimal     # clamped to [0, 100], quantized to 0.01
    sem: Decimal          # quantized to 0.0001
    reliability: Decimal  # ρ in [0, 1), quantized to 0.0001
    confidence: Decimal   # alias for reliability (= ρ)
    score_type: str       # "vr", "hr", or "org_air"


class ConfidenceCalculator:
    """Calculate SEM-based confidence interval for a score."""

    BASE_RELIABILITY: float = 0.70
    SIGMA: float = 15.0
    Z_95: float = 1.96

    def calculate(
        self,
        score: float,
        evidence_count: int,
        score_type: str,
    ) -> CIResult:
        """
        Calculate confidence interval using Spearman-Brown formula.

        Args:
            score: The score to compute CI for (e.g. V^R, H^R, Org-AI-R).
            evidence_count: Number of independent evidence items (>= 1).
            score_type: Label for the score type ("vr", "hr", or "org_air").

        Returns:
            CIResult with ci_lower, ci_upper, sem, reliability, confidence.

        Examples:
            >>> calc = ConfidenceCalculator()
            >>> result = calc.calculate(80.0, 5, "vr")
            >>> float(result.reliability)  # ≈ 0.9211
            0.9211...
        """
        if evidence_count < 1:
            raise ValueError(f"evidence_count must be >= 1, got {evidence_count}")

        score_d = Decimal(str(score))
        n = Decimal(str(evidence_count))
        r = Decimal("0.70")

        # Spearman-Brown: ρ = (n × r) / (1 + (n − 1) × r)
        rho = (n * r) / (Decimal("1") + (n - Decimal("1")) * r)

        # SEM = σ × √(1 − ρ)
        sem_float = self.SIGMA * math.sqrt(float(Decimal("1") - rho))
        margin = Decimal(str(self.Z_95 * sem_float))

        # CI bounds, clamped to [0, 100]
        ci_lower = clamp(
            (score_d - margin).quantize(Decimal("0.01")),
            Decimal("0"),
            Decimal("100"),
        )
        ci_upper = clamp(
            (score_d + margin).quantize(Decimal("0.01")),
            Decimal("0"),
            Decimal("100"),
        )

        rho_quantized = rho.quantize(Decimal("0.0001"))
        sem_quantized = Decimal(str(sem_float)).quantize(Decimal("0.0001"))

        logger.info(
            "confidence_calculated",
            extra={
                "score": float(score_d),
                "score_type": score_type,
                "evidence_count": evidence_count,
                "reliability": float(rho_quantized),
                "sem": float(sem_quantized),
                "ci_lower": float(ci_lower),
                "ci_upper": float(ci_upper),
            },
        )

        return CIResult(
            ci_lower=ci_lower,
            ci_upper=ci_upper,
            sem=sem_quantized,
            reliability=rho_quantized,
            confidence=rho_quantized,
            score_type=score_type,
        )
