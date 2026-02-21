"""
scoring/synergy_calculator.py — CS3 Task 6.2

Computes Synergy score from V^R and H^R scores.

Formula:
    Synergy = (VR × HR / 100) × Alignment × TimingFactor

Where:
    - Alignment = 1 − |VR − HR| / 100  (auto-computed if not provided)
    - TimingFactor clamped to [0.8, 1.2]
    - Result clamped to [0, 100]
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

from app.scoring.utils import clamp

logger = logging.getLogger(__name__)


@dataclass
class SynergyResult:
    """Output of SynergyCalculator.calculate()."""
    synergy_score: Decimal   # [0, 100] quantized to 0.01
    vr_used: Decimal
    hr_used: Decimal
    alignment: Decimal       # [0, 1] quantized to 0.0001
    timing_factor: Decimal   # clamped [0.8, 1.2] quantized to 0.0001


class SynergyCalculator:
    """Calculate Synergy score from V^R and H^R."""

    TIMING_MIN = Decimal("0.8")
    TIMING_MAX = Decimal("1.2")

    def calculate(
        self,
        vr_score: float,
        hr_score: float,
        alignment: float | None = None,
        timing_factor: float = 1.0,
    ) -> SynergyResult:
        """
        Calculate Synergy score.

        Formula:
            Synergy = (VR × HR / 100) × Alignment × TimingFactor

        Args:
            vr_score: V^R score in [0, 100]
            hr_score: H^R score in [0, 100]
            alignment: Optional alignment value in [0, 1].
                       If None, auto-computed as 1 − |VR − HR| / 100.
            timing_factor: Market timing factor, clamped to [0.8, 1.2].

        Returns:
            SynergyResult with synergy_score and breakdown.

        Examples:
            >>> calc = SynergyCalculator()
            >>> result = calc.calculate(80.0, 70.0)
            >>> result.synergy_score
            Decimal('50.40')
        """
        vr_d = Decimal(str(vr_score))
        hr_d = Decimal(str(hr_score))

        # Clamp timing factor to [0.8, 1.2]
        tf_d = clamp(
            Decimal(str(timing_factor)),
            self.TIMING_MIN,
            self.TIMING_MAX,
        )

        # Compute alignment
        if alignment is None:
            alignment_d = Decimal("1") - abs(vr_d - hr_d) / Decimal("100")
        else:
            alignment_d = clamp(
                Decimal(str(alignment)),
                Decimal("0"),
                Decimal("1"),
            )

        # Synergy = (VR × HR / 100) × Alignment × TimingFactor
        raw = (vr_d * hr_d / Decimal("100")) * alignment_d * tf_d

        synergy = clamp(
            raw.quantize(Decimal("0.01")),
            Decimal("0"),
            Decimal("100"),
        )

        logger.info(
            "synergy_calculated",
            extra={
                "vr_score": float(vr_d),
                "hr_score": float(hr_d),
                "alignment": float(alignment_d),
                "timing_factor": float(tf_d),
                "synergy_score": float(synergy),
            },
        )

        return SynergyResult(
            synergy_score=synergy,
            vr_used=vr_d,
            hr_used=hr_d,
            alignment=alignment_d.quantize(Decimal("0.0001")),
            timing_factor=tf_d.quantize(Decimal("0.0001")),
        )
