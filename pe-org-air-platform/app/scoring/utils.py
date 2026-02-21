"""
Decimal Utilities — Task 5.1 (CS3)
app/scoring/utils.py

Provides precision-safe decimal math for scoring calculations.
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import List


def to_decimal(value: float, places: int = 4) -> Decimal:
    """Convert float to Decimal with explicit precision."""
    return Decimal(str(value)).quantize(
        Decimal(10) ** -places, rounding=ROUND_HALF_UP
    )


def clamp(
    value: Decimal,
    min_val: Decimal = Decimal("0"),
    max_val: Decimal = Decimal("100"),
) -> Decimal:
    """Clamp value to range [min_val, max_val]."""
    return max(min_val, min(max_val, value))


def weighted_mean(values: List[Decimal], weights: List[Decimal]) -> Decimal:
    """
    Calculate weighted mean.

    Formula: Σ(value_i × weight_i) / Σ(weight_i)
    Returns Decimal("0") if all weights are zero.
    """
    if len(values) != len(weights):
        raise ValueError("values and weights must have same length")

    total_weight = sum(weights)
    if total_weight == 0:
        return Decimal("0")

    numerator = sum(v * w for v, w in zip(values, weights))
    return (numerator / total_weight).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def weighted_std_dev(
    values: List[Decimal],
    weights: List[Decimal],
    mean: Decimal,
) -> Decimal:
    """
    Calculate weighted standard deviation.

    Formula: sqrt(Σ(weight_i × (value_i - mean)²) / Σ(weight_i))
    """
    if len(values) != len(weights):
        raise ValueError("values and weights must have same length")

    total_weight = sum(weights)
    if total_weight == 0:
        return Decimal("0")

    variance_sum = sum(w * (v - mean) ** 2 for v, w in zip(values, weights))
    variance = variance_sum / total_weight

    # Decimal-safe square root via float conversion
    std = Decimal(str(float(variance) ** 0.5))
    return std.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def coefficient_of_variation(std_dev: Decimal, mean: Decimal) -> Decimal:
    """
    Calculate coefficient of variation with zero-division protection.

    Formula: CV = std_dev / mean (if mean > 0, else 0)
    """
    if mean <= 0:
        return Decimal("0")
    return (std_dev / mean).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)