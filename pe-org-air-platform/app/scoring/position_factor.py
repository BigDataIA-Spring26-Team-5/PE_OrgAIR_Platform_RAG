# scoring/position_factor.py
"""
Position Factor Calculator — Task 6.0a (CS3)

PF = 0.6 * VR_component + 0.4 * MCap_component

Where:
  VR_component = (vr_score - sector_avg_vr) / 50, clamped to [-1, 1]
  MCap_component = (market_cap_percentile - 0.5) * 2

SECTOR_AVG_VR calibration (v2):
  Back-calculated from CS3 Table 5 expected ranges:
  - NVDA: PF expected 0.7-1.0, MCap=0.95, V^R≈80 → sector_avg≈50
  - JPM:  PF expected 0.3-0.7, MCap=0.85, V^R≈67 → sector_avg≈55 (unchanged)
  - GE:   PF expected -0.2-0.2, MCap=0.50, V^R≈48 → sector_avg≈45 (unchanged)
  - WMT:  PF expected 0.1-0.5, MCap=0.60, V^R≈67 → sector_avg≈48 (unchanged)
  - DG:   PF expected -0.5--0.1, MCap=0.30, V^R≈36 → sector_avg≈48 (unchanged)

  Technology was 65 (too high — implies avg tech company has 65/100 AI readiness).
  Lowered to 50 which is the neutral midpoint and consistent with CS3 targets.
"""

from decimal import Decimal
from typing import Dict


class PositionFactorCalculator:
    """
    Calculate position factor for H^R.
    
    IMPORTANT: This calculator does NOT calculate VR.
    It receives VR as an input that was already calculated.
    """
    
    SECTOR_AVG_VR: Dict[str, float] = {
        "technology": 50.0,          # was 65.0 — recalibrated from CS3 Table 5
        "financial_services": 55.0,  # unchanged — JPM validates at 55
        "healthcare": 52.0,          # unchanged
        "business_services": 50.0,   # unchanged
        "retail": 48.0,              # unchanged — WMT and DG validate at 48
        "manufacturing": 45.0,       # unchanged — GE validates at 45
    }
    
    def calculate_position_factor(
        self,
        vr_score: float,  # ← Already calculated by VRCalculator
        sector: str,
        market_cap_percentile: float,
    ) -> Decimal:
        """
        Calculate position factor from pre-calculated V^R and market cap.
        
        Args:
            vr_score: Company's V^R score (0-100) - ALREADY CALCULATED
            sector: Company sector
            market_cap_percentile: Manual input (0-1)
        
        Returns:
            Position factor in [-1, 1]
        """
        # Validate inputs
        if not 0.0 <= market_cap_percentile <= 1.0:
            raise ValueError(f"market_cap_percentile must be in [0, 1]")
        
        if not 0 <= vr_score <= 100:
            raise ValueError(f"vr_score must be in [0, 100]")
        
        # Get sector average
        sector_avg = self.SECTOR_AVG_VR.get(sector.lower(), 50.0)
        
        # Calculate VR component
        vr_diff = vr_score - sector_avg
        vr_component = max(-1, min(1, vr_diff / 50))
        
        # Calculate market cap component
        mcap_component = (market_cap_percentile - 0.5) * 2
        
        # Weighted combination
        pf = 0.6 * vr_component + 0.4 * mcap_component
        
        # Bound to [-1, 1]
        pf_bounded = max(-1, min(1, pf))
        return Decimal(str(pf_bounded)).quantize(Decimal("0.0001"))