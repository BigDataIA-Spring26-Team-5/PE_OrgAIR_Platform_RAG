"""
scoring/hr_calculator.py — CS3 Task 6.1

Calculates H^R (Human Readiness) score based on:
- Sector baseline readiness
- Position factor adjustment

Formula:
    H^R = HR_base × (1 + δ × PF)
    
Where:
    - HR_base = sector-specific baseline readiness
    - δ = 0.15 (position adjustment factor, CORRECTED in v3.0)
    - PF = position factor from Task 6.0a
"""

from decimal import Decimal
from typing import Dict, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class HRResult:
    """H^R calculation result with breakdown."""
    hr_score: Decimal
    hr_base: Decimal
    position_factor: Decimal
    position_adjustment: Decimal  # δ × PF
    sector: str


class HRCalculator:
    """
    Calculate H^R (Human Readiness) score.
    
    H^R represents how ready the sector/market is to adopt AI,
    adjusted for the company's competitive position.
    """
    
    # Sector baseline H^R values (from CS3 Lab 6 documentation)
    SECTOR_HR_BASE: Dict[str, float] = {
        # "technology": 75.0,           # High digital maturity, abundant tech talent
         "technology": 84.0,           # was 75.0 — recalibrated for 2025-2026 AI boom
        "financial_services": 68.0,   # Heavy regulation, but strong digital innovation
        "healthcare": 62.0,           # Complex regulations, privacy concerns
        "business_services": 60.0,    # Growing adoption, service-oriented
        "retail": 55.0,               # Operational focus, cost pressure
        "manufacturing": 52.0,        # Legacy infrastructure, industrial IoT emerging
        "energy": 50.0,               # Traditional industry, slow digital adoption
        "agriculture": 45.0,          # Limited tech infrastructure
    }
    
    # Position adjustment factor (CORRECTED from v2.0)
    DELTA = 0.15  # Changed from 0.12 to 0.15 in v3.0
    
    def calculate(
        self,
        sector: str,
        position_factor: float,
    ) -> HRResult:
        """
        Calculate H^R score.
        
        Formula:
            H^R = HR_base × (1 + 0.15 × PF)
        
        Args:
            sector: Company sector (e.g., "technology", "retail")
            position_factor: Position factor from Task 6.0a [-1, 1]
        
        Returns:
            HRResult with score and breakdown
        
        Examples:
            >>> calc = HRCalculator()
            
            >>> # NVIDIA: Tech sector, strong position
            >>> result = calc.calculate("technology", 0.68)
            >>> result.hr_score
            Decimal('82.65')
            
            >>> # Dollar General: Retail, weak position
            >>> result = calc.calculate("retail", -0.28)
            >>> result.hr_score
            Decimal('52.69')
        """
        # Validate inputs
        if not -1.0 <= position_factor <= 1.0:
            raise ValueError(
                f"position_factor must be in [-1, 1], got {position_factor}"
            )
        
        # Get sector baseline
        hr_base = self.SECTOR_HR_BASE.get(sector.lower())
        if hr_base is None:
            logger.warning(
                f"Unknown sector '{sector}', using default HR_base=50.0"
            )
            hr_base = 50.0
        
        # Convert to Decimal for precision
        hr_base_decimal = Decimal(str(hr_base))
        pf_decimal = Decimal(str(position_factor))
        delta_decimal = Decimal(str(self.DELTA))
        
        # Calculate position adjustment
        position_adjustment = delta_decimal * pf_decimal
        
        # Calculate H^R
        # H^R = HR_base × (1 + δ × PF)
        hr_score = hr_base_decimal * (Decimal("1") + position_adjustment)
        
        # Bound to [0, 100]
        hr_score = max(Decimal("0"), min(Decimal("100"), hr_score))
        
        # Round to 2 decimal places
        hr_score = hr_score.quantize(Decimal("0.01"))
        
        logger.info(
            f"H^R Calculation: sector={sector}, HR_base={hr_base}, "
            f"PF={position_factor:.4f}, adjustment={float(position_adjustment):.4f}, "
            f"H^R={float(hr_score):.2f}"
        )
        
        return HRResult(
            hr_score=hr_score,
            hr_base=hr_base_decimal,
            position_factor=pf_decimal,
            position_adjustment=position_adjustment,
            sector=sector,
        )
    
    def get_sector_baseline(self, sector: str) -> float:
        """Get the baseline H^R for a sector."""
        return self.SECTOR_HR_BASE.get(sector.lower(), 50.0)
    
    def interpret_hr_score(self, hr_score: float) -> str:
        """
        Interpret H^R score into human-readable category.
        
        Args:
            hr_score: H^R score (0-100)
        
        Returns:
            Interpretation string
        """
        if hr_score >= 75:
            return "Highly Ready - Sector embraces AI, talent abundant, infrastructure mature"
        elif hr_score >= 60:
            return "Moderately Ready - Growing adoption, some barriers remain"
        elif hr_score >= 45:
            return "Developing - Early stage, significant challenges"
        else:
            return "Not Ready - Hostile environment, major obstacles"