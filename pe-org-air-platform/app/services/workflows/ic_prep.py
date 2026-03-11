# """IC Meeting Prep Workflow — full 7-dimension package for investment committees."""
# from __future__ import annotations

# from dataclasses import dataclass, field
# from datetime import datetime
# from typing import List, Optional, Dict

# from app.services.integration.cs1_client import CS1Client, Company
# from app.services.integration.cs3_client import CS3Client, CompanyAssessment
# from app.services.justification.generator import JustificationGenerator, ScoreJustification

# DIMENSIONS = [
#     "data_infrastructure",
#     "ai_governance",
#     "technology_stack",
#     "talent",
#     "leadership",
#     "use_case_portfolio",
#     "culture",
# ]


# @dataclass
# class ICMeetingPackage:
#     company: Company
#     assessment: Optional[CompanyAssessment]
#     dimension_justifications: Dict[str, ScoreJustification]
#     executive_summary: str
#     key_strengths: List[str]
#     key_gaps: List[str]
#     risk_factors: List[str]
#     recommendation: str  # "PROCEED" | "PROCEED WITH CAUTION" | "FURTHER DILIGENCE"
#     generated_at: str
#     total_evidence_count: int
#     avg_evidence_strength: str  # "strong", "moderate", "weak"


# class ICPrepWorkflow:
#     """Orchestrates full IC meeting preparation package."""

#     def __init__(
#         self,
#         cs1: Optional[CS1Client] = None,
#         cs3: Optional[CS3Client] = None,
#         generator: Optional[JustificationGenerator] = None,
#     ):
#         self.cs1 = cs1 or CS1Client()
#         self.cs3 = cs3 or CS3Client()
#         self.generator = generator or JustificationGenerator()

#     def prepare_meeting(
#         self,
#         ticker: str,
#         focus_dimensions: Optional[List[str]] = None,
#     ) -> ICMeetingPackage:
#         """Generate full IC meeting package for a company."""
#         # Step 1: Fetch company metadata
#         company = self.cs1.get_company(ticker)
#         if company is None:
#             company = Company(
#                 company_id=ticker,
#                 ticker=ticker,
#                 name=ticker,
#                 sector="Unknown",
#             )

#         # Step 2: Fetch assessment
#         assessment = self.cs3.get_assessment(ticker)

#         # Step 3: Generate justifications for each dimension
#         dims_to_process = focus_dimensions or DIMENSIONS
#         justifications: Dict[str, ScoreJustification] = {}
#         for dim in dims_to_process:
#             try:
#                 j = self.generator.generate_justification(ticker, dim)
#                 justifications[dim] = j
#             except Exception as e:
#                 # Log and continue — don't fail entire package for one dim
#                 pass

#         # Step 4: Identify strengths (level >= 4, strong/moderate evidence)
#         strengths = self._identify_strengths(justifications)

#         # Step 5: Identify gaps (level <= 2)
#         gaps = self._identify_gaps(justifications)

#         # Step 6: Assess risks
#         risks = self._assess_risks(assessment, justifications)

#         # Step 7: Generate executive summary
#         summary = self._generate_summary(company, assessment, justifications)

#         # Step 8: Generate recommendation
#         recommendation = self._generate_recommendation(assessment, justifications, risks)

#         total_evidence = sum(
#             len(j.supporting_evidence) for j in justifications.values()
#         )
#         strength_counts = {"strong": 0, "moderate": 0, "weak": 0}
#         for j in justifications.values():
#             strength_counts[j.evidence_strength] = (
#                 strength_counts.get(j.evidence_strength, 0) + 1
#             )
#         avg_strength = max(strength_counts, key=strength_counts.get) if strength_counts else "weak"

#         return ICMeetingPackage(
#             company=company,
#             assessment=assessment,
#             dimension_justifications=justifications,
#             executive_summary=summary,
#             key_strengths=strengths,
#             key_gaps=gaps,
#             risk_factors=risks,
#             recommendation=recommendation,
#             generated_at=datetime.utcnow().isoformat(),
#             total_evidence_count=total_evidence,
#             avg_evidence_strength=avg_strength,
#         )

#     @staticmethod
#     def _identify_strengths(justifications: Dict[str, ScoreJustification]) -> List[str]:
#         strengths = []
#         for dim, j in justifications.items():
#             if j.level >= 4:
#                 label = f"{dim.replace('_', ' ').title()}: Level {j.level} ({j.score:.0f}/100) — {j.level_name}"
#                 if j.evidence_strength == "weak":
#                     label += " (score-driven; limited evidence)"
#                 strengths.append(label)
#         return strengths

#     @staticmethod
#     def _identify_gaps(justifications: Dict[str, ScoreJustification]) -> List[str]:
#         gaps = []
#         for dim, j in justifications.items():
#             if j.level <= 2:
#                 gaps.append(
#                     f"{dim.replace('_', ' ').title()}: Level {j.level} ({j.score:.0f}/100) — {j.level_name}"
#                 )
#         return gaps

#     @staticmethod
#     def _assess_risks(
#         assessment: Optional[CompanyAssessment],
#         justifications: Dict[str, ScoreJustification],
#     ) -> List[str]:
#         risks = []
#         if assessment:
#             if assessment.talent_concentration > 0.7:
#                 risks.append(
#                     f"High talent concentration risk (TC={assessment.talent_concentration:.2f}) — "
#                     "key-person dependency"
#                 )
#             if assessment.valuation_risk > 0.6:
#                 risks.append(
#                     f"Elevated valuation risk (V^R={assessment.valuation_risk:.2f})"
#                 )
#             if assessment.position_factor < -0.3:
#                 risks.append(
#                     f"Negative position factor (PF={assessment.position_factor:.2f}) — "
#                     "unfavorable market positioning"
#                 )
#         # Weak evidence dimensions
#         weak_dims = [
#             dim for dim, j in justifications.items()
#             if j.evidence_strength == "weak"
#         ]
#         if weak_dims:
#             risks.append(
#                 f"Insufficient evidence for: {', '.join(weak_dims)} — further diligence required"
#             )
#         return risks

#     @staticmethod
#     def _generate_summary(
#         company: Company,
#         assessment: Optional[CompanyAssessment],
#         justifications: Dict[str, ScoreJustification],
#     ) -> str:
#         n_dims = len(justifications)
#         avg_score = (
#             sum(j.score for j in justifications.values()) / n_dims
#             if n_dims > 0 else 0.0
#         )
#         org_air_str = (
#             f"{assessment.org_air_score:.1f}"
#             if assessment and assessment.org_air_score
#             else "not yet computed"
#         )
#         return (
#             f"{company.name} ({company.ticker}) demonstrates an average AI readiness score of "
#             f"{avg_score:.0f}/100 across {n_dims} assessed dimensions, with an Org-AI-R composite "
#             f"of {org_air_str}. The company operates in {company.sector} with approximately "
#             f"{company.employee_count:,} employees and ${company.revenue_millions:.0f}M revenue. "
#             f"Key differentiators and risk factors are detailed in the dimension-level justifications below."
#         )

#     @staticmethod
#     def _generate_recommendation(
#         assessment: Optional[CompanyAssessment],
#         justifications: Dict[str, ScoreJustification],
#         risks: List[str],
#     ) -> str:
#         if not justifications:
#             return "FURTHER DILIGENCE"
#         avg_score = sum(j.score for j in justifications.values()) / len(justifications)
#         n_weak = sum(1 for j in justifications.values() if j.level <= 2)
#         n_high_risk = len([r for r in risks if "High" in r or "Elevated" in r])
#         n_weak_evidence = sum(1 for j in justifications.values() if j.evidence_strength == "weak")

#         if avg_score >= 65 and n_weak == 0 and n_high_risk == 0 and n_weak_evidence <= 1:
#             return "PROCEED"
#         if avg_score >= 45 and n_weak <= 2 and n_weak_evidence <= 3:
#             return "PROCEED WITH CAUTION"
#         return "FURTHER DILIGENCE"
"""IC Meeting Prep Workflow — full 7-dimension package for investment committees."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict

from app.services.integration.cs1_client import CS1Client, Company
from app.services.integration.cs3_client import CS3Client, CompanyAssessment
from app.services.justification.generator import JustificationGenerator, ScoreJustification

logger = logging.getLogger(__name__)

DIMENSIONS = [
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent",
    "leadership",
    "use_case_portfolio",
    "culture",
]


@dataclass
class ICMeetingPackage:
    company: Company
    assessment: Optional[CompanyAssessment]
    dimension_justifications: Dict[str, ScoreJustification]
    executive_summary: str
    key_strengths: List[str]
    key_gaps: List[str]
    risk_factors: List[str]
    recommendation: str  # "PROCEED" | "PROCEED WITH CAUTION" | "FURTHER DILIGENCE"
    generated_at: str
    total_evidence_count: int
    avg_evidence_strength: str  # "strong", "moderate", "weak"


class ICPrepWorkflow:
    """Orchestrates full IC meeting preparation package.

    NOTE: prepare_meeting() is async to support:
      - Non-blocking CS1 API calls (CS1Client uses httpx.AsyncClient)
      - Concurrent dimension justification generation via asyncio.gather()
      - FastAPI async endpoint compatibility
      - Future streaming support when switching to Claude
    """

    def __init__(
        self,
        cs1: Optional[CS1Client] = None,
        cs3: Optional[CS3Client] = None,
        generator: Optional[JustificationGenerator] = None,
    ):
        self.cs1 = cs1 or CS1Client()
        self.cs3 = cs3 or CS3Client()
        self.generator = generator or JustificationGenerator()

    async def prepare_meeting(
        self,
        ticker: str,
        focus_dimensions: Optional[List[str]] = None,
    ) -> ICMeetingPackage:
        """Generate full IC meeting package for a company.

        Async because:
          1. CS1Client.get_company() uses httpx.AsyncClient (requires await)
          2. Dimension justifications run concurrently via asyncio.gather()
             instead of sequentially — faster IC prep
        """
        # Step 1: Fetch company metadata (async — CS1Client uses AsyncClient)
        company = await self.cs1.get_company(ticker)
        if company is None:
            company = Company(
                company_id=ticker,
                ticker=ticker,
                name=ticker,
                sector="Unknown",
            )

        # Step 2: Fetch assessment (sync — CS3Client still uses sync httpx.Client)
        assessment = self.cs3.get_assessment(ticker)

        # Step 3: Generate justifications concurrently for all dimensions
        # asyncio.gather() runs all 7 dimensions in parallel instead of sequentially
        # This is the main performance benefit of making this method async
        dims_to_process = focus_dimensions or DIMENSIONS

        async def _safe_justify(dim: str) -> tuple[str, Optional[ScoreJustification]]:
            """Wrap sync justification call — returns (dim, result) or (dim, None) on error."""
            try:
                # JustificationGenerator is still sync — run in thread pool
                # to avoid blocking the event loop
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None,
                    self.generator.generate_justification,
                    ticker,
                    dim,
                )
                return dim, result
            except Exception as e:
                logger.warning("justification_failed dim=%s ticker=%s error=%s", dim, ticker, e)
                return dim, None

        results = await asyncio.gather(*[_safe_justify(dim) for dim in dims_to_process])
        justifications: Dict[str, ScoreJustification] = {
            dim: j for dim, j in results if j is not None
        }

        # Step 4: Identify strengths (level >= 4, strong/moderate evidence)
        strengths = self._identify_strengths(justifications)

        # Step 5: Identify gaps (level <= 2)
        gaps = self._identify_gaps(justifications)

        # Step 6: Assess risks
        risks = self._assess_risks(assessment, justifications)

        # Step 7: Generate executive summary
        summary = self._generate_summary(company, assessment, justifications)

        # Step 8: Generate recommendation
        recommendation = self._generate_recommendation(assessment, justifications, risks)

        total_evidence = sum(
            len(j.supporting_evidence) for j in justifications.values()
        )
        strength_counts = {"strong": 0, "moderate": 0, "weak": 0}
        for j in justifications.values():
            strength_counts[j.evidence_strength] = (
                strength_counts.get(j.evidence_strength, 0) + 1
            )
        avg_strength = max(strength_counts, key=strength_counts.get) if strength_counts else "weak"

        return ICMeetingPackage(
            company=company,
            assessment=assessment,
            dimension_justifications=justifications,
            executive_summary=summary,
            key_strengths=strengths,
            key_gaps=gaps,
            risk_factors=risks,
            recommendation=recommendation,
            generated_at=datetime.utcnow().isoformat(),
            total_evidence_count=total_evidence,
            avg_evidence_strength=avg_strength,
        )

    @staticmethod
    def _identify_strengths(justifications: Dict[str, ScoreJustification]) -> List[str]:
        strengths = []
        for dim, j in justifications.items():
            if j.level >= 4:
                label = f"{dim.replace('_', ' ').title()}: Level {j.level} ({j.score:.0f}/100) — {j.level_name}"
                if j.evidence_strength == "weak":
                    label += " (score-driven; limited evidence)"
                strengths.append(label)
        return strengths

    @staticmethod
    def _identify_gaps(justifications: Dict[str, ScoreJustification]) -> List[str]:
        gaps = []
        for dim, j in justifications.items():
            if j.level <= 2:
                gaps.append(
                    f"{dim.replace('_', ' ').title()}: Level {j.level} ({j.score:.0f}/100) — {j.level_name}"
                )
        return gaps

    @staticmethod
    def _assess_risks(
        assessment: Optional[CompanyAssessment],
        justifications: Dict[str, ScoreJustification],
    ) -> List[str]:
        risks = []
        if assessment:
            if assessment.talent_concentration > 0.7:
                risks.append(
                    f"High talent concentration risk (TC={assessment.talent_concentration:.2f}) — "
                    "key-person dependency"
                )
            if assessment.valuation_risk > 0.6:
                risks.append(
                    f"Elevated valuation risk (V^R={assessment.valuation_risk:.2f})"
                )
            if assessment.position_factor < -0.3:
                risks.append(
                    f"Negative position factor (PF={assessment.position_factor:.2f}) — "
                    "unfavorable market positioning"
                )
        # Weak evidence dimensions
        weak_dims = [
            dim for dim, j in justifications.items()
            if j.evidence_strength == "weak"
        ]
        if weak_dims:
            risks.append(
                f"Insufficient evidence for: {', '.join(weak_dims)} — further diligence required"
            )
        return risks

    @staticmethod
    def _generate_summary(
        company: Company,
        assessment: Optional[CompanyAssessment],
        justifications: Dict[str, ScoreJustification],
    ) -> str:
        n_dims = len(justifications)
        avg_score = (
            sum(j.score for j in justifications.values()) / n_dims
            if n_dims > 0 else 0.0
        )
        org_air_str = (
            f"{assessment.org_air_score:.1f}"
            if assessment and assessment.org_air_score
            else "not yet computed"
        )
        employee_str = (
            f"{company.employee_count:,}" if company.employee_count else "N/A"
        )
        revenue_str = (
            f"${company.revenue_millions:.0f}M" if company.revenue_millions else "N/A"
        )
        return (
            f"{company.name} ({company.ticker}) demonstrates an average AI readiness score of "
            f"{avg_score:.0f}/100 across {n_dims} assessed dimensions, with an Org-AI-R composite "
            f"of {org_air_str}. The company operates in {company.sector} with approximately "
            f"{employee_str} employees and {revenue_str} revenue. "
            f"Key differentiators and risk factors are detailed in the dimension-level justifications below."
        )

    @staticmethod
    def _generate_recommendation(
        assessment: Optional[CompanyAssessment],
        justifications: Dict[str, ScoreJustification],
        risks: List[str],
    ) -> str:
        if not justifications:
            return "FURTHER DILIGENCE"
        avg_score = sum(j.score for j in justifications.values()) / len(justifications)
        n_weak = sum(1 for j in justifications.values() if j.level <= 2)
        n_high_risk = len([r for r in risks if "High" in r or "Elevated" in r])
        n_weak_evidence = sum(
            1 for j in justifications.values() if j.evidence_strength == "weak"
        )

        if avg_score >= 65 and n_weak == 0 and n_high_risk == 0 and n_weak_evidence <= 1:
            return "PROCEED"
        if avg_score >= 45 and n_weak <= 2 and n_weak_evidence <= 3:
            return "PROCEED WITH CAUTION"
        return "FURTHER DILIGENCE"