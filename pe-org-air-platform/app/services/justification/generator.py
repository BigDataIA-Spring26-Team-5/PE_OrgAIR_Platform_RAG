"""Score Justification Generator — cited evidence for IC-ready summaries."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

from app.services.integration.cs3_client import CS3Client, DimensionScore, RubricCriteria
from app.services.retrieval.hybrid import HybridRetriever, RetrievedDocument
from app.services.llm.router import ModelRouter

JUSTIFICATION_PROMPT = """You are a senior private equity analyst preparing an Investment Committee brief.

Company: {company_id}
Dimension: {dimension}
Score: {score}/100 (Level {level} — {level_name})
Confidence Interval: [{ci_low:.1f}, {ci_high:.1f}]

Rubric Criteria for Level {level}:
{rubric_criteria}

Supporting Evidence ({n_evidence} pieces):
{evidence_text}

Evidence Gaps (criteria not yet met for Level {next_level}):
{gaps_text}

Write a 150–200 word IC-ready justification paragraph that:
1. States the score and level clearly
2. Cites 2–3 specific evidence pieces with source references
3. Explains what is driving the score
4. Notes key gaps that would push to the next level
5. Uses precise, professional PE investment language

Justification:"""


@dataclass
class CitedEvidence:
    evidence_id: str
    content: str  # ≤500 chars
    source_type: str
    source_url: str
    confidence: float
    matched_keywords: List[str]
    relevance_score: float


@dataclass
class ScoreJustification:
    company_id: str
    dimension: str
    score: float
    level: int
    level_name: str
    confidence_interval: tuple[float, float]
    rubric_criteria: str
    rubric_keywords: List[str]
    supporting_evidence: List[CitedEvidence]
    gaps_identified: List[str]
    generated_summary: str
    evidence_strength: str  # "strong", "moderate", "weak"


class JustificationGenerator:
    """Generates IC-ready score justifications with cited evidence."""

    def __init__(
        self,
        cs3: Optional[CS3Client] = None,
        retriever: Optional[HybridRetriever] = None,
        router: Optional[ModelRouter] = None,
    ):
        self.cs3 = cs3 or CS3Client()
        self.retriever = retriever or HybridRetriever()
        self.router = router or ModelRouter()

    def generate_justification(
        self, ticker: str, dimension: str
    ) -> ScoreJustification:
        """Full pipeline: fetch score → retrieve evidence → generate summary."""
        # Step 1: Fetch CS3 dimension score
        dim_score = self.cs3.get_dimension_score(ticker, dimension)
        if dim_score is None:
            dim_score = DimensionScore(
                dimension=dimension, score=50.0, level=3, level_name="Adequate"
            )

        # Step 2: Get rubric criteria for this level
        rubric = self.cs3.get_rubric(dimension, dim_score.level)
        rubric_text = rubric[0].criteria if rubric else f"Level {dim_score.level} criteria"
        rubric_keywords = rubric[0].keywords if rubric else dim_score.rubric_keywords[:5]

        # Step 3: Build search query from rubric keywords
        query = f"{dimension} " + " ".join(rubric_keywords[:5])

        # Step 4: Retrieve evidence
        filter_meta: Dict[str, Any] = {"ticker": ticker}
        if dimension:
            filter_meta["dimension"] = dimension
        raw_results = self.retriever.retrieve(query, k=15, filter_metadata=filter_meta)

        # Step 5: Match to rubric keywords
        cited = self._match_to_rubric(raw_results, rubric_keywords)

        # Step 6: Identify gaps for next level
        next_level = min(dim_score.level + 1, 5)
        next_rubric = self.cs3.get_rubric(dimension, next_level)
        gaps = self._identify_gaps(cited, next_rubric)

        # Step 7: Call DeepSeek for IC summary
        ci = dim_score.confidence_interval
        evidence_text = "\n".join(
            f"- [{e.source_type}] {e.content[:300]}..." for e in cited[:5]
        ) or "No specific evidence retrieved."
        gaps_text = "\n".join(f"- {g}" for g in gaps) or "None identified."

        prompt = JUSTIFICATION_PROMPT.format(
            company_id=ticker,
            dimension=dimension,
            score=dim_score.score,
            level=dim_score.level,
            level_name=dim_score.level_name,
            ci_low=ci[0] if ci else 0.0,
            ci_high=ci[1] if ci else 0.0,
            rubric_criteria=rubric_text,
            n_evidence=len(cited),
            evidence_text=evidence_text,
            gaps_text=gaps_text,
            next_level=next_level,
        )
        messages = [
            {"role": "system", "content": "You are a senior PE investment analyst."},
            {"role": "user", "content": prompt},
        ]
        try:
            summary = self.router.complete("justification_generation", messages)
        except Exception as e:
            summary = f"[Summary generation unavailable: {e}]"

        return ScoreJustification(
            company_id=ticker,
            dimension=dimension,
            score=dim_score.score,
            level=dim_score.level,
            level_name=dim_score.level_name,
            confidence_interval=dim_score.confidence_interval or (0.0, 0.0),
            rubric_criteria=rubric_text,
            rubric_keywords=rubric_keywords,
            supporting_evidence=cited,
            gaps_identified=gaps,
            generated_summary=summary.strip(),
            evidence_strength=self._assess_strength(cited),
        )

    @staticmethod
    def _match_to_rubric(
        results: List[RetrievedDocument], keywords: List[str]
    ) -> List[CitedEvidence]:
        """Filter and rank evidence by keyword overlap and relevance."""
        cited = []
        for r in results:
            if r.score < 0.3:
                continue
            content_lower = r.content.lower()
            matched = [kw for kw in keywords if kw.lower() in content_lower]
            cited.append(
                CitedEvidence(
                    evidence_id=r.doc_id,
                    content=r.content[:500],
                    source_type=r.metadata.get("source_type", ""),
                    source_url=r.metadata.get("source_url", ""),
                    confidence=float(r.metadata.get("confidence", 0.5)),
                    matched_keywords=matched,
                    relevance_score=r.score,
                )
            )
        return sorted(cited, key=lambda x: x.relevance_score, reverse=True)

    @staticmethod
    def _identify_gaps(
        cited: List[CitedEvidence], next_rubric: List[RubricCriteria]
    ) -> List[str]:
        """Find next-level criteria not covered by current evidence."""
        if not next_rubric:
            return []
        found_keywords: set = set()
        for ev in cited:
            found_keywords.update(k.lower() for k in ev.matched_keywords)
        gaps = []
        for rubric in next_rubric:
            for kw in rubric.keywords:
                if kw.lower() not in found_keywords:
                    gaps.append(f"Missing evidence of: {kw} ({rubric.criteria[:80]})")
        return gaps[:5]  # Top 5 gaps

    @staticmethod
    def _assess_strength(cited: List[CitedEvidence]) -> str:
        if len(cited) >= 5 and any(e.confidence >= 0.7 for e in cited):
            return "strong"
        if len(cited) >= 2:
            return "moderate"
        return "weak"
