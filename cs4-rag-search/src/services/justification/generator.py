"""Generate score justifications with cited evidence."""
from dataclasses import dataclass, field
from typing import List, Optional
from services.integration.cs3_client import CS3Client, Dimension, DimensionScore, RubricCriteria, ScoreLevel
from services.retrieval.hybrid import HybridRetriever, RetrievedDocument
from services.llm.router import ModelRouter, TaskType

@dataclass
class CitedEvidence:
    """Evidence with citation details."""
    evidence_id: str
    content: str  # Truncated to 500 chars
    source_type: str
    source_url: Optional[str]
    confidence: float
    matched_keywords: List[str]
    relevance_score: float

@dataclass
class ScoreJustification:
    """Complete score justification for IC presentation."""
    company_id: str
    dimension: Dimension
    score: float
    level: int
    level_name: str
    confidence_interval: tuple

    rubric_criteria: str
    rubric_keywords: List[str]

    supporting_evidence: List[CitedEvidence]
    gaps_identified: List[str]

    generated_summary: str
    evidence_strength: str  # "strong", "moderate", "weak"

JUSTIFICATION_PROMPT = """You are a PE analyst preparing score justification for an investment committee.

COMPANY: {company_id}
DIMENSION: {dimension}
SCORE: {score}/100 (Level {level} - {level_name})

RUBRIC CRITERIA FOR THIS LEVEL:
{rubric_criteria}

RUBRIC KEYWORDS TO MATCH:
{rubric_keywords}

SUPPORTING EVIDENCE FOUND:
{evidence_text}

Generate a concise IC-ready justification (150-200 words) that:
1. States why the score matches this rubric level
2. Cites specific evidence with source references [Source Type, FY]
3. Identifies gaps preventing a higher score
4. Assesses evidence strength (strong/moderate/weak)

Write in professional PE memo style."""

class JustificationGenerator:
    """Generate score justifications."""

    def __init__(self):
        self.cs3 = CS3Client()
        self.retriever = HybridRetriever()
        self.router = ModelRouter()

    async def generate_justification(
        self, company_id: str, dimension: Dimension
    ) -> ScoreJustification:
        """Generate full justification for a dimension score."""

        # 1. Fetch score from CS3
        score = await self.cs3.get_dimension_score(company_id, dimension)

        # 2. Get rubric for that level
        rubrics = await self.cs3.get_rubric(dimension, score.level)
        rubric = rubrics[0] if rubrics else None

        # 3. Build search query from rubric keywords
        query = " ".join(rubric.keywords[:5]) if rubric else dimension.value.replace("_", " ")

        # 4. Search for evidence
        results = await self.retriever.retrieve(
            query=query, k=15,
            filter_metadata={"company_id": company_id, "dimension": dimension.value}
        )

        # 5. Match evidence to rubric keywords
        cited = self._match_to_rubric(results, rubric)

        # 6. Identify gaps (next level criteria not found)
        gaps = await self._identify_gaps(company_id, dimension, score.level, results)

        # 7. Generate summary with LLM
        evidence_text = "\n".join([
            f"[{e.source_type}, conf={e.confidence:.2f}] {e.content[:300]}..."
            for e in cited[:5]
        ])

        response = await self.router.complete(
            task=TaskType.JUSTIFICATION_GENERATION,
            messages=[{"role": "user", "content": JUSTIFICATION_PROMPT.format(
                company_id=company_id,
                dimension=dimension.value.replace("_", " ").title(),
                score=score.score,
                level=score.level.value,
                level_name=score.level.name_label,
                rubric_criteria=rubric.criteria_text if rubric else "N/A",
                rubric_keywords=", ".join(rubric.keywords) if rubric else "N/A",
                evidence_text=evidence_text,
            )}]
        )

        summary = response.choices[0].message.content
        strength = self._assess_strength(cited)

        return ScoreJustification(
            company_id=company_id,
            dimension=dimension,
            score=score.score,
            level=score.level.value,
            level_name=score.level.name_label,
            confidence_interval=score.confidence_interval,
            rubric_criteria=rubric.criteria_text if rubric else "",
            rubric_keywords=rubric.keywords if rubric else [],
            supporting_evidence=cited,
            gaps_identified=gaps,
            generated_summary=summary,
            evidence_strength=strength,
        )

    def _match_to_rubric(
        self, results: List[RetrievedDocument], rubric: Optional[RubricCriteria]
    ) -> List[CitedEvidence]:
        if not rubric:
            return []

        cited = []
        for r in results:
            matched = [kw for kw in rubric.keywords if kw.lower() in r.content.lower()]
            if matched or r.score > 0.7:
                cited.append(CitedEvidence(
                    evidence_id=r.doc_id,
                    content=r.content[:500],
                    source_type=r.metadata.get("source_type", "unknown"),
                    source_url=r.metadata.get("source_url"),
                    confidence=r.metadata.get("confidence", 0.5),
                    matched_keywords=matched,
                    relevance_score=r.score,
                ))
        return sorted(cited, key=lambda x: len(x.matched_keywords), reverse=True)[:5]

    async def _identify_gaps(
        self, company_id: str, dimension: Dimension,
        current_level: ScoreLevel, evidence: List[RetrievedDocument]
    ) -> List[str]:
        """Find criteria from next level not matched in evidence."""
        if current_level == ScoreLevel.LEVEL_5:
            return []  # Already at top

        next_level = ScoreLevel(current_level.value + 1)
        next_rubrics = await self.cs3.get_rubric(dimension, next_level)
        if not next_rubrics:
            return []

        next_rubric = next_rubrics[0]
        evidence_text = " ".join([e.content.lower() for e in evidence])

        gaps = []
        for kw in next_rubric.keywords:
            if kw.lower() not in evidence_text:
                gaps.append(f"No evidence of '{kw}' (Level {next_level.value} criterion)")

        return gaps[:5]

    def _assess_strength(self, evidence: List[CitedEvidence]) -> str:
        if not evidence:
            return "weak"
        avg_conf = sum(e.confidence for e in evidence) / len(evidence)
        avg_matches = sum(len(e.matched_keywords) for e in evidence) / len(evidence)

        if avg_conf >= 0.8 and avg_matches >= 2:
            return "strong"
        elif avg_conf >= 0.6 or avg_matches >= 1:
            return "moderate"
        return "weak"
