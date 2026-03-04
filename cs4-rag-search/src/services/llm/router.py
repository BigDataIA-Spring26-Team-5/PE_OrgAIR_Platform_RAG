"""Multi-model routing with LiteLLM and streaming support."""
from typing import AsyncIterator, Dict, Any, List
from enum import Enum
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
import litellm
from litellm import acompletion
import structlog

logger = structlog.get_logger()

class TaskType(str, Enum):
    EVIDENCE_EXTRACTION = "evidence_extraction"
    DIMENSION_SCORING = "dimension_scoring"
    JUSTIFICATION_GENERATION = "justification_generation"
    CHAT_RESPONSE = "chat_response"

@dataclass
class ModelConfig:
    primary: str
    fallbacks: List[str]
    temperature: float
    max_tokens: int
    cost_per_1k_tokens: float

MODEL_ROUTING: Dict[TaskType, ModelConfig] = {
    TaskType.EVIDENCE_EXTRACTION: ModelConfig(
        primary="gpt-4o-2024-08-06",
        fallbacks=["claude-sonnet-4-6-20250514"],
        temperature=0.3, max_tokens=4000, cost_per_1k_tokens=0.015,
    ),
    TaskType.JUSTIFICATION_GENERATION: ModelConfig(
        primary="claude-sonnet-4-6-20250514",
        fallbacks=["gpt-4o-2024-08-06"],
        temperature=0.2, max_tokens=2000, cost_per_1k_tokens=0.012,
    ),
    TaskType.CHAT_RESPONSE: ModelConfig(
        primary="claude-haiku-4-5-20251001",
        fallbacks=["gpt-3.5-turbo"],
        temperature=0.7, max_tokens=1000, cost_per_1k_tokens=0.002,
    ),
}

@dataclass
class DailyBudget:
    date: date = field(default_factory=date.today)
    spent_usd: Decimal = Decimal("0")
    limit_usd: Decimal = Decimal("50.00")

    def can_spend(self, amount: Decimal) -> bool:
        if self.date != date.today():
            self.date, self.spent_usd = date.today(), Decimal("0")
        return self.spent_usd + amount <= self.limit_usd

class ModelRouter:
    """Route LLM requests with fallbacks and cost tracking."""

    def __init__(self, daily_limit_usd: float = 50.0):
        self.daily_budget = DailyBudget(limit_usd=Decimal(str(daily_limit_usd)))

    async def complete(
        self,
        task: TaskType,
        messages: List[Dict[str, str]],
        stream: bool = False,
        **kwargs
    ) -> Any:
        """Route completion request with fallbacks."""
        config = MODEL_ROUTING[task]
        for model in [config.primary] + config.fallbacks:
            try:
                if stream:
                    return self._stream_complete(model, messages, config)
                response = await acompletion(
                    model=model, messages=messages,
                    temperature=config.temperature, max_tokens=config.max_tokens,
                )
                logger.info("llm_complete", model=model, task=task.value)
                return response
            except Exception as e:
                logger.warning("model_failed", model=model, error=str(e))
        raise RuntimeError("All models failed")

    async def _stream_complete(
        self, model: str, messages: List[Dict], config: ModelConfig
    ) -> AsyncIterator[str]:
        response = await acompletion(
            model=model, messages=messages, stream=True,
            temperature=config.temperature,
        )
        async for chunk in response:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
