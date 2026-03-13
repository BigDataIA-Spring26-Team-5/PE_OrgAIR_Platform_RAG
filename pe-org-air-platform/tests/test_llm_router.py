"""Unit tests for DailyBudget and ModelRouter — no real LLM calls."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.services.llm.router import DailyBudget, ModelRouter, _TASK_ROUTING


# ---------------------------------------------------------------------------
# DailyBudget
# ---------------------------------------------------------------------------

def test_budget_starts_at_zero():
    budget = DailyBudget(50.0)
    assert budget.spend == 0.0


def test_budget_can_spend_below_limit():
    budget = DailyBudget(50.0)
    # 500 tokens at $0.00005/1K = $0.000025
    budget.record(500, "groq/llama-3.1-8b-instant")
    assert not budget.is_over_limit()
    assert budget.spend > 0.0


def test_budget_over_limit_after_exceeding():
    budget = DailyBudget(0.001)  # very small limit
    # 1_000_000 tokens at $0.00005/1K = $0.05 >> $0.001
    budget.record(1_000_000, "groq/llama-3.1-8b-instant")
    assert budget.is_over_limit()


def test_budget_resets_after_24h():
    budget = DailyBudget(50.0)
    budget._spend = 100.0  # directly set spend over limit
    assert budget.is_over_limit()

    # Advance time by 86401 seconds (more than 24h)
    future = budget._reset_ts + 86401
    with patch("app.services.llm.router.time.time", return_value=future):
        budget._maybe_reset()

    assert budget._spend == 0.0


def test_budget_record_uses_correct_cost_per_1k():
    budget = DailyBudget(50.0)
    # Groq cost: $0.00005 per 1K tokens
    # 1000 tokens → $0.00005
    budget.record(1000, "groq/llama-3.1-8b-instant")
    assert abs(budget.spend - 0.00005) < 1e-9


# ---------------------------------------------------------------------------
# ModelRouter — _TASK_ROUTING
# ---------------------------------------------------------------------------

def test_task_routing_coverage():
    expected_keys = {
        "evidence_extraction", "keyword_matching", "hyde_generation",
        "justification_generation", "ic_summary", "chat_response",
        "tech_stack_fallback",
    }
    assert expected_keys.issubset(set(_TASK_ROUTING.keys()))


def test_complete_uses_correct_task_routing():
    """hyde_generation routes to groq primary."""
    router = ModelRouter()

    with patch("app.services.llm.router._LITELLM_AVAILABLE", False):
        result = router.complete("hyde_generation", [{"role": "user", "content": "test"}])

    primary_model, _ = _TASK_ROUTING["hyde_generation"]
    assert primary_model in result


def test_complete_fallback_on_primary_failure():
    """Primary model raises → fallback model succeeds."""
    router = ModelRouter()

    call_count = 0

    def fake_call_model(model, messages):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("Primary model down")
        return "fallback response"

    router._call_model = fake_call_model
    result = router.complete("hyde_generation", [{"role": "user", "content": "test"}])
    assert result == "fallback response"
    assert call_count == 2


def test_complete_raises_when_all_models_fail():
    router = ModelRouter()

    def always_fail(model, messages):
        raise RuntimeError("API error")

    router._call_model = always_fail
    with pytest.raises(RuntimeError, match="Both models failed"):
        router.complete("hyde_generation", [{"role": "user", "content": "test"}])


def test_complete_raises_when_over_budget():
    router = ModelRouter(daily_limit_usd=0.0)
    # Force budget over limit
    router.budget._spend = 1.0

    with pytest.raises(RuntimeError, match="budget"):
        router.complete("hyde_generation", [{"role": "user", "content": "test"}])


def test_fallback_stub_format():
    result = ModelRouter._fallback_stub(
        "groq/llama-3.1-8b-instant",
        [{"role": "user", "content": "test question"}],
    )
    assert "[groq/llama-3.1-8b-instant stub]" in result


def test_complete_without_litellm():
    """When litellm not available, _fallback_stub is used."""
    router = ModelRouter()
    with patch("app.services.llm.router._LITELLM_AVAILABLE", False):
        result = router.complete(
            "hyde_generation",
            [{"role": "user", "content": "What is data infrastructure?"}],
        )
    assert isinstance(result, str)
    assert len(result) > 0


def test_unknown_task_uses_default_routing():
    """Unknown task key falls back to groq primary."""
    router = ModelRouter()

    with patch("app.services.llm.router._LITELLM_AVAILABLE", False):
        result = router.complete(
            "nonexistent_task_xyz",
            [{"role": "user", "content": "test"}],
        )
    # Default primary is groq/llama-3.1-8b-instant
    assert "groq/llama-3.1-8b-instant stub" in result
