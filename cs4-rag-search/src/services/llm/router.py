"""
router.py — CS4 RAG Search
src/services/llm/router.py

LiteLLM router with model fallback list.
Primary model: gpt-4o; fallbacks: gpt-4o-mini → claude-sonnet-4-6.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import litellm
from litellm import Router

import structlog

logger = structlog.get_logger(__name__)


def build_router() -> Router:
    """
    Build a litellm.Router with tiered model fallbacks.
    API keys are read from environment variables.
    """
    model_list = [
        {
            "model_name": "gpt-4o",
            "litellm_params": {
                "model": "gpt-4o",
                "api_key": os.getenv("OPENAI_API_KEY", ""),
            },
        },
        {
            "model_name": "gpt-4o-mini",
            "litellm_params": {
                "model": "gpt-4o-mini",
                "api_key": os.getenv("OPENAI_API_KEY", ""),
            },
        },
        {
            "model_name": "claude-sonnet",
            "litellm_params": {
                "model": "claude-sonnet-4-6",
                "api_key": os.getenv("ANTHROPIC_API_KEY", ""),
            },
        },
    ]

    return Router(
        model_list=model_list,
        fallbacks=[
            {"gpt-4o": ["gpt-4o-mini", "claude-sonnet"]},
        ],
        allowed_fails=2,
        retry_after=5,
        num_retries=2,
    )


# Module-level singleton
_router: Optional[Router] = None


def get_router() -> Router:
    global _router
    if _router is None:
        _router = build_router()
    return _router


async def chat(
    messages: List[Dict[str, str]],
    model: str = "gpt-4o",
    temperature: float = 0.2,
    max_tokens: int = 2048,
    **kwargs: Any,
) -> str:
    """Convenience wrapper — returns the assistant message content."""
    router = get_router()
    logger.info("llm call", model=model, message_count=len(messages))
    response = await router.acompletion(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
        **kwargs,
    )
    return response.choices[0].message.content.strip()
