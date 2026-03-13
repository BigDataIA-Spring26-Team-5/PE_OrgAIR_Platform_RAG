# # app/services/llm/router.py
# """LLM Router — LiteLLM multi-provider router (Groq + DeepSeek).

# NOTE: Currently using Groq + DeepSeek for testing/development.
# TODO: Once testing is complete, switch to Claude Sonnet + Claude Haiku for production quality.
#       Steps to switch:
#         1. Add ANTHROPIC_API_KEY to .env
#         2. Uncomment the PRODUCTION routing block below
#         3. Comment out the TESTING routing block below
#         That's it — LiteLLM handles the rest.
# """
# from __future__ import annotations

# import os
# import time
# from dataclasses import dataclass, field
# from typing import AsyncIterator, List, Dict, Any, Optional

# # Fix for LiteLLM hanging on import on Windows
# # Must be set BEFORE importing litellm
# os.environ['LITELLM_LOCAL_MODEL_COST_MAP'] = 'True'

# try:
#     import litellm
#     _LITELLM_AVAILABLE = True
# except ImportError:
#     _LITELLM_AVAILABLE = False


# # ---------------------------------------------------------------------------
# # Task Routing
# # ---------------------------------------------------------------------------

# # TESTING routing — Groq + DeepSeek (current)
# _TASK_ROUTING: Dict[str, tuple[str, str]] = {
#     # Fast/cheap tasks — Groq is primary, DeepSeek as fallback
#     "evidence_extraction":      ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),
#     "keyword_matching":         ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),
#     "hyde_generation":          ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),

#     # Quality-critical tasks — DeepSeek primary, Groq as fallback
#     "justification_generation": ("deepseek/deepseek-chat", "groq/llama-3.1-8b-instant"),
#     "ic_summary":               ("deepseek/deepseek-chat", "groq/llama-3.1-8b-instant"),
#     "chat_response":            ("deepseek/deepseek-chat", "groq/llama-3.1-8b-instant"),

#     "subdomain_suggestion":         ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),
#     "governance_pattern_extraction": ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),
#     "governance_extraction":        ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),

#     # Tech stack LLM fallback — used by tech_signals.py when BuiltWith/Wappalyzer return 0
#     # TODO: SWITCH TO CLAUDE — in production block below this routes to Claude Sonnet instead
#     "tech_stack_fallback":      ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),
# }

# # ---------------------------------------------------------------------------
# # TODO: PRODUCTION routing — uncomment this block and comment out TESTING
# #       block above when ready to switch to Claude.
# #       Requires: ANTHROPIC_API_KEY in .env
# # ---------------------------------------------------------------------------
# # _TASK_ROUTING: Dict[str, tuple[str, str]] = {
# #     # Fast/cheap tasks — Groq stays as primary even in production
# #     "evidence_extraction":      ("groq/llama-3.1-8b-instant", "claude-haiku-4-5-20251001"),
# #     "keyword_matching":         ("groq/llama-3.1-8b-instant", "claude-haiku-4-5-20251001"),
# #     "hyde_generation":          ("groq/llama-3.1-8b-instant", "claude-haiku-4-5-20251001"),
# #
# #     # Quality-critical tasks — Claude Sonnet for best IC memo quality (~$0.26/company)
# #     "justification_generation": ("claude-sonnet-4-20250514", "groq/llama-3.1-8b-instant"),
# #     "ic_summary":               ("claude-sonnet-4-20250514", "groq/llama-3.1-8b-instant"),
# #     "chat_response":            ("claude-haiku-4-5-20251001", "groq/llama-3.1-8b-instant"),
# #
# #     # Tech stack LLM fallback — Claude Sonnet has strongest company tech stack knowledge
# #     "tech_stack_fallback":      ("claude-sonnet-4-20250514", "groq/llama-3.1-8b-instant"),
# # }
# # ---------------------------------------------------------------------------


# # ---------------------------------------------------------------------------
# # Model Configs
# # ---------------------------------------------------------------------------

# _MODEL_CONFIGS: Dict[str, Dict[str, Any]] = {
#     "groq/llama-3.1-8b-instant": {
#         "max_tokens": 1024,
#         "temperature": 0.3,
#         "api_key_env": "GROQ_API_KEY",
#     },
#     "deepseek/deepseek-chat": {
#         "max_tokens": 2048,
#         "temperature": 0.4,
#         "api_key_env": "DEEPSEEK_API_KEY",
#     },
#     # TODO: Uncomment when switching to production
#     # "claude-sonnet-4-20250514": {
#     #     "max_tokens": 2000,
#     #     "temperature": 0.2,
#     #     "api_key_env": "ANTHROPIC_API_KEY",
#     # },
#     # "claude-haiku-4-5-20251001": {
#     #     "max_tokens": 1000,
#     #     "temperature": 0.3,
#     #     "api_key_env": "ANTHROPIC_API_KEY",
#     # },
# }

# # Approximate cost per 1K tokens (USD) for budget tracking
# _MODEL_COST_PER_1K: Dict[str, float] = {
#     "groq/llama-3.1-8b-instant": 0.00005,
#     "deepseek/deepseek-chat":    0.00014,
#     # TODO: Uncomment when switching to production
#     # "claude-sonnet-4-20250514":  0.015,
#     # "claude-haiku-4-5-20251001": 0.00125,
# }


# # ---------------------------------------------------------------------------
# # Daily Budget
# # ---------------------------------------------------------------------------

# @dataclass
# class DailyBudget:
#     limit_usd: float
#     _spend: float = field(default=0.0, init=False)
#     _reset_ts: float = field(default_factory=time.time, init=False)

#     def _maybe_reset(self):
#         now = time.time()
#         if now - self._reset_ts > 86400:
#             self._spend = 0.0
#             self._reset_ts = now

#     def record(self, tokens: int, model: str):
#         self._maybe_reset()
#         cost = (tokens / 1000) * _MODEL_COST_PER_1K.get(model, 0.0001)
#         self._spend += cost

#     def is_over_limit(self) -> bool:
#         self._maybe_reset()
#         return self._spend >= self.limit_usd

#     @property
#     def spend(self) -> float:
#         self._maybe_reset()
#         return self._spend


# # ---------------------------------------------------------------------------
# # Model Router
# # ---------------------------------------------------------------------------

# class ModelRouter:
#     """Routes LLM calls to Groq or DeepSeek based on task type.

#     To switch to Claude Sonnet in production:
#       1. Add ANTHROPIC_API_KEY to .env
#       2. Swap the _TASK_ROUTING block at the top of this file
#       3. Uncomment the Claude entries in _MODEL_CONFIGS and _MODEL_COST_PER_1K
#     """

#     def __init__(self, daily_limit_usd: float = 50.0):
#         self.budget = DailyBudget(limit_usd=daily_limit_usd)
#         if _LITELLM_AVAILABLE:
#             litellm.set_verbose = False

#     def complete(
#         self,
#         task: str,
#         messages: List[Dict[str, str]],
#         stream: bool = False,
#     ) -> str:
#         """Synchronous completion. Tries primary model, falls back on error."""
#         if self.budget.is_over_limit():
#             raise RuntimeError(f"Daily budget of ${self.budget.limit_usd} exceeded.")

#         primary, fallback = _TASK_ROUTING.get(
#             task, ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat")
#         )

#         last_exc: Exception = RuntimeError("No models tried.")
#         for model in (primary, fallback):
#             try:
#                 return self._call_model(model, messages)
#             except Exception as exc:
#                 last_exc = exc
#                 continue
#         raise RuntimeError(f"Both models failed for task '{task}': {last_exc}")

#     def _call_model(self, model: str, messages: List[Dict[str, str]]) -> str:
#         config = _MODEL_CONFIGS.get(model, {})
#         api_key = os.getenv(config.get("api_key_env", ""))

#         if not _LITELLM_AVAILABLE:
#             return self._fallback_stub(model, messages)

#         response = litellm.completion(
#             model=model,
#             messages=messages,
#             max_tokens=config.get("max_tokens", 1024),
#             temperature=config.get("temperature", 0.3),
#             api_key=api_key,
#         )
#         tokens = getattr(response.usage, "total_tokens", 500)
#         self.budget.record(tokens, model)
#         return response.choices[0].message.content or ""

#     async def _stream_complete(
#         self, model: str, messages: List[Dict[str, str]], config: Dict[str, Any]
#     ) -> AsyncIterator[str]:
#         """Async streaming completion."""
#         api_key = os.getenv(config.get("api_key_env", ""))
#         if not _LITELLM_AVAILABLE:
#             yield self._fallback_stub(model, messages)
#             return
#         response = await litellm.acompletion(
#             model=model,
#             messages=messages,
#             max_tokens=config.get("max_tokens", 1024),
#             temperature=config.get("temperature", 0.3),
#             api_key=api_key,
#             stream=True,
#         )
#         async for chunk in response:
#             delta = chunk.choices[0].delta.content or ""
#             if delta:
#                 yield delta

#     @staticmethod
#     def _fallback_stub(model: str, messages: List[Dict[str, str]]) -> str:
#         """Used when litellm is not installed — returns a placeholder."""
#         user_msg = next((m["content"] for m in messages if m["role"] == "user"), "")
#         return f"[{model} stub] Response to: {user_msg[:100]}..."

# app/services/llm/router.py
"""LLM Router — LiteLLM multi-provider router (Groq + DeepSeek)."""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import AsyncIterator, List, Dict, Any, Optional

# Must be set BEFORE importing litellm
os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"

try:
    import litellm
    _LITELLM_AVAILABLE = True
except ImportError:
    _LITELLM_AVAILABLE = False


# ---------------------------------------------------------------------------
# Task Routing
# ---------------------------------------------------------------------------

_TASK_ROUTING: Dict[str, tuple[str, str]] = {
    "evidence_extraction": ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),
    "keyword_matching": ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),
    "hyde_generation": ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),
    "justification_generation": ("deepseek/deepseek-chat", "groq/llama-3.1-8b-instant"),
    "ic_summary": ("deepseek/deepseek-chat", "groq/llama-3.1-8b-instant"),
    "chat_response": ("deepseek/deepseek-chat", "groq/llama-3.1-8b-instant"),
    "subdomain_suggestion": ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),
    "governance_pattern_extraction": ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),
    "governance_extraction": ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),
    "tech_stack_fallback": ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat"),
}


# ---------------------------------------------------------------------------
# Model Configs
# ---------------------------------------------------------------------------

_MODEL_CONFIGS: Dict[str, Dict[str, Any]] = {
    "groq/llama-3.1-8b-instant": {
        "max_tokens": 1024,
        "temperature": 0.3,
        "api_key_env": "GROQ_API_KEY",
    },
    "deepseek/deepseek-chat": {
        "max_tokens": 2048,
        "temperature": 0.4,
        "api_key_env": "DEEPSEEK_API_KEY",
    },
}

_MODEL_COST_PER_1K: Dict[str, float] = {
    "groq/llama-3.1-8b-instant": 0.00005,
    "deepseek/deepseek-chat": 0.00014,
}


# ---------------------------------------------------------------------------
# Daily Budget
# ---------------------------------------------------------------------------

@dataclass
class DailyBudget:
    limit_usd: float
    _spend: float = field(default=0.0, init=False)
    _reset_ts: float = field(default_factory=time.time, init=False)

    def _maybe_reset(self) -> None:
        now = time.time()
        if now - self._reset_ts > 86400:
            self._spend = 0.0
            self._reset_ts = now

    def record(self, tokens: int, model: str) -> None:
        self._maybe_reset()
        cost = (tokens / 1000) * _MODEL_COST_PER_1K.get(model, 0.0001)
        self._spend += cost

    def is_over_limit(self) -> bool:
        self._maybe_reset()
        return self._spend >= self.limit_usd

    @property
    def spend(self) -> float:
        self._maybe_reset()
        return self._spend


# ---------------------------------------------------------------------------
# Model Router
# ---------------------------------------------------------------------------

class ModelRouter:
    def __init__(self, daily_limit_usd: float = 50.0):
        self.budget = DailyBudget(limit_usd=daily_limit_usd)
        if _LITELLM_AVAILABLE:
            litellm.set_verbose = False

    async def complete(
        self,
        task: str,
        messages: List[Dict[str, str]],
        stream: bool = False,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        """
        Async completion. Tries primary model, then fallback.
        Accepts per-call overrides for temperature and max_tokens.
        """
        if self.budget.is_over_limit():
            raise RuntimeError(f"Daily budget of ${self.budget.limit_usd} exceeded.")

        primary, fallback = _TASK_ROUTING.get(
            task, ("groq/llama-3.1-8b-instant", "deepseek/deepseek-chat")
        )

        last_exc: Exception = RuntimeError("No models tried.")
        for model in (primary, fallback):
            try:
                if stream:
                    chunks = []
                    config = _MODEL_CONFIGS.get(model, {})
                    async for chunk in self._stream_complete(
                        model=model,
                        messages=messages,
                        config=config,
                        temperature=temperature,
                        max_tokens=max_tokens,
                    ):
                        chunks.append(chunk)
                    return "".join(chunks)

                return await self._call_model(
                    model=model,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
            except Exception as exc:
                last_exc = exc
                continue

        raise RuntimeError(f"Both models failed for task '{task}': {last_exc}")

    async def _call_model(
        self,
        model: str,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        config = _MODEL_CONFIGS.get(model, {})
        api_key_env = config.get("api_key_env", "")
        api_key = os.getenv(api_key_env) if api_key_env else None

        if not _LITELLM_AVAILABLE:
            return self._fallback_stub(model, messages)

        response = await litellm.acompletion(
            model=model,
            messages=messages,
            max_tokens=max_tokens if max_tokens is not None else config.get("max_tokens", 1024),
            temperature=temperature if temperature is not None else config.get("temperature", 0.3),
            api_key=api_key,
            stream=False,
        )

        usage = getattr(response, "usage", None)
        tokens = getattr(usage, "total_tokens", 500) if usage else 500
        self.budget.record(tokens, model)

        return response.choices[0].message.content or ""

    async def _stream_complete(
        self,
        model: str,
        messages: List[Dict[str, str]],
        config: Dict[str, Any],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> AsyncIterator[str]:
        api_key_env = config.get("api_key_env", "")
        api_key = os.getenv(api_key_env) if api_key_env else None

        if not _LITELLM_AVAILABLE:
            yield self._fallback_stub(model, messages)
            return

        response = await litellm.acompletion(
            model=model,
            messages=messages,
            max_tokens=max_tokens if max_tokens is not None else config.get("max_tokens", 1024),
            temperature=temperature if temperature is not None else config.get("temperature", 0.3),
            api_key=api_key,
            stream=True,
        )

        async for chunk in response:
            delta = getattr(chunk.choices[0].delta, "content", "") or ""
            if delta:
                yield delta


    def complete_sync(
        self,
        task: str,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        """Synchronous wrapper for async complete() method.
        
        Use this when calling from synchronous code like services.
        """
        import asyncio
        
        try:
            # Try to get existing event loop
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If event loop is already running (e.g., in async context),
                # we need to run in a new thread
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self.complete(task, messages, temperature=temperature, max_tokens=max_tokens)
                    )
                    return future.result()
            else:
                # No running loop, we can use asyncio.run directly
                return asyncio.run(self.complete(task, messages, temperature=temperature, max_tokens=max_tokens))
        except RuntimeError:
            # No event loop exists, create one
            return asyncio.run(self.complete(task, messages, temperature=temperature, max_tokens=max_tokens))

    @staticmethod
    def _fallback_stub(model: str, messages: List[Dict[str, str]]) -> str:
        user_msg = next((m["content"] for m in messages if m["role"] == "user"), "")
        return f"[{model} stub] Response to: {user_msg[:100]}..."


# ---------------------------------------------------------------------------
# Singleton factory export
# ---------------------------------------------------------------------------

_router_instance = None

def get_llm_router():
    global _router_instance
    if _router_instance is None:
        _router_instance = ModelRouter()
    return _router_instance