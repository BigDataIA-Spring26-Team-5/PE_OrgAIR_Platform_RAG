# PE Org-AI-R Platform (CS4)

RAG-powered chatbot and scoring pipeline for evaluating company AI readiness across seven dimensions.

## Guardrails

The chatbot endpoint (`GET /rag/chatbot/{ticker}`) applies layered guardrails at three chokepoints.

### Architecture

```
User Request
     │
     ▼
┌─────────────────────┐
│  1. INPUT GUARDS    │  ← validate question, ticker, dimension
└─────────────────────┘
     │ (blocked → 400 with reason)
     ▼
  [Retrieval + Context Assembly]
     │
     ▼
┌─────────────────────┐
│  2. CONTEXT GUARDS  │  ← cap total context size, flag empty evidence
└─────────────────────┘
     │
     ▼
  [LLM Call]
     │
     ▼
┌─────────────────────┐
│  3. OUTPUT GUARDS   │  ← grounding check, length check
└─────────────────────┘
     │
     ▼
  Response
```

---

### Layer 1 — Input Guards (`app/guardrails/input_guards.py`)

**Ticker format validation**
- Must match `^[A-Z][A-Z0-9\.\-]{0,9}$`
- Rejects lowercase tickers, symbols starting with digits, and strings over 10 chars

**Question length limits**
- Minimum: 10 characters
- Maximum: 500 characters

**Prompt injection detection**
The following patterns are blocked (case-insensitive):

| Pattern | Targets |
|---|---|
| `ignore (previous\|all\|above) instructions?` | Generic jailbreak |
| `you are (now\|actually)` | Role reassignment |
| `forget (everything\|your instructions)` | Memory wipe |
| `system prompt` | System prompt exfiltration |
| `<\|im_start\|>` | LLaMA injection token |
| `[INST]` | Mistral injection token |

**Dimension allow-list**
- Must be `None` or one of the 7 valid dimensions:
  `data_infrastructure`, `ai_governance`, `technology_stack`, `talent`, `leadership`, `use_case_portfolio`, `culture`

---

### Layer 2 — Context Guard (inline in `app/routers/rag.py`)

After retrieval results are assembled into a context string, the total character count is capped:

- **Limit:** `MAX_CONTEXT_CHARS = 6000` characters
- **Behavior:** if exceeded, context is truncated and a `[Context truncated to fit token budget.]` suffix is appended
- **Purpose:** prevents token budget overrun on the LLM call

---

### Layer 3 — Output Guards (`app/guardrails/output_guards.py`)

**Answer length check**
- Minimum: 20 characters
- Maximum: 2,000 characters
- Failure produces a `[Guard: answer quality check failed — ...]` message

**Grounding check**
- If the evidence list is empty but the answer contains citation-like patterns (`per SEC`, `per the`, `[sec`, `[job`), a disclaimer is appended:
  > *Note: No supporting evidence was retrieved for this response. The answer above may not be grounded in verified filings or disclosures.*
- Partial answers are preserved; only the disclaimer is added

**Refusal detection**
- If the LLM answer begins with `"I cannot"`, `"I'm unable"`, or `"As an AI"`, the response is replaced with a structured fallback:
  > *The system was unable to generate an answer for this question. Please rephrase or try a different query.*

---

### Configuration

| Constant | File | Default | Purpose |
|---|---|---|---|
| `MAX_CONTEXT_CHARS` | `app/routers/rag.py` | `6000` | Context size cap before LLM call |

---

### HTTP Error Codes

| Code | Cause |
|---|---|
| `400` | Blocked input (invalid ticker, short/long/injected question, bad dimension) — `detail` contains human-readable reason |
| `500` | Unexpected LLM or retrieval failure |

---

### Logging

All guard blocks emit a structlog event with key `rag.guardrail_blocked` and fields:
- `guard` — name of the guard that blocked (e.g. `validate_ticker`)
- `reason` — human-readable block reason
