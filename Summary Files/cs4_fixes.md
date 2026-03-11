# CS4 Fixes & Changes Summary

## Files Modified (fixes to existing code)

### 1. `services/integration/cs1_client.py`
- Switched `httpx.Client` → `httpx.AsyncClient` (all methods now `async def`)
- Added `Sector` enum matching CS4 spec
- Added `Portfolio` dataclass matching CS4 spec
- `_parse_company()` now maps raw sector string to `Sector` enum with safe fallback
- Context manager updated to `__aenter__` / `__aexit__`

---

### 2. `services/search/vector_store.py`
- Changed embedding model from `BAAI/bge-small-en-v1.5` → `all-MiniLM-L6-v2` to match CS4 spec exactly

---

### 3. `services/retrieval/hybrid.py`
- Changed embedding model from `BAAI/bge-small-en-v1.5` → `all-MiniLM-L6-v2`
- Both `vector_store.py` and `hybrid.py` must use the **same model** — different models produce incompatible embedding spaces

---

### 4. `services/llm/router.py`
- Added clearly marked `# TESTING` routing block (Groq + DeepSeek — current)
- Added commented `# PRODUCTION` routing block (Claude Sonnet + Claude Haiku — ready to uncomment)
- Added `TODO` comments on `_MODEL_CONFIGS` and `_MODEL_COST_PER_1K` for production switch
- **To switch to Claude later:** uncomment production block, comment testing block, add `ANTHROPIC_API_KEY` to `.env`

---

### 5. `services/workflows/ic_prep.py`
- `prepare_meeting()` changed from sync to `async def`
- `await self.cs1.get_company(ticker)` — fixes CS1 async mismatch
- 7 dimension justifications now run **concurrently** via `asyncio.gather()` instead of sequentially
- `loop.run_in_executor()` wraps sync `generate_justification()` to avoid blocking event loop
- Safe null handling added for `employee_count` and `revenue_millions` in summary

---

### 6. `routers/rag.py`
- `/ic-prep` endpoint: replaced `asyncio.to_thread(workflow.prepare_meeting, ...)` with `await workflow.prepare_meeting(...)` — `prepare_meeting` is now async so `to_thread` was incorrect

---

### 7. `services/collection/analyst_notes.py`
- Added `AnalystNotesRepository` class for Snowflake persistence
- Added `_persist()` — saves to S3 → Snowflake → ChromaDB → memory in that order
- S3 path: `analyst_notes/{company_id}/{note_id}.json`
- `load_from_snowflake()` restores memory cache on server restart
- `get_note()` falls back to Snowflake if not in memory cache
- Snowflake connection gracefully degrades — S3 + ChromaDB still work if DB is down

---

### 8. `tests/test_justification.py`
- Fix assert in `test_match_to_rubric_filters_low_score`:
  ```python
  # Wrong — both docs match keywords so both are included
  assert len(cited) == 1

  # Correct
  assert len(cited) == 2
  ```

---

## New Files Created

| File | Purpose |
|---|---|
| `database/analyst_notes_schema.sql` | Snowflake DDL for `ANALYST_NOTES` table |

---

## Snowflake Schema Notes

- `ANALYST_NOTES` table added — run `analyst_notes_schema.sql` before using `AnalystNotesCollector`
- No `CHECK` constraints — Snowflake parses but does not enforce them; validation is at app layer
- No `CREATE INDEX` — only supported on Hybrid Tables; use `CLUSTER BY (COMPANY_ID)` instead
- All existing CS1/CS2/CS3 tables unchanged

---

## Production Switch Checklist (when ready)

- [ ] Get `ANTHROPIC_API_KEY` and add to `.env`
- [ ] In `router.py`: uncomment `PRODUCTION` block, comment out `TESTING` block
- [ ] In `router.py`: uncomment Claude entries in `_MODEL_CONFIGS` and `_MODEL_COST_PER_1K`
- [ ] If ChromaDB already has indexed data: call `vector_store.wipe()` and re-index
      (embedding model changed from `bge-small-en-v1.5` → `all-MiniLM-L6-v2`)