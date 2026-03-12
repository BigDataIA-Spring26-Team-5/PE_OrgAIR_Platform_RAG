"""
Pipeline Client — streamlit/utils/pipeline_client.py

Step order (9 steps, all synchronous):
  1. Company Setup          POST /api/v1/companies
  2. SEC Filings            POST /api/v1/documents/collect
  3. Parse Documents        POST /api/v1/documents/parse/{ticker}       NON-FATAL
  4. Chunk Documents        POST /api/v1/documents/chunk/{ticker}       NON-FATAL
  5. Signal Scoring         POST /api/v1/signals/score/{ticker}/all     SYNC/FATAL
  6. Glassdoor Culture      POST /api/v1/glassdoor-signals/{ticker}     NON-FATAL
  7. Board Governance       POST /api/v1/board-governance/analyze/{ticker} NON-FATAL
  8. Scoring                POST /api/v1/scoring/{ticker}               FATAL
  9. Index Evidence         POST /rag/index/{ticker}?force=true         NON-FATAL

KEY FIX: Step 5 uses /signals/score/{ticker}/all (synchronous, blocks until all 4
signals complete) instead of /signals/collect (async fire-and-forget).
Steps 6+7 run BEFORE Step 8 so Glassdoor/board S3 files exist when CS3 reads them.
"""
from __future__ import annotations

import os
import time
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, timezone, datetime
from typing import Optional, Dict, Any, Callable, List

logger = logging.getLogger(__name__)

API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")
REQUEST_TIMEOUT_SHORT = 30
REQUEST_TIMEOUT_LONG  = 600   # signals/score/all takes 5+ min (jobs + patents + leadership)


@dataclass
class SignalFlag:
    """
    LLM sanity-check result for a single signal score.
    Populated after Step 5 completes — surfaced in Streamlit as a ⚠️ warning.
    Never blocks the pipeline or auto-corrects scores.
    """
    category: str                         # e.g. "digital_presence"
    score: float                          # the score being questioned
    plausible: bool                       # LLM verdict
    reason: str                           # short explanation
    severity: str                         # "low" | "medium" | "high"
    raw_value: Optional[str] = None       # signal's raw_value for context


@dataclass
class PipelineStepResult:
    step: int
    name: str
    status: str                           # "success", "error", "skipped"
    message: str
    data: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    duration_seconds: float = 0.0
    signal_flags: List = field(default_factory=list)
    # signal_flags: List[SignalFlag] — populated on Step 5 only


@dataclass
class PipelineResult:
    ticker: str
    overall_status: str                   # "success", "partial", "failed"
    steps: List[PipelineStepResult] = field(default_factory=list)
    org_air_score: Optional[float] = None
    indexed_count: int = 0
    error: Optional[str] = None
    signal_flags: List = field(default_factory=list)
    # signal_flags: List[SignalFlag] — rolled up from Step 5, read directly by Streamlit


class PipelineClient:

    def __init__(self, base_url: str = API_BASE):
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    # ── Health / status ───────────────────────────────────────────────────────

    def is_backend_alive(self) -> bool:
        try:
            resp = self._session.get(f"{self.base_url}/health", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False

    def get_company_status(self, ticker: str) -> Dict[str, Any]:
        status = {
            "chatbot_ready": False,
            "org_air_score": None,
            "indexed_documents": 0,
            "has_scores": False,
            "has_documents": False,
        }
        try:
            resp = self._session.get(
                f"{self.base_url}/rag/debug",
                params={"ticker": ticker, "limit": 5},
                timeout=REQUEST_TIMEOUT_SHORT,
            )
            if resp.status_code == 200:
                data = resp.json()
                ticker_count = data.get("by_ticker", {}).get(ticker, 0)
                total = data.get("total", 0)
                if ticker_count > 0:
                    status["indexed_documents"] = ticker_count
                    status["chatbot_ready"] = True
                elif total > 0:
                    try:
                        check = self._session.get(
                            f"{self.base_url}/rag/chatbot/{ticker}",
                            params={"question": "test"},
                            timeout=10,
                        )
                        if check.status_code == 200:
                            result = check.json()
                            if result.get("sources_used", 0) > 0:
                                status["chatbot_ready"] = True
                                status["indexed_documents"] = result["sources_used"]
                    except Exception:
                        pass
        except Exception:
            pass

        try:
            resp = self._session.get(
                f"{self.base_url}/api/v1/scoring/{ticker}/dimensions",
                timeout=REQUEST_TIMEOUT_SHORT,
            )
            if resp.status_code == 200:
                dim_scores = resp.json().get("scores", [])
                if dim_scores:
                    status["has_scores"] = True
                    avg = sum(d["score"] for d in dim_scores) / len(dim_scores)
                    status["org_air_score"] = round(avg, 1)
        except Exception:
            pass

        try:
            resp = self._session.get(
                f"{self.base_url}/api/v1/companies/{ticker}/evidence",
                timeout=REQUEST_TIMEOUT_SHORT,
            )
            if resp.status_code == 200:
                doc_summary = resp.json().get("document_summary", {})
                if doc_summary.get("total_documents", 0) > 0:
                    status["has_documents"] = True
        except Exception:
            pass

        return status

    # ── Company ID lookup ─────────────────────────────────────────────────────

    def _get_company_id(self, ticker: str) -> Optional[str]:
        try:
            resp = self._session.get(
                f"{self.base_url}/api/v1/companies/{ticker}",
                timeout=REQUEST_TIMEOUT_SHORT,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("id") or data.get("company_id") or data.get("uuid")
        except Exception:
            pass
        return None

    # ── Document status pre-check ─────────────────────────────────────────────

    def _get_doc_status(self, ticker: str) -> Dict[str, Any]:
        """
        Query how many documents are raw / parsed / chunked for this ticker.

        Reads the ACTUAL response shape from /api/v1/companies/{ticker}/evidence:
          {
            "document_summary": {
              "total_documents": 40,
              "by_status": {          ← statuses that EXIST are listed here
                "chunked": 6,         ← only present if > 0
                "parsed": 34          ← only present if > 0
              }                       ← "raw" key absent = 0 raw documents
            }
          }

        Key insight: the API only includes statuses with count > 0 in by_status.
        A missing key means count == 0, NOT unknown. So:
          - no "raw" / "collected" key  → nothing unprocessed (safe to skip parse)
          - no "parsed" key             → nothing parsed-but-not-chunked (safe to skip chunk)

        Returns:
            {
                "total_documents": int,
                "parsed_count": int,    # docs with status == "parsed" (not yet chunked)
                "chunked_count": int,   # docs with status == "chunked"
                "raw_count": int,       # docs with status == "raw" or "collected"
                "by_status": dict,      # raw by_status dict for debugging
            }
        Falls back to all-zeros on any error so the caller always gets a dict.
        """
        empty = {
            "total_documents": 0, "parsed_count": 0,
            "chunked_count": 0,   "raw_count": 0,
            "by_status": {},
        }
        try:
            resp = self._session.get(
                f"{self.base_url}/api/v1/companies/{ticker}/evidence",
                timeout=REQUEST_TIMEOUT_SHORT,
            )
            if resp.status_code != 200:
                return empty

            data    = resp.json()
            summary = data.get("document_summary", data)
            total   = int(summary.get("total_documents", 0))

            # by_status only lists statuses with count > 0
            # Missing key = 0 documents in that state
            by_status = summary.get("by_status", {})
            chunked   = int(by_status.get("chunked", 0))
            parsed    = int(by_status.get("parsed",  0))

            # "raw" documents may appear as "raw", "collected", or "pending"
            raw = int(
                by_status.get("raw", 0)
                or by_status.get("collected", 0)
                or by_status.get("pending", 0)
            )

            return {
                "total_documents": total,
                "parsed_count":    parsed,    # parsed but NOT yet chunked
                "chunked_count":   chunked,
                "raw_count":       raw,
                "by_status":       by_status,
            }
        except Exception:
            return empty

    # ── Step 1 — Company Setup ────────────────────────────────────────────────

    def _step_create_company(self, resolved) -> PipelineStepResult:
        start = time.time()
        payload = {
            "name": resolved.name,
            "ticker": resolved.ticker,
            "industry_id": resolved.industry_id,
            "position_factor": resolved.position_factor,
        }
        if resolved.sector:        payload["sector"] = resolved.sector
        if resolved.revenue_millions: payload["revenue_millions"] = resolved.revenue_millions
        if resolved.employee_count:   payload["employee_count"] = resolved.employee_count

        try:
            resp = self._session.post(
                f"{self.base_url}/api/v1/companies",
                json=payload, timeout=REQUEST_TIMEOUT_SHORT,
            )
            duration = time.time() - start
            if resp.status_code == 409:
                company_id = self._get_company_id(resolved.ticker)
                return PipelineStepResult(
                    step=1, name="Company Setup", status="skipped",
                    message=f"{resolved.name} ({resolved.ticker}) already exists in platform",
                    data={"ticker": resolved.ticker, "company_id": company_id},
                    duration_seconds=duration,
                )
            resp.raise_for_status()
            return PipelineStepResult(
                step=1, name="Company Setup", status="success",
                message=f"Created {resolved.name} ({resolved.ticker})",
                data=resp.json(), duration_seconds=duration,
            )
        except requests.HTTPError as e:
            return PipelineStepResult(
                step=1, name="Company Setup", status="error",
                message="Failed to create company in platform",
                error=f"HTTP {e.response.status_code}: {e.response.text[:200]}",
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return PipelineStepResult(
                step=1, name="Company Setup", status="error",
                message="Failed to create company in platform",
                error=str(e), duration_seconds=time.time() - start,
            )

    # ── Step 2 — SEC Filings ──────────────────────────────────────────────────

    def _step_collect_sec(
        self,
        ticker: str,
        cik: Optional[str],
        on_substep: Optional[Callable[[str], None]] = None,
    ) -> PipelineStepResult:
        start = time.time()
        import datetime
        filing_types = ["10-K", "8-K", "DEF 14A"]

        if on_substep:
            yr = datetime.datetime.now().year
            for ft in filing_types:
                for y in [yr, yr - 1]:
                    on_substep(f"Querying {ft} ({y})...")

        payload: Dict[str, Any] = {
            "ticker": ticker, "filing_types": filing_types, "lookback_days": 730,
        }
        if cik:
            payload["cik"] = cik

        try:
            resp = self._session.post(
                f"{self.base_url}/api/v1/documents/collect",
                json=payload, timeout=REQUEST_TIMEOUT_LONG,
            )
            resp.raise_for_status()
            data = resp.json()
            doc_count = data.get("collected_count", data.get("documents_found", data.get("count", "?")))
            return PipelineStepResult(
                step=2, name="SEC Filings", status="success",
                message=f"Collected {doc_count} filings (10-K, 8-K, DEF 14A — past 2 years)",
                data=data, duration_seconds=time.time() - start,
            )
        except requests.Timeout:
            return PipelineStepResult(
                step=2, name="SEC Filings", status="error",
                message="SEC collection timed out — EDGAR may be slow. Try re-running.",
                error="Request timed out.", duration_seconds=time.time() - start,
            )
        except requests.HTTPError as e:
            return PipelineStepResult(
                step=2, name="SEC Filings", status="error",
                message="SEC filing collection failed",
                error=f"HTTP {e.response.status_code}: {e.response.text[:200]}",
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return PipelineStepResult(
                step=2, name="SEC Filings", status="error",
                message="SEC filing collection failed",
                error=str(e), duration_seconds=time.time() - start,
            )

    def _get_signal_scores_today(self, ticker: str) -> Dict[str, Any]:
        """
        Return per-signal status for today from /api/v1/companies/{ticker}/evidence.

        Returns a dict keyed by signal category:
          {
            "technology_hiring":  {"score": 63.8, "raw_value": "...", "scored_today": True,  "should_skip": True},
            "digital_presence":   {"score": 0.0,  "raw_value": "...", "scored_today": True,  "should_skip": False},
            "innovation_activity":{"score": 100,  "raw_value": "...", "scored_today": True,  "should_skip": True},
            "leadership_signals": {"score": 70.0, "raw_value": "...", "scored_today": True,  "should_skip": True},
          }

        Skip rule per signal:
          scored_today=True AND score > 0  →  should_skip=True
          score == 0 OR score is None      →  should_skip=False  (re-run it)
          not scored today                 →  should_skip=False  (re-run it)

        Falls back to all should_skip=False on any error (safe default = always run).
        """
        SIGNAL_SCORE_KEYS = {
            "technology_hiring":   "technology_hiring_score",
            "digital_presence":    "digital_presence_score",
            "innovation_activity": "innovation_activity_score",
            "leadership_signals":  "leadership_signals_score",
        }
        default = {
            cat: {"score": None, "raw_value": None, "scored_today": False, "should_skip": False}
            for cat in SIGNAL_SCORE_KEYS
        }

        try:
            resp = self._session.get(
                f"{self.base_url}/api/v1/companies/{ticker}/evidence",
                timeout=REQUEST_TIMEOUT_SHORT,
            )
            if resp.status_code != 200:
                return default

            data    = resp.json()
            summary = data.get("signal_summary", {})
            signals = data.get("signals", [])   # full signal list with raw_value
            if not summary:
                return default

            # Parse last_updated date from summary
            last_updated = summary.get("last_updated", "")
            today = datetime.now(timezone.utc).date()
            scored_today = False
            try:
                if last_updated:
                    updated_dt   = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
                    scored_today = (updated_dt.date() == today)
            except (ValueError, AttributeError):
                pass

            # Build raw_value lookup from signals list (most recent per category)
            raw_by_cat: Dict[str, str] = {}
            for sig in signals:
                cat = sig.get("category", "")
                if cat in SIGNAL_SCORE_KEYS and cat not in raw_by_cat:
                    raw_by_cat[cat] = sig.get("raw_value", "")

            result = {}
            for cat, summary_key in SIGNAL_SCORE_KEYS.items():
                score = summary.get(summary_key)   # None or float
                is_zero_or_missing = (score is None or score == 0)
                result[cat] = {
                    "score":       score,
                    "raw_value":   raw_by_cat.get(cat),
                    "scored_today": scored_today,
                    # Skip only if: scored today AND score is real (> 0)
                    "should_skip": scored_today and not is_zero_or_missing,
                }

            return result

        except Exception:
            return default

    def _sanity_check_scores(
        self,
        ticker: str,
        company_name: str,
        signal_results: Dict[str, Any],
    ) -> List[SignalFlag]:
        """
        LLM sanity check — ask Groq whether each signal score is plausible
        for this specific company.

        Called AFTER all signals complete (skipped or freshly run).
        Only checks scores that are non-None and non-zero (zeros are already
        flagged as failures, not sanity issues).

        Uses the signal's raw_value as evidence context so the LLM can reason
        about the actual data, not just the number.

        Returns a list of SignalFlag — only for scores that seem suspicious
        (plausible=False). If all scores look fine, returns empty list.

        Never raises — returns empty list on any LLM or network failure.
        """
        try:
            import httpx
            _use_httpx = True
        except ImportError:
            _use_httpx = False

        import json

        groq_api_key = os.getenv("GROQ_API_KEY", "")
        if not groq_api_key:
            logger.warning("GROQ_API_KEY not set — skipping score sanity check")
            return []

        flags: List[SignalFlag] = []

        CATEGORY_LABELS = {
            "technology_hiring":   "Technology Hiring (job postings for AI/ML roles)",
            "digital_presence":    "Digital Presence (tech stack sophistication)",
            "innovation_activity": "Innovation Activity (AI patent portfolio)",
            "leadership_signals":  "Leadership Signals (DEF 14A executive/board tech profile)",
        }

        for cat, info in signal_results.items():
            score     = info.get("score")
            raw_value = info.get("raw_value", "")
            skipped   = info.get("should_skip", False)

            # Only sanity-check scores we actually have (skip None and 0 — already handled)
            if score is None or score == 0:
                continue

            label = CATEGORY_LABELS.get(cat, cat)
            prompt = f"""You are a PE analyst reviewing an AI readiness signal score.

Company: {company_name} ({ticker})
Signal: {label}
Score: {score}/100
Raw evidence: {raw_value or "not available"}

Does this score seem plausible for {company_name}?
Consider the company's industry, size, and general reputation.

Reply ONLY with valid JSON (no markdown, no explanation outside JSON):
{{"plausible": true/false, "reason": "one sentence explanation", "severity": "low/medium/high"}}

severity guide:
- low: mildly surprising but defensible
- medium: noticeably off, warrants review
- high: clearly wrong, likely a data or scoring error"""

            try:
                payload = {
                    "model": "llama-3.1-8b-instant",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 120,
                }
                headers = {
                    "Authorization": f"Bearer {groq_api_key}",
                    "Content-Type": "application/json",
                }
                # Use httpx if available, fall back to requests (always available)
                if _use_httpx:
                    import httpx
                    resp = httpx.post(
                        "https://api.groq.com/openai/v1/chat/completions",
                        headers=headers, json=payload, timeout=15.0,
                    )
                    resp.raise_for_status()
                    raw_json = resp.json()
                else:
                    s = requests.Session()
                    resp = s.post(
                        "https://api.groq.com/openai/v1/chat/completions",
                        headers=headers, json=payload, timeout=15.0,
                    )
                    resp.raise_for_status()
                    raw_json = resp.json()
                    s.close()

                content = raw_json["choices"][0]["message"]["content"].strip()
                content = content.replace("```json", "").replace("```", "").strip()
                parsed  = json.loads(content)

                plausible = bool(parsed.get("plausible", True))
                reason    = str(parsed.get("reason", ""))
                severity  = str(parsed.get("severity", "low"))

                if not plausible:
                    flags.append(SignalFlag(
                        category=cat,
                        score=score,
                        plausible=False,
                        reason=reason,
                        severity=severity,
                        raw_value=raw_value,
                    ))
                    logger.warning(
                        f"[{ticker}] Score sanity flag — {cat}: {score}/100 | "
                        f"severity={severity} | {reason}"
                    )

            except Exception as e:
                logger.debug(f"[{ticker}] Sanity check skipped for {cat}: {e}")
                continue

        return flags

    def _step_signal_scoring(
        self,
        ticker: str,
        company_name: str = "",
        on_substep: Optional[Callable[[str], None]] = None,
        skip_if_scored_today: bool = True,
    ) -> PipelineStepResult:
        """
        Selective per-signal scoring with LLM sanity check.

        Per-signal skip logic:
          scored today AND score > 0  →  skip (data unchanged, no point re-running)
          score == 0 OR None          →  re-run (previous run failed or got bad data)
          not scored today            →  re-run (stale data)

        Signals that need re-running are called individually via their dedicated
        endpoints and executed concurrently via ThreadPoolExecutor.

        After all signals settle (skipped or freshly run), runs a Groq LLM
        sanity check on each non-zero score. Suspicious scores are attached as
        SignalFlag objects — surfaced as ⚠️ in Streamlit but never block the pipeline.
        """
        start = time.time()

        # ── Per-signal status check ───────────────────────────────────────────
        signal_status = self._get_signal_scores_today(ticker) if skip_if_scored_today else {
            cat: {"score": None, "raw_value": None, "scored_today": False, "should_skip": False}
            for cat in ["technology_hiring", "digital_presence", "innovation_activity", "leadership_signals"]
        }

        to_skip = [cat for cat, s in signal_status.items() if s["should_skip"]]
        to_run  = [cat for cat, s in signal_status.items() if not s["should_skip"]]

        INDIVIDUAL_ENDPOINTS = {
            "technology_hiring":   f"/api/v1/signals/score/{ticker}/hiring",
            "digital_presence":    f"/api/v1/signals/score/{ticker}/digital",
            "innovation_activity": f"/api/v1/signals/score/{ticker}/innovation",
            "leadership_signals":  f"/api/v1/signals/score/{ticker}/leadership",
        }
        CATEGORY_LABELS = {
            "technology_hiring":   "Job postings (LinkedIn / Indeed)...",
            "digital_presence":    "Digital presence (BuiltWith / Wappalyzer)...",
            "innovation_activity": "Patents (USPTO)...",
            "leadership_signals":  "Leadership signals (DEF 14A)...",
        }

        if to_skip:
            logger.info(f"[{ticker}] Skipping signals (scored today, score > 0): {to_skip}")
        if to_run:
            logger.info(f"[{ticker}] Re-running signals (zero/missing/stale): {to_run}")

        if on_substep:
            for cat in to_skip:
                on_substep(f"⏭️  {CATEGORY_LABELS[cat]} (skipped — scored today)")
            for cat in to_run:
                on_substep(f"Scoring {CATEGORY_LABELS[cat]}")

        # ── Run only the signals that need it — concurrently ──────────────────
        fresh_results: Dict[str, Any] = {}   # cat → API response dict

        def _call_signal(cat: str) -> tuple[str, Any]:
            endpoint = INDIVIDUAL_ENDPOINTS[cat]
            # Fresh session per thread — self._session is not thread-safe
            s = requests.Session()
            s.headers.update({"Content-Type": "application/json"})
            try:
                resp = s.post(
                    f"{self.base_url}{endpoint}",
                    timeout=REQUEST_TIMEOUT_LONG,
                )
                resp.raise_for_status()
                return cat, resp.json()
            except Exception as e:
                logger.error(f"[{ticker}] Signal endpoint {endpoint} failed: {e}")
                return cat, {"status": "failed", "error": str(e), "score": None}
            finally:
                s.close()

        if to_run:
            with ThreadPoolExecutor(max_workers=len(to_run)) as pool:
                futures = {pool.submit(_call_signal, cat): cat for cat in to_run}
                for future in as_completed(futures):
                    cat, result = future.result()
                    fresh_results[cat] = result

        # ── Merge skipped + fresh results into unified signal_results ─────────
        # signal_results[cat] = {"score": float, "raw_value": str, "source": "skipped"|"fresh"|"failed"}
        merged: Dict[str, Any] = {}

        for cat in to_skip:
            merged[cat] = {
                "score":     signal_status[cat]["score"],
                "raw_value": signal_status[cat]["raw_value"],
                "source":    "skipped",
            }

        for cat in to_run:
            r = fresh_results.get(cat, {})
            score = r.get("score") or r.get("normalized_score")
            merged[cat] = {
                "score":     score,
                "raw_value": r.get("raw_value"),
                "source":    "failed" if r.get("status") == "failed" else "fresh",
            }

        # ── Build human-readable summary ──────────────────────────────────────
        parts = []
        for cat, info in merged.items():
            score  = info["score"]
            source = info["source"]
            label  = cat.replace("_", " ")
            if source == "skipped":
                parts.append(f"{label}: {score:.1f} (skipped)")
            elif source == "failed":
                parts.append(f"{label}: FAILED")
            else:
                score_str = f"{score:.1f}" if score is not None else "?"
                parts.append(f"{label}: {score_str}")

        summary_msg = " | ".join(parts) if parts else "signals processed"

        # ── LLM sanity check on all non-zero scores ───────────────────────────
        flags = self._sanity_check_scores(ticker, company_name or ticker, merged)

        if flags:
            flag_summary = ", ".join(
                f"{f.category} ({f.score}/100, severity={f.severity})" for f in flags
            )
            logger.warning(f"[{ticker}] Score flags: {flag_summary}")
            summary_msg += f" | ⚠️ {len(flags)} score(s) flagged"

        # ── Check for any hard failures ───────────────────────────────────────
        failed = [cat for cat, info in merged.items() if info["source"] == "failed"]
        if failed:
            # Non-fatal: CS3 can still score with partial signals
            logger.warning(f"[{ticker}] Signals failed (will score with partial data): {failed}")

        return PipelineStepResult(
            step=5, name="Signal Scoring", status="success",
            message=summary_msg,
            data={"signal_results": merged, "skipped": to_skip, "ran": to_run, "failed": failed},
            duration_seconds=time.time() - start,
            signal_flags=flags,
        )

    # ── Step 3 — Parse Documents (NON-FATAL) ──────────────────────────────────

    def _step_parse(self, ticker: str, doc_status: Optional[Dict] = None) -> PipelineStepResult:
        """
        POST /api/v1/documents/parse/{ticker}

        Smart skip: if the pre-check shows total_documents > 0 AND raw_count == 0,
        all documents are already parsed — skip the API call entirely.
        This mirrors how Step 2 skips on HTTP 409 (already exists).
        """
        start = time.time()

        # Pre-check: skip if nothing needs parsing
        # raw_count == 0 means no "raw"/"collected"/"pending" keys in by_status
        # A missing key = truly 0 docs in that state (API only lists non-zero statuses)
        if doc_status:
            total  = doc_status.get("total_documents", 0)
            raw    = doc_status.get("raw_count", 0)
            parsed = doc_status.get("parsed_count", 0)
            chunked = doc_status.get("chunked_count", 0)
            # Skip if: docs exist AND nothing is in raw/collected state
            # (some may be "parsed", some "chunked" — both are fine, nothing left to parse)
            if total > 0 and raw == 0:
                already_done = parsed + chunked
                return PipelineStepResult(
                    step=3, name="Parse Documents", status="skipped",
                    message=f"All {already_done} documents already parsed or chunked — skipping",
                    data=doc_status, duration_seconds=time.time() - start,
                )
        try:
            resp = self._session.post(
                f"{self.base_url}/api/v1/documents/parse/{ticker}",
                timeout=REQUEST_TIMEOUT_LONG,
            )
            resp.raise_for_status()
            data = resp.json()
            parsed = (
                data.get("parsed_count") or data.get("total_parsed")
                or data.get("documents_parsed") or data.get("count", "?")
            )
            skipped = data.get("skipped_count", data.get("skipped", 0))
            msg = f"Parsed {parsed} documents"
            if skipped:
                msg += f" ({skipped} already parsed, skipped)"
            return PipelineStepResult(
                step=3, name="Parse Documents", status="success",
                message=msg, data=data, duration_seconds=time.time() - start,
            )
        except requests.HTTPError as e:
            return PipelineStepResult(
                step=3, name="Parse Documents", status="error",
                message="Parsing failed — leadership & SEC section scores may be missing",
                error=f"HTTP {e.response.status_code}: {e.response.text[:200]}",
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return PipelineStepResult(
                step=3, name="Parse Documents", status="error",
                message="Parsing failed — leadership & SEC section scores may be missing",
                error=str(e), duration_seconds=time.time() - start,
            )

    # ── Step 4 — Chunk Documents (NON-FATAL) ──────────────────────────────────

    def _step_chunk(self, ticker: str, doc_status: Optional[Dict] = None) -> PipelineStepResult:
        """
        POST /api/v1/documents/chunk/{ticker}

        Smart skip: if the pre-check shows chunked_count > 0 AND raw_count == 0
        AND parsed_count == 0 (nothing left un-chunked), skip the API call.
        Uses post-parse doc_status passed in from run_pipeline().
        """
        start = time.time()

        # Pre-check: skip if nothing needs chunking
        # parsed_count == 0 means no "parsed" key in by_status (nothing waiting to be chunked)
        # chunked_count > 0 means at least some docs ARE chunked (not a fresh empty state)
        if doc_status:
            total   = doc_status.get("total_documents", 0)
            chunked = doc_status.get("chunked_count", 0)
            parsed  = doc_status.get("parsed_count", 0)   # parsed-but-not-yet-chunked
            raw     = doc_status.get("raw_count", 0)
            # Skip if: docs exist AND nothing is waiting to be chunked (parsed==0 and raw==0)
            if total > 0 and chunked > 0 and parsed == 0 and raw == 0:
                return PipelineStepResult(
                    step=4, name="Chunk Documents", status="skipped",
                    message=f"All {chunked} documents already chunked — skipping",
                    data=doc_status, duration_seconds=time.time() - start,
                )
        try:
            resp = self._session.post(
                f"{self.base_url}/api/v1/documents/chunk/{ticker}",
                timeout=REQUEST_TIMEOUT_LONG,
            )
            resp.raise_for_status()
            data = resp.json()
            chunks = (
                data.get("chunk_count") or data.get("total_chunks")
                or data.get("chunks_created") or data.get("count", "?")
            )
            return PipelineStepResult(
                step=4, name="Chunk Documents", status="success",
                message=f"Created {chunks} chunks — ready for RAG indexing",
                data=data, duration_seconds=time.time() - start,
            )
        except requests.HTTPError as e:
            return PipelineStepResult(
                step=4, name="Chunk Documents", status="error",
                message="Chunking failed — RAG chatbot will have limited SEC coverage",
                error=f"HTTP {e.response.status_code}: {e.response.text[:200]}",
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return PipelineStepResult(
                step=4, name="Chunk Documents", status="error",
                message="Chunking failed — RAG chatbot will have limited SEC coverage",
                error=str(e), duration_seconds=time.time() - start,
            )

    # ── Step 6 — Glassdoor Culture (NON-FATAL) ────────────────────────────────

    def _step_glassdoor(self, ticker: str) -> PipelineStepResult:
        """
        POST /api/v1/glassdoor-signals/{ticker}
        Scrapes Glassdoor/Indeed/CareerBliss → CultureSignal → writes to S3.
        CS3 scoring reads this at Step 2.5b. Must run BEFORE Step 8.
        """
        start = time.time()
        try:
            resp = self._session.post(
                f"{self.base_url}/api/v1/glassdoor-signals/{ticker}",
                timeout=REQUEST_TIMEOUT_LONG,
            )
            resp.raise_for_status()
            data = resp.json()
            score   = data.get("culture_score", data.get("score", "?"))
            reviews = data.get("reviews_analyzed", data.get("total_reviews", "?"))
            return PipelineStepResult(
                step=6, name="Glassdoor Culture", status="success",
                message=f"Culture score: {score} | Reviews analyzed: {reviews}",
                data=data, duration_seconds=time.time() - start,
            )
        except requests.HTTPError as e:
            return PipelineStepResult(
                step=6, name="Glassdoor Culture", status="error",
                message="Glassdoor scraping failed — culture_change score may be lower",
                error=f"HTTP {e.response.status_code}: {e.response.text[:200]}",
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return PipelineStepResult(
                step=6, name="Glassdoor Culture", status="error",
                message="Glassdoor scraping failed — culture_change score may be lower",
                error=str(e), duration_seconds=time.time() - start,
            )

    # ── Step 7 — Board Governance (NON-FATAL) ─────────────────────────────────

    def _step_board_governance(self, ticker: str) -> PipelineStepResult:
        """
        POST /api/v1/board-governance/analyze/{ticker}
        Parses DEF 14A → board composition → writes to S3.
        CS3 scoring reads this at Step 2.5a. Must run BEFORE Step 8.
        """
        start = time.time()
        try:
            resp = self._session.post(
                f"{self.base_url}/api/v1/board-governance/analyze/{ticker}",
                timeout=REQUEST_TIMEOUT_LONG,
            )
            resp.raise_for_status()
            data = resp.json()
            score   = data.get("governance_score", data.get("score", "?"))
            members = data.get("board_members_analyzed", data.get("total_members", "?"))
            return PipelineStepResult(
                step=7, name="Board Governance", status="success",
                message=f"Governance score: {score} | Board members: {members}",
                data=data, duration_seconds=time.time() - start,
            )
        except requests.HTTPError as e:
            return PipelineStepResult(
                step=7, name="Board Governance", status="error",
                message="Board governance failed — board_composition may be missing",
                error=f"HTTP {e.response.status_code}: {e.response.text[:200]}",
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return PipelineStepResult(
                step=7, name="Board Governance", status="error",
                message="Board governance failed — board_composition may be missing",
                error=str(e), duration_seconds=time.time() - start,
            )

    # ── Step 8 — CS3 Scoring ──────────────────────────────────────────────────

    def _step_score(self, ticker: str) -> PipelineStepResult:
        start = time.time()
        try:
            resp = self._session.post(
                f"{self.base_url}/api/v1/scoring/{ticker}",
                timeout=120,
            )
            resp.raise_for_status()
            data = resp.json()
            orgair = data.get("orgair_score", data.get("org_air_score", "?"))
            vr = data.get("vr_score", "?")
            hr = data.get("hr_score", "?")
            return PipelineStepResult(
                step=8, name="Scoring", status="success",
                message=f"Org-AI-R: {orgair} | VR: {vr} | HR: {hr}",
                data=data, duration_seconds=time.time() - start,
            )
        except requests.HTTPError as e:
            return PipelineStepResult(
                step=8, name="Scoring", status="error",
                message="Scoring failed",
                error=f"HTTP {e.response.status_code}: {e.response.text[:200]}",
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return PipelineStepResult(
                step=8, name="Scoring", status="error",
                message="Scoring failed",
                error=str(e), duration_seconds=time.time() - start,
            )

    # ── Step 9 — Index Evidence ───────────────────────────────────────────────

    def _step_index(self, ticker: str, force: bool = True) -> PipelineStepResult:
        start = time.time()
        try:
            resp = self._session.post(
                f"{self.base_url}/rag/index/{ticker}",
                params={"force": str(force).lower()},
                timeout=REQUEST_TIMEOUT_LONG,
            )
            resp.raise_for_status()
            data = resp.json()
            indexed = data.get("indexed_count", "?")
            return PipelineStepResult(
                step=9, name="Index Evidence", status="success",
                message=f"{indexed} evidence vectors indexed — chatbot ready",
                data=data, duration_seconds=time.time() - start,
            )
        except requests.HTTPError as e:
            return PipelineStepResult(
                step=9, name="Index Evidence", status="error",
                message="Evidence indexing failed",
                error=f"HTTP {e.response.status_code}: {e.response.text[:200]}",
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return PipelineStepResult(
                step=9, name="Index Evidence", status="error",
                message="Evidence indexing failed",
                error=str(e), duration_seconds=time.time() - start,
            )

    # ── Full pipeline orchestration ───────────────────────────────────────────

    def run_pipeline(
        self,
        resolved,
        on_step_start: Optional[Callable[[str], None]] = None,
        on_step_complete: Optional[Callable[[PipelineStepResult], None]] = None,
        on_substep: Optional[Callable[[str, str], None]] = None,
        force_reindex: bool = False,
        force_rescore: bool = False,   # True = re-run signals even if scored today
    ) -> PipelineResult:
        ticker = resolved.ticker
        steps: List[PipelineStepResult] = []

        def substep(step_name: str, msg: str) -> None:
            if on_substep:
                on_substep(step_name, msg)

        def run_step(step_fn) -> PipelineStepResult:
            result = step_fn()
            steps.append(result)
            if on_step_complete:
                on_step_complete(result)
            return result

        # Step 1 — Company Setup (FATAL on error)
        if on_step_start: on_step_start("Company Setup")
        s1 = run_step(lambda: self._step_create_company(resolved))
        if s1.status == "error":
            return self._build_result(ticker, steps)

        company_id: Optional[str] = (
            s1.data.get("id") or s1.data.get("company_id") or s1.data.get("uuid")
        )
        if not company_id:
            company_id = self._get_company_id(ticker)

        # Step 2 — SEC Filings (FATAL on error)
        if on_step_start: on_step_start("SEC Filings")
        s2 = run_step(lambda: self._step_collect_sec(
            ticker, resolved.cik,
            on_substep=lambda msg: substep("SEC Filings", msg),
        ))
        if s2.status == "error":
            return self._build_result(ticker, steps)

        # Step 3 — Parse Documents (NON-FATAL)
        # Pre-check doc status once — used by both parse and chunk to smart-skip
        if on_step_start: on_step_start("Parse Documents")
        doc_status_pre = self._get_doc_status(ticker)
        s3 = run_step(lambda: self._step_parse(ticker, doc_status=doc_status_pre))
        if s3.status == "error":
            logger.warning("[%s] Parse failed — continuing.", ticker)

        # Step 4 — Chunk Documents (NON-FATAL)
        # Re-check doc status after parse so chunk skip reflects current state
        if on_step_start: on_step_start("Chunk Documents")
        doc_status_post_parse = self._get_doc_status(ticker)
        s4 = run_step(lambda: self._step_chunk(ticker, doc_status=doc_status_post_parse))
        if s4.status == "error":
            logger.warning("[%s] Chunk failed — continuing.", ticker)

        # ── Steps 5 + 6 + 7  — CONCURRENT ────────────────────────────────────
        #
        # Signal Scoring (Step 5), Glassdoor (Step 6), and Board Governance (Step 7)
        # are fully independent — none reads the output of another.
        # Running them in parallel saves ~90s (Steps 6+7 no longer wait for Step 5).
        #
        # Implementation: ThreadPoolExecutor with 3 workers.
        # Each step makes a blocking HTTP request to the local FastAPI server.
        # The backend's /signals/score/{ticker}/all now runs its 4 signals in
        # parallel internally (asyncio.gather), so Step 5 itself is ~4 min, not ~9.
        #
        # Step 5 is FATAL — if it errors, abort before Steps 6+7 results matter.
        # Steps 6+7 are NON-FATAL — their failures are logged but don't stop the run.
        #
        # Note: on_step_start callbacks fire before the thread pool starts
        # (all three step names appear in the UI immediately, then update as done).

        if on_step_start:
            on_step_start("Signal Scoring")
            on_step_start("Glassdoor Culture")
            on_step_start("Board Governance")

        _concurrent_results: Dict[str, PipelineStepResult] = {}

        def _run_signal():
            # Each thread gets its own session — requests.Session is NOT thread-safe
            session = requests.Session()
            session.headers.update({"Content-Type": "application/json"})
            self_copy = PipelineClient(self.base_url)
            self_copy._session = session
            return "signal", self_copy._step_signal_scoring(
                ticker,
                company_name=resolved.company_name if hasattr(resolved, "company_name") else ticker,
                on_substep=None,   # Cannot call Streamlit UI from background thread (NoSessionContext)
                skip_if_scored_today=not force_rescore,
            )

        def _run_glassdoor():
            session = requests.Session()
            session.headers.update({"Content-Type": "application/json"})
            self_copy = PipelineClient(self.base_url)
            self_copy._session = session
            return "glassdoor", self_copy._step_glassdoor(ticker)

        def _run_board():
            session = requests.Session()
            session.headers.update({"Content-Type": "application/json"})
            self_copy = PipelineClient(self.base_url)
            self_copy._session = session
            return "board", self_copy._step_board_governance(ticker)

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {
                pool.submit(_run_signal):    "signal",
                pool.submit(_run_glassdoor): "glassdoor",
                pool.submit(_run_board):     "board",
            }
            for future in as_completed(futures):
                try:
                    key, result = future.result()
                    _concurrent_results[key] = result
                except Exception as exc:
                    import traceback
                    key = futures[future]
                    logger.error(
                        f"[{ticker}] Concurrent step '{key}' raised: {exc}\n"
                        f"{traceback.format_exc()}"
                    )
                    step_map = {"signal": (5, "Signal Scoring"), "glassdoor": (6, "Glassdoor Culture"), "board": (7, "Board Governance")}
                    snum, sname = step_map.get(key, (0, key))
                    _concurrent_results[key] = PipelineStepResult(
                        step=snum, name=sname, status="error",
                        message=f"{sname} raised an unexpected exception",
                        error=f"{type(exc).__name__}: {exc}",
                    )

        s5 = _concurrent_results["signal"]
        s6 = _concurrent_results["glassdoor"]
        s7 = _concurrent_results["board"]

        steps.append(s5)
        if on_step_complete: on_step_complete(s5)

        steps.append(s6)
        if on_step_complete: on_step_complete(s6)

        steps.append(s7)
        if on_step_complete: on_step_complete(s7)

        # Step 5 is FATAL
        if s5.status == "error":
            logger.error(f"[{ticker}] Signal scoring failed — aborting pipeline.")
            return self._build_result(ticker, steps)

        if s6.status == "error":
            logger.warning(f"[{ticker}] Glassdoor failed — culture_change may be lower.")
        if s7.status == "error":
            logger.warning(f"[{ticker}] Board governance failed — board_composition may be missing.")
        # ── END CONCURRENT ────────────────────────────────────────────────────

        # Step 8 — CS3 Scoring (FATAL)
        if on_step_start: on_step_start("Scoring")
        s8 = run_step(lambda: self._step_score(ticker))
        if s8.status == "error":
            return self._build_result(ticker, steps)

        # Step 9 — Index Evidence (NON-FATAL)
        if on_step_start: on_step_start("Index Evidence")
        run_step(lambda: self._step_index(ticker, force=True))

        return self._build_result(ticker, steps)

    def _build_result(self, ticker: str, steps: List[PipelineStepResult]) -> PipelineResult:
        error_steps   = [s for s in steps if s.status == "error"]
        success_steps = [s for s in steps if s.status == "success"]
        overall = "partial" if (error_steps and success_steps) else "failed" if error_steps else "success"

        org_air_score = None
        scoring_step = next(
            (s for s in steps if s.name == "Scoring" and s.status == "success"), None
        )
        if scoring_step:
            raw = scoring_step.data.get("orgair_score", scoring_step.data.get("org_air_score"))
            try:
                org_air_score = float(raw) if raw not in (None, "?") else None
            except (ValueError, TypeError):
                org_air_score = None

        index_step = next(
            (s for s in steps if s.name == "Index Evidence" and s.status == "success"), None
        )
        indexed_count = index_step.data.get("indexed_count", 0) if index_step else 0

        # Roll up signal_flags from Step 5 into the top-level result
        # Streamlit reads pipeline_result.signal_flags directly — no need to dig into steps
        signal_flags = []
        signal_step = next(
            (s for s in steps if s.name == "Signal Scoring"), None
        )
        if signal_step and signal_step.signal_flags:
            signal_flags = signal_step.signal_flags

        return PipelineResult(
            ticker=ticker, overall_status=overall, steps=steps,
            org_air_score=org_air_score, indexed_count=indexed_count,
            signal_flags=signal_flags,
        )

    # ── RAG helpers ───────────────────────────────────────────────────────────

    def is_company_indexed(self, ticker: str) -> bool:
        return self.get_company_status(ticker)["chatbot_ready"]

    def ask_chatbot(self, ticker: str, question: str, dimension: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"question": question}
        if dimension:
            params["dimension"] = dimension
        try:
            resp = self._session.get(
                f"{self.base_url}/rag/chatbot/{ticker}",
                params=params, timeout=REQUEST_TIMEOUT_SHORT,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            return {"answer": f"HTTP {e.response.status_code}", "evidence": [], "sources_used": 0, "error": e.response.text[:200]}
        except Exception as e:
            return {"answer": str(e), "evidence": [], "sources_used": 0, "error": str(e)}

    def get_ic_prep(self, ticker: str) -> Dict[str, Any]:
        try:
            resp = self._session.get(f"{self.base_url}/rag/ic-prep/{ticker}", timeout=REQUEST_TIMEOUT_LONG)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"error": str(e)}

    def get_justification(self, ticker: str, dimension: str) -> Dict[str, Any]:
        try:
            resp = self._session.get(f"{self.base_url}/rag/justify/{ticker}/{dimension}", timeout=REQUEST_TIMEOUT_SHORT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"error": str(e)}

    def get_rag_status(self) -> Dict[str, Any]:
        try:
            resp = self._session.get(f"{self.base_url}/rag/status", timeout=REQUEST_TIMEOUT_SHORT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"status": "error", "error": str(e)}