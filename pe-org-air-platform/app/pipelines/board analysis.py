# """
# Task 5.0d: Board Composition Analyzer
# Scrapes SEC EDGAR DEF 14A proxy statements, extracts board members
# and committees, then scores AI governance per case study Table 3.

# Usage:
#     poetry run python app/pipelines/board_analyzer.py
#     poetry run python app/pipelines/board_analyzer.py NVDA JPM WMT GE DG
#     poetry run python app/pipelines/board_analyzer.py --all
#     poetry run python app/pipelines/board_analyzer.py --all --no-cache
# """

# import argparse
# import logging
# import re
# import sys
# import time
# from pathlib import Path
# from typing import Dict, List, Optional, Tuple

# import httpx

# # ── Ensure project root and app/ are on sys.path (for imports + .env)
# _THIS_FILE = Path(__file__).resolve()
# _APP_DIR = _THIS_FILE.parent.parent          # app/
# _PROJECT_ROOT = _APP_DIR.parent              # repo root
# for _p in [str(_PROJECT_ROOT), str(_APP_DIR)]:
#     if _p not in sys.path:
#         sys.path.insert(0, _p)

# from models.evidence import BoardMember, GovernanceSignal  # noqa: E402

# logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
# logger = logging.getLogger(__name__)

# # ═════════════════════════════════════════════════════════════════
# # COMPANY REGISTRY
# # ═════════════════════════════════════════════════════════════════

# class CompanyRegistry:
#     """
#     Central registry for company metadata.
#     Add new companies here — the rest of the pipeline picks them up.
#     """

#     COMPANIES: Dict[str, Dict] = {
#         "NVDA": {
#             "cik": "0001045810",
#             "name": "NVIDIA Corporation",
#             "sector": "technology",
#             "proxy_url": "https://www.sec.gov/Archives/edgar/data/1045810/000104581025000095/nvda-20250512.htm",
#         },
#         "JPM": {
#             "cik": "0000019617",
#             "name": "JPMorgan Chase & Co.",
#             "sector": "financial_services",
#             "proxy_url": "https://www.sec.gov/Archives/edgar/data/19617/000001961725000321/jpm-20250405.htm",
#         },
#         "WMT": {
#             "cik": "0000104169",
#             "name": "Walmart Inc.",
#             "sector": "retail",
#             "proxy_url": None,
#         },
#         "GE": {
#             # ✅ FIX: Correct CIK is 0000040545 (not 0000040554)
#             "cik": "0000040545",
#             "name": "GE Aerospace",
#             "sector": "manufacturing",
#             # ✅ Known 2025 DEF 14A document (March 13, 2025)
#             "proxy_url": "https://www.sec.gov/Archives/edgar/data/40545/000130817925000114/ge4356871-def14a.htm",
#         },
#         "DG": {
#             "cik": "0000034067",
#             "name": "Dollar General Corporation",
#             "sector": "retail",
#             "proxy_url": None,
#         },
#     }

#     @classmethod
#     def get(cls, ticker: str) -> Dict:
#         t = ticker.upper()
#         if t in cls.COMPANIES:
#             return cls.COMPANIES[t]
#         raise ValueError(f"Unknown ticker '{ticker}'")

#     @classmethod
#     def all_tickers(cls) -> List[str]:
#         return list(cls.COMPANIES.keys())


# # ═════════════════════════════════════════════════════════════════
# # SEC EDGAR PROXY SCRAPER
# # ═════════════════════════════════════════════════════════════════

# SEC_HEADERS = {
#     "User-Agent": "OrgAIR-Scoring-Engine cs3-lab@quantuniversity.com",
#     "Accept-Encoding": "gzip, deflate",
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
# }

# EDGAR_DELAY_SECONDS = 0.5
# CACHE_DIR = Path("data/proxy_cache")


# def _cached_path(ticker: str) -> Path:
#     return CACHE_DIR / f"{ticker.upper()}_def14a.html"


# def _save_cache(ticker: str, html: str):
#     CACHE_DIR.mkdir(parents=True, exist_ok=True)
#     path = _cached_path(ticker)
#     path.write_text(html, encoding="utf-8")
#     logger.info(f"[{ticker}] Cached proxy to {path}")


# def fetch_proxy_html(
#     ticker: str,
#     timeout: float = 30.0,
#     use_cache: bool = True,
# ) -> str:
#     """
#     Fetch DEF 14A proxy statement HTML from SEC EDGAR.

#     Resolution order:
#       1. Local cache (data/proxy_cache/{TICKER}_def14a.html)
#       2. Known proxy_url from registry
#       3. EDGAR browse lookup for latest DEF 14A
#     """
#     ticker = ticker.upper()
#     info = CompanyRegistry.get(ticker)

#     cache_file = _cached_path(ticker)
#     if use_cache and cache_file.exists():
#         logger.info(f"[{ticker}] Loading proxy from cache: {cache_file}")
#         return cache_file.read_text(encoding="utf-8")

#     proxy_url = info.get("proxy_url")
#     if proxy_url:
#         logger.info(f"[{ticker}] Fetching known proxy URL: {proxy_url}")
#         time.sleep(EDGAR_DELAY_SECONDS)
#         resp = httpx.get(proxy_url, headers=SEC_HEADERS, timeout=timeout, follow_redirects=True)
#         resp.raise_for_status()
#         _save_cache(ticker, resp.text)
#         return resp.text

#     cik = info["cik"]
#     return _fetch_via_browse_edgar(ticker, cik, timeout)


# def _fetch_via_browse_edgar(ticker: str, cik: str, timeout: float) -> str:
#     """
#     Look up the latest DEF 14A from EDGAR browse page.
#     """
#     browse_url = (
#         "https://www.sec.gov/cgi-bin/browse-edgar"
#         f"?action=getcompany&CIK={cik}&type=DEF+14A&count=10&owner=exclude"
#     )
#     logger.info(f"[{ticker}] Looking up DEF 14A on EDGAR (CIK {cik})...")
#     time.sleep(EDGAR_DELAY_SECONDS)
#     resp = httpx.get(browse_url, headers=SEC_HEADERS, timeout=timeout, follow_redirects=True)
#     resp.raise_for_status()

#     # find the filing "Documents" page link (archives path)
#     m = re.search(r'href="(/Archives/edgar/data/[^"]+-index\.html)"', resp.text, re.IGNORECASE)
#     if not m:
#         raise RuntimeError(f"[{ticker}] No DEF 14A filing index link found on EDGAR browse page")

#     filing_index_url = f"https://www.sec.gov{m.group(1)}"
#     logger.info(f"[{ticker}] Filing index: {filing_index_url}")

#     time.sleep(EDGAR_DELAY_SECONDS)
#     resp2 = httpx.get(filing_index_url, headers=SEC_HEADERS, timeout=timeout, follow_redirects=True)
#     resp2.raise_for_status()

#     # primary .htm document — prefer def14a.htm if present
#     htm = re.search(r'href="([^"]*def14a[^"]*\.htm)"', resp2.text, re.IGNORECASE)
#     if not htm:
#         htm = re.search(r'href="([^"]+\.htm)"', resp2.text, re.IGNORECASE)
#     if not htm:
#         raise RuntimeError(f"[{ticker}] No .htm document found in filing index")

#     doc_url = htm.group(1)
#     if not doc_url.startswith("http"):
#         doc_url = f"https://www.sec.gov{doc_url}"

#     logger.info(f"[{ticker}] Fetching proxy: {doc_url}")
#     time.sleep(EDGAR_DELAY_SECONDS)
#     resp3 = httpx.get(doc_url, headers=SEC_HEADERS, timeout=timeout, follow_redirects=True)
#     resp3.raise_for_status()

#     _save_cache(ticker, resp3.text)
#     return resp3.text


# # ═════════════════════════════════════════════════════════════════
# # HTML PARSER
# # ═════════════════════════════════════════════════════════════════

# def strip_html(html: str) -> str:
#     text = re.sub(r"<[^>]+>", " ", html)
#     text = re.sub(r"&[a-zA-Z]+;", " ", text)
#     text = re.sub(r"&#\d+;", " ", text)
#     return re.sub(r"\s+", " ", text).strip()


# def extract_committees(text: str) -> List[str]:
#     committees: List[str] = []
#     pattern = re.compile(
#         r"\b((?:Audit|Compensation|Nominating|Corporate Governance|Risk|Technology|"
#         r"Digital|Innovation|IT|Public Responsibility|Cybersecurity|"
#         r"Science|Sustainability)"
#         r"(?:\s*(?:and|&|,)\s*(?:Audit|Compensation|Nominating|Corporate Governance|"
#         r"Management Development|Risk|Technology|Digital|Innovation|IT|Cybersecurity|"
#         r"Sustainability|Science))*"
#         r"\s+Committee)\b",
#         re.IGNORECASE,
#     )
#     for m in pattern.finditer(text):
#         name = m.group(1).strip()
#         if name.lower() not in [c.lower() for c in committees]:
#             committees.append(name)
#     return committees


# def extract_board_members_from_proxy(text: str) -> List[BoardMember]:
#     """
#     Extract board member names, titles, bios from proxy text.

#     NOTE: Regex parsing proxies is noisy. We add stronger filters
#     to avoid "required by Item" etc becoming a "person".
#     """
#     members: List[BoardMember] = []
#     seen_names: set = set()

#     bio_pattern = re.compile(
#         r"([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)"
#         r"\s*[,.]?\s*(?:age\s*)?(\d{2})\b"
#         r"(.*?)(?=(?:[A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+\s*[,.]?\s*(?:age\s*)?\d{2})|$)",
#         re.IGNORECASE | re.DOTALL,
#     )

#     BAD_NAME_TOKENS = {
#         "required", "business", "held", "item", "form", "proxy", "statement",
#         "proposal", "section", "table", "annual", "meeting", "appendix",
#         "schedule", "fiscal", "total", "stock",
#     }

#     for m in bio_pattern.finditer(text):
#         name = m.group(1).strip()
#         bio_snippet = (m.group(3) or "")[:800]

#         nl = name.lower()
#         if nl in seen_names:
#             continue
#         # hard filters to prevent trash names
#         if any(tok in nl for tok in BAD_NAME_TOKENS):
#             continue
#         # require a "director-ish" context in the bio block
#         bio_low = bio_snippet.lower()
#         if not any(k in bio_low for k in ["director", "board", "committee", "served", "serves"]):
#             continue

#         seen_names.add(nl)
#         title, is_independent, tenure, member_comms = _parse_bio_details(bio_snippet)

#         members.append(BoardMember(
#             name=name,
#             title=title,
#             committees=member_comms,
#             bio=bio_snippet.strip()[:500],
#             is_independent=is_independent,
#             tenure_years=tenure,
#         ))

#     logger.info(f"Extracted {len(members)} board members")
#     return members


# def _parse_bio_details(bio: str) -> Tuple[str, bool, int, List[str]]:
#     bio_lower = bio.lower()

#     title = "Director"
#     title_map = [
#         (["chief executive", "ceo", "president and ceo"], "President and CEO"),
#         (["chief financial", "cfo"], "CFO"),
#         (["chief technology", "cto"], "CTO"),
#         (["chief data officer", "cdo"], "Chief Data Officer"),
#         (["chief ai officer", "caio"], "Chief AI Officer"),
#         (["lead independent"], "Lead Independent Director"),
#         (["chairman"], "Chairman"),
#     ]
#     for keywords, t in title_map:
#         if any(kw in bio_lower for kw in keywords):
#             title = t
#             break

#     is_independent = "independent" in bio_lower and "not independent" not in bio_lower

#     tenure = 0
#     since_match = re.search(r"director\s+since\s+(\d{4})", bio_lower)
#     if since_match:
#         try:
#             tenure = 2026 - int(since_match.group(1))
#         except ValueError:
#             pass

#     member_comms: List[str] = []
#     for comm in ["Audit", "Compensation", "Nominating", "Risk",
#                  "Technology", "Governance", "Public Responsibility",
#                  "Digital", "Innovation", "Cybersecurity"]:
#         if comm.lower() in bio_lower:
#             member_comms.append(f"{comm} Committee")

#     return title, is_independent, tenure, member_comms


# def extract_strategy_text(text: str) -> str:
#     patterns = [
#         r"(?:BUSINESS\s+OVERVIEW|Business\s+Highlights)(.*?)(?:PROXY\s+SUMMARY|Table\s+of\s+Contents)",
#         r"(?:strategic\s+(?:overview|priorities|highlights))(.*?)(?:Table\s+of\s+Contents)",
#         r"(?:our\s+strategy)(.*?)(?:corporate\s+governance|risk\s+factors)",
#     ]
#     for pat in patterns:
#         match = re.search(pat, text, re.IGNORECASE | re.DOTALL)
#         if match:
#             return match.group(1)[:3000]

#     for keyword in ["business overview", "business highlights", "strategic priorities"]:
#         idx = text.lower().find(keyword)
#         if idx >= 0:
#             return text[idx: idx + 3000]

#     return ""


# # ═════════════════════════════════════════════════════════════════
# # GOVERNANCE SCORER — case study Table 3
# # ═════════════════════════════════════════════════════════════════

# class BoardCompositionAnalyzer:
#     AI_EXPERTISE_KEYWORDS = [
#         "artificial intelligence", "machine learning",
#         "chief data officer", "cdo", "caio", "chief ai",
#         "chief technology", "cto", "chief digital",
#         "data science", "analytics", "digital transformation",
#     ]

#     TECH_COMMITTEE_NAMES = [
#         "technology committee", "digital committee",
#         "innovation committee", "it committee",
#         "technology and cybersecurity",
#     ]

#     DATA_OFFICER_TITLES = [
#         "chief data officer", "cdo",
#         "chief ai officer", "caio",
#         "chief analytics officer", "cao",
#         "chief digital officer",
#     ]

#     AI_STRATEGY_KEYWORDS = [
#         "artificial intelligence", "machine learning",
#         "ai strategy", "ai-driven", "ai transformation",
#         "generative ai", "ai initiative", "ai applications",
#         "large language model", "deep learning",
#     ]

#     def analyze_board(
#         self,
#         company_id: str,
#         ticker: str,
#         members: List[BoardMember],
#         committees: List[str],
#         strategy_text: str = "",
#     ) -> GovernanceSignal:
#         score = 20.0
#         relevant_comms: List[str] = []

#         has_tech = any(any(tc in c.lower() for tc in self.TECH_COMMITTEE_NAMES) for c in committees)
#         if has_tech:
#             score += 15.0
#             relevant_comms.extend([c for c in committees if "tech" in c.lower() or "digital" in c.lower() or "innovation" in c.lower()])

#         ai_experts: List[str] = []
#         for member in members:
#             combined = f"{member.bio} {member.title}".lower()
#             if any(kw in combined for kw in self.AI_EXPERTISE_KEYWORDS):
#                 ai_experts.append(member.name)
#         if ai_experts:
#             score += 20.0

#         has_data_officer = False
#         for member in members:
#             combined = f"{member.title} {member.bio}".lower()
#             if any(dt in combined for dt in self.DATA_OFFICER_TITLES):
#                 has_data_officer = True
#                 break
#         if has_data_officer:
#             score += 15.0

#         independent_ratio = 0.0
#         if members:
#             independent_ratio = sum(1 for m in members if m.is_independent) / len(members)
#         if independent_ratio > 0.5:
#             score += 10.0

#         has_risk_tech = any(("risk" in c.lower() and any(w in c.lower() for w in ["technology", "cyber", "digital"])) for c in committees)
#         if has_risk_tech:
#             score += 10.0
#             relevant_comms.extend([c for c in committees if "risk" in c.lower()])

#         has_ai_in_strategy = False
#         if strategy_text and any(kw in strategy_text.lower() for kw in self.AI_STRATEGY_KEYWORDS):
#             has_ai_in_strategy = True
#             score += 10.0

#         score = min(score, 100.0)
#         confidence = min(0.5 + len(members) / 20.0, 0.95)

#         return GovernanceSignal(
#             company_id=company_id,
#             ticker=ticker,
#             has_tech_committee=has_tech,
#             has_ai_expertise=len(ai_experts) > 0,
#             has_data_officer=has_data_officer,
#             has_risk_tech_oversight=has_risk_tech,
#             has_ai_in_strategy=has_ai_in_strategy,
#             tech_expertise_count=len(ai_experts),
#             independent_ratio=round(independent_ratio, 4),
#             governance_score=score,
#             confidence=round(confidence, 4),
#             ai_experts=ai_experts,
#             relevant_committees=relevant_comms,
#             board_members=[m.model_dump() for m in members[:5]],
#         )

#     def scrape_and_analyze(
#         self,
#         ticker: str,
#         company_id: Optional[str] = None,
#         use_cache: bool = True,
#     ) -> GovernanceSignal:
#         ticker = ticker.upper()
#         info = CompanyRegistry.get(ticker)
#         cid = company_id or ticker

#         logger.info(f"=== Board analysis: {ticker} ({info['name']}) ===")

#         raw_html = fetch_proxy_html(ticker, use_cache=use_cache)
#         plain_text = strip_html(raw_html)
#         logger.info(f"[{ticker}] Proxy text: {len(plain_text):,} chars")

#         committees = extract_committees(plain_text)
#         logger.info(f"[{ticker}] Committees: {committees}")

#         members = extract_board_members_from_proxy(plain_text)
#         for m in members[:3]:
#             logger.info(f"[{ticker}]   {m.name} | {m.title} | indep={m.is_independent}")

#         strategy_text = extract_strategy_text(plain_text)
#         logger.info(f"[{ticker}] Strategy text: {len(strategy_text):,} chars")

#         return self.analyze_board(cid, ticker, members, committees, strategy_text)

#     def analyze_multiple(
#         self,
#         tickers: List[str],
#         use_cache: bool = True,
#         delay: float = 1.0,
#     ) -> Dict[str, GovernanceSignal]:
#         results: Dict[str, GovernanceSignal] = {}
#         for i, ticker in enumerate(tickers):
#             try:
#                 signal = self.scrape_and_analyze(ticker, use_cache=use_cache)
#                 results[ticker.upper()] = signal
#             except Exception as e:
#                 logger.error(f"[{ticker}] FAILED: {e}")
#             if i < len(tickers) - 1:
#                 logger.info(f"Waiting {delay}s before next company...")
#                 time.sleep(delay)
#         return results


# # ═════════════════════════════════════════════════════════════════
# # OUTPUT HELPERS
# # ═════════════════════════════════════════════════════════════════

# def print_signal(signal: GovernanceSignal):
#     info = CompanyRegistry.get(signal.ticker)
#     print(f"\n{'=' * 60}")
#     print(f"  BOARD GOVERNANCE — {signal.ticker} ({info['name']})")
#     print(f"{'=' * 60}")
#     print(f"  Governance Score:       {signal.governance_score}/100")
#     print(f"  Confidence:             {signal.confidence}")
#     print(f"  Independent Ratio:      {signal.independent_ratio}")
#     print(f"  Tech Expertise Count:   {signal.tech_expertise_count}")


# def save_signal(signal: GovernanceSignal, out_dir: str = "results") -> Path:
#     d = Path(out_dir)
#     d.mkdir(parents=True, exist_ok=True)
#     path = d / f"{signal.ticker.lower()}_governance.json"
#     path.write_text(signal.model_dump_json(indent=2))
#     logger.info(f"[{signal.ticker}] Saved → {path}")
#     return path


# # ═════════════════════════════════════════════════════════════════
# # MAIN
# # ═════════════════════════════════════════════════════════════════

# def main():
#     parser = argparse.ArgumentParser(description="Board Composition Analyzer (DEF 14A)")
#     parser.add_argument("tickers", nargs="*", help="Tickers to analyze (e.g., NVDA JPM WMT GE DG)")
#     parser.add_argument("--all", action="store_true", help="Analyze all registered tickers")
#     parser.add_argument("--no-cache", action="store_true", help="Ignore local HTML cache and refetch")
#     parser.add_argument("--delay", type=float, default=1.0, help="Delay between companies (seconds)")
#     args = parser.parse_args()

#     if args.all:
#         tickers = CompanyRegistry.all_tickers()
#     elif args.tickers:
#         tickers = [t.upper() for t in args.tickers]
#     else:
#         tickers = ["NVDA"]

#     analyzer = BoardCompositionAnalyzer()
#     results = analyzer.analyze_multiple(tickers, use_cache=(not args.no_cache), delay=args.delay)

#     print("\n\n" + "#" * 60)
#     print(f"#  GOVERNANCE ANALYSIS — {len(results)} companies")
#     print("#" * 60)

#     for _, signal in results.items():
#         print_signal(signal)
#         save_signal(signal)

#     if len(results) > 1:
#         print(f"\n{'=' * 60}")
#         print(f"  {'Ticker':<8} {'Score':>6} {'Indep%':>7} {'AI Exp':>7} {'Conf':>6}")
#         print(f"  {'-'*8} {'-'*6} {'-'*7} {'-'*7} {'-'*6}")
#         for t, s in sorted(results.items(), key=lambda x: x[1].governance_score, reverse=True):
#             print(
#                 f"  {t:<8} {s.governance_score:>5.0f}  "
#                 f"{s.independent_ratio*100:>5.1f}%  "
#                 f"{s.tech_expertise_count:>5}   "
#                 f"{s.confidence:>5.2f}"
#             )
#         print(f"{'=' * 60}")


# if __name__ == "__main__":
#     main()

"""
Task 5.0d: Board Composition Analyzer (Case Study 3)

Scrapes SEC EDGAR DEF 14A proxy statements, extracts board members and committees,
then scores AI governance per Case Study 3 Table 3.

Scoring (Table 3):
  Base score                               20
  Technology/Digital committee exists      +15
  Board member with AI/ML expertise        +20
  CAIO, CDO, or CTO on executive team      +15
  Independent director ratio > 0.5         +10
  Risk committee with tech oversight       +10
  AI mentioned in strategic priorities     +10
  Max                                     100

Usage:
  poetry run python -m app.pipelines.board_analyzer                 # default: 5 tickers
  poetry run python -m app.pipelines.board_analyzer NVDA JPM WMT    # specific tickers
  poetry run python -m app.pipelines.board_analyzer --all           # all registered
  poetry run python -m app.pipelines.board_analyzer --no-cache      # bypass cache
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx


# ────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("board_analyzer")


# ────────────────────────────────────────────────────────────────
# Data Models (match CS3 spec)
# ────────────────────────────────────────────────────────────────

@dataclass
class BoardMember:
    """A board member or executive."""
    name: str
    title: str
    committees: List[str]
    bio: str
    is_independent: bool
    tenure_years: int


@dataclass
class GovernanceSignal:
    """Board-derived governance signal."""
    company_id: str
    ticker: str

    has_tech_committee: bool
    has_ai_expertise: bool
    has_data_officer: bool
    has_risk_tech_oversight: bool
    has_ai_in_strategy: bool

    tech_expertise_count: int
    independent_ratio: float

    governance_score: float
    confidence: float

    ai_experts: List[str] = field(default_factory=list)
    relevant_committees: List[str] = field(default_factory=list)
    board_members: List[dict] = field(default_factory=list)


# ────────────────────────────────────────────────────────────────
# Company Registry
# ────────────────────────────────────────────────────────────────

class CompanyRegistry:
    """
    Central registry for company metadata.
    Add new companies here — the rest of the pipeline picks them up.
    """

    # ticker → { cik, name, sector, proxy_url (optional cache) }
    COMPANIES: Dict[str, Dict] = {
        "NVDA": {
            "cik": "0001045810",
            "name": "NVIDIA Corporation",
            "sector": "technology",
            "proxy_url": "https://www.sec.gov/Archives/edgar/data/1045810/000104581025000095/nvda-20250512.htm",
        },
        "JPM": {
            "cik": "0000019617",
            "name": "JPMorgan Chase & Co.",
            "sector": "financial_services",
            "proxy_url": "https://www.sec.gov/Archives/edgar/data/19617/000001961725000321/jpm-20250405.htm",
        },
        "WMT": {
            "cik": "0000104169",
            "name": "Walmart Inc.",
            "sector": "retail",
            "proxy_url": None,
        },
        "GE": {
            "cik": "0000040554",
            "name": "GE Aerospace",
            "sector": "manufacturing",
            "proxy_url": None,
        },
        "DG": {
            "cik": "0000034067",
            "name": "Dollar General Corporation",
            "sector": "retail",
            "proxy_url": None,
        },
    }

    @classmethod
    def get(cls, ticker: str) -> Dict:
        t = ticker.upper()
        if t in cls.COMPANIES:
            return cls.COMPANIES[t]
        raise ValueError(
            f"Unknown ticker '{ticker}'. Register it with "
            f"CompanyRegistry.register('{ticker}', cik='...', name='...', sector='...')"
        )

    @classmethod
    def register(
        cls,
        ticker: str,
        cik: str,
        name: str,
        sector: str = "unknown",
        proxy_url: Optional[str] = None,
    ):
        cls.COMPANIES[ticker.upper()] = {
            "cik": cik,
            "name": name,
            "sector": sector,
            "proxy_url": proxy_url,
        }
        logger.info(f"Registered {ticker.upper()} (CIK {cik})")

    @classmethod
    def all_tickers(cls) -> List[str]:
        return list(cls.COMPANIES.keys())


# ────────────────────────────────────────────────────────────────
# SEC EDGAR Proxy Scraper
# ────────────────────────────────────────────────────────────────

SEC_HEADERS = {
    "User-Agent": "OrgAIR-Scoring-Engine cs3-lab@quantuniversity.com",
    "Accept-Encoding": "gzip, deflate",
}

EDGAR_DELAY_SECONDS = 0.5
CACHE_DIR = Path("data/proxy_cache")


def _cached_path(ticker: str) -> Path:
    return CACHE_DIR / f"{ticker.upper()}_def14a.html"


def _save_cache(ticker: str, html: str):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cached_path(ticker)
    path.write_text(html, encoding="utf-8")
    logger.info(f"[{ticker}] Cached proxy to {path}")


def fetch_proxy_html(
    ticker: str,
    timeout: float = 30.0,
    use_cache: bool = True,
) -> str:
    """
    Fetch DEF 14A proxy statement HTML from SEC EDGAR.

    Resolution order:
      1. Local cache (data/proxy_cache/{TICKER}_def14a.html)
      2. Known proxy_url from registry
      3. EDGAR browse-edgar latest DEF 14A filing index → primary .htm
    """
    ticker = ticker.upper()
    info = CompanyRegistry.get(ticker)

    # 1) cache
    cache_file = _cached_path(ticker)
    if use_cache and cache_file.exists():
        logger.info(f"[{ticker}] Loading proxy from cache: {cache_file}")
        return cache_file.read_text(encoding="utf-8")

    # 2) known URL
    proxy_url = info.get("proxy_url")
    if proxy_url:
        logger.info(f"[{ticker}] Fetching known proxy URL: {proxy_url}")
        resp = httpx.get(proxy_url, headers=SEC_HEADERS, timeout=timeout, follow_redirects=True)
        resp.raise_for_status()
        _save_cache(ticker, resp.text)
        return resp.text

    # 3) lookup latest DEF 14A in browse-edgar
    cik = info["cik"]
    browse_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={cik}&type=DEF+14A&count=1&owner=include"
    )
    logger.info(f"[{ticker}] Looking up latest DEF 14A on EDGAR (CIK {cik})...")
    time.sleep(EDGAR_DELAY_SECONDS)
    resp = httpx.get(browse_url, headers=SEC_HEADERS, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()

    # Find a filing index link (/Archives/edgar/data/.../index.html)
    m = re.search(r'href="(/Archives/edgar/data/[^"]+?index\.html)"', resp.text, re.IGNORECASE)
    if not m:
        # fallback: first /Archives/edgar/data/ link
        m = re.search(r'href="(/Archives/edgar/data/[^"]+)"', resp.text, re.IGNORECASE)
    if not m:
        raise RuntimeError(f"[{ticker}] No DEF 14A filing link found on browse-edgar page")

    filing_index_url = f"https://www.sec.gov{m.group(1)}"
    logger.info(f"[{ticker}] Filing index: {filing_index_url}")

    time.sleep(EDGAR_DELAY_SECONDS)
    resp2 = httpx.get(filing_index_url, headers=SEC_HEADERS, timeout=timeout, follow_redirects=True)
    resp2.raise_for_status()

    # Prefer "Complete submission text file"? no, we want primary .htm proxy doc.
    # Heuristic: first .htm that is not "R1.htm" exhibits if possible.
    htm_links = re.findall(r'href="([^"]+\.htm)"', resp2.text, flags=re.IGNORECASE)
    if not htm_links:
        raise RuntimeError(f"[{ticker}] No .htm documents found in filing index")

    # choose best candidate
    def score_doc(u: str) -> int:
        u_low = u.lower()
        s = 0
        if "def" in u_low or "proxy" in u_low:
            s += 3
        if "ex" in u_low or "exhibit" in u_low:
            s -= 2
        if u_low.endswith(".htm"):
            s += 1
        return s

    best = sorted(htm_links, key=score_doc, reverse=True)[0]
    doc_url = best if best.startswith("http") else f"https://www.sec.gov{best}"

    logger.info(f"[{ticker}] Fetching proxy doc: {doc_url}")
    time.sleep(EDGAR_DELAY_SECONDS)
    resp3 = httpx.get(doc_url, headers=SEC_HEADERS, timeout=timeout, follow_redirects=True)
    resp3.raise_for_status()

    _save_cache(ticker, resp3.text)
    return resp3.text


# ────────────────────────────────────────────────────────────────
# HTML → Text and Parsing Helpers
# ────────────────────────────────────────────────────────────────

_MONTHS = {"january","february","march","april","may","june","july","august","september","october","november","december"}
_BAD_NAME_TOKENS = {
    "item","form","proxy","statement","required","business","annual","meeting","proposal","section","table","schedule",
    "total","fiscal","year","held","date","stock","shares","vote","voting","record","notice","appendix","part","page"
}

def strip_html(html: str) -> str:
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;|&amp;|&quot;|&apos;|&lt;|&gt;", " ", text)
    text = re.sub(r"&#\d+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _is_plausible_person_name(name: str) -> bool:
    """
    Strong filter to prevent garbage like "required by Item" from being treated as a person.
    Accept 2-4 tokens, each starting with capital, allow middle initial.
    """
    name = name.strip()
    if len(name) < 5 or len(name) > 60:
        return False

    parts = name.split()
    if len(parts) < 2 or len(parts) > 4:
        return False

    # allow middle initial like "A." or "A"
    for p in parts:
        p_clean = p.replace(".", "")
        if not p_clean:
            return False
        if not p_clean[0].isalpha() or not p_clean[0].isupper():
            return False
        # reject obvious non-names
        if p_clean.lower() in _MONTHS:
            return False
        if p_clean.lower() in _BAD_NAME_TOKENS:
            return False
        # reject token that is mostly digits
        if sum(ch.isdigit() for ch in p_clean) > 0:
            return False

    # reject if any token is too generic
    low = name.lower()
    if any(tok in low for tok in ["item ", "form ", "proxy statement", "schedule "]):
        return False

    return True


def extract_committees(text: str) -> List[str]:
    """
    Extract named board committees from proxy text.

    We include classic + tech/digital/cyber + retail variants (eCommerce, omnichannel).
    """
    committees: List[str] = []
    # common committee patterns
    pat = re.compile(
        r"\b("
        r"(?:Audit|Compensation|Nominating|Governance|Corporate Governance|Risk|Technology|Digital|Innovation|IT|"
        r"Cybersecurity|Science|Sustainability|Public Responsibility|ESG|Data|AI|Artificial Intelligence|"
        r"E[- ]?Commerce|Omnichannel|Supply Chain)"
        r"(?:\s*(?:and|&|,)\s*(?:Audit|Compensation|Nominating|Governance|Corporate Governance|Risk|Technology|Digital|"
        r"Innovation|IT|Cybersecurity|Sustainability|Public Responsibility|ESG|Data|AI|E[- ]?Commerce|Omnichannel|"
        r"Supply Chain))*"
        r"\s+Committee"
        r")\b",
        re.IGNORECASE,
    )

    seen = set()
    for m in pat.finditer(text):
        name = " ".join(m.group(1).split())
        key = name.lower()
        if key not in seen:
            seen.add(key)
            committees.append(name)

    return committees


def _parse_bio_details(bio: str) -> Tuple[str, bool, int, List[str]]:
    bio_lower = bio.lower()

    # Title
    title = "Director"
    title_map = [
        (["president and ceo", "chief executive", " ceo "], "President and CEO"),
        (["chief financial", " cfo "], "CFO"),
        (["chief technology", " cto "], "CTO"),
        (["chief information", " cio "], "CIO"),
        (["chief data officer", " cdo "], "Chief Data Officer"),
        (["chief ai officer", " caio "], "Chief AI Officer"),
        (["chief analytics", "cao", "chief digital"], "Chief Digital/Analytics Officer"),
        (["lead independent"], "Lead Independent Director"),
        (["chairman"], "Chairman"),
    ]
    for kws, t in title_map:
        if any(kw in bio_lower for kw in kws):
            title = t
            break

    # Independence
    is_independent = ("independent" in bio_lower) and ("not independent" not in bio_lower)

    # Tenure (best-effort)
    tenure = 0
    since_match = re.search(r"\bdirector\s+since\s+(\d{4})\b", bio_lower)
    if since_match:
        try:
            tenure = max(0, 2026 - int(since_match.group(1)))
        except ValueError:
            tenure = 0

    # Committees in bio
    member_comms: List[str] = []
    for comm in [
        "audit", "compensation", "nominating", "governance", "risk", "technology",
        "digital", "innovation", "cybersecurity", "sustainability", "esg", "data", "ai",
        "e-commerce", "omnichannel", "supply chain",
    ]:
        if comm in bio_lower:
            member_comms.append(f"{comm.title()} Committee")

    return title, is_independent, tenure, member_comms


def extract_board_members_from_proxy(text: str) -> List[BoardMember]:
    """
    Extract board member names and bios.

    Fixes your current false-positives by:
      - requiring explicit 'Age' label near the name
      - validating name tokens
      - heavy stopword filtering
    """
    members: List[BoardMember] = []
    seen = set()

    # Pattern: "Firstname M. Lastname, Age 55" OR "Firstname Lastname Age 55"
    bio_pattern = re.compile(
        r"\b([A-Z][a-z]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][a-z]+){1,2})\b"
        r"(?:\s*,)?\s*(?:Age|age)\s*(\d{2})\b"
        r"(.*?)(?=\b[A-Z][a-z]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][a-z]+){1,2}\b(?:\s*,)?\s*(?:Age|age)\s*\d{2}\b|$)",
        re.DOTALL,
    )

    for m in bio_pattern.finditer(text):
        name = " ".join(m.group(1).split())
        if not _is_plausible_person_name(name):
            continue
        key = name.lower()
        if key in seen:
            continue

        bio = (m.group(3) or "").strip()
        # extra guard: throw away bios that are clearly not bios
        bio_low = bio.lower()
        if any(bad in bio_low[:200] for bad in ["table of contents", "proposal", "item 1", "item 7", "signatures"]):
            continue

        seen.add(key)
        title, indep, tenure, comms = _parse_bio_details(bio[:1200])
        members.append(
            BoardMember(
                name=name,
                title=title,
                committees=comms,
                bio=bio[:600],
                is_independent=indep,
                tenure_years=tenure,
            )
        )

        # Safety cap: a real board should not be 150+ people.
        if len(members) >= 40:
            break

    # If extraction failed badly, do a weaker fallback (but still filtered)
    if len(members) < 3:
        honorific = re.compile(r"\b(?:Mr\.|Ms\.|Mrs\.|Dr\.)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b")
        for m in honorific.finditer(text):
            name = " ".join(m.group(1).split())
            if not _is_plausible_person_name(name):
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            start = max(0, m.start() - 120)
            ctx = text[start : m.end() + 600]
            title, indep, tenure, comms = _parse_bio_details(ctx)
            members.append(
                BoardMember(
                    name=name,
                    title=title,
                    committees=comms,
                    bio=ctx[:600],
                    is_independent=indep,
                    tenure_years=tenure,
                )
            )
            if len(members) >= 25:
                break

    logger.info(f"Extracted {len(members)} board members")
    return members


def extract_strategy_text(text: str) -> str:
    """
    Best-effort: extract a chunk likely to include strategy/priorities.
    (Used only for the '+10 AI in strategic priorities' check.)
    """
    t = text.lower()

    anchors = [
        "strategic priorities",
        "our strategy",
        "strategy",
        "business strategy",
        "digital strategy",
        "technology strategy",
        "ai",
        "artificial intelligence",
        "machine learning",
        "generative ai",
    ]
    # try window around first meaningful anchor among top-level strategy anchors
    for a in ["strategic priorities", "our strategy", "business strategy", "strategy"]:
        idx = t.find(a)
        if idx != -1:
            return text[idx : idx + 6000]

    # fallback: window around AI mention
    for a in ["artificial intelligence", "machine learning", "generative ai", "ai "]:
        idx = t.find(a)
        if idx != -1:
            start = max(0, idx - 1500)
            return text[start : start + 6000]

    return ""


# ────────────────────────────────────────────────────────────────
# Analyzer (Table 3 scoring)
# ────────────────────────────────────────────────────────────────

class BoardCompositionAnalyzer:
    """
    Implements Case Study 3 Table 3 scoring.
    """

    # Keep existing keywords + add Walmart/DG-style wording (retail ops, supply chain, etc.)
    AI_EXPERTISE_KEYWORDS = [
        # existing
        "artificial intelligence", "machine learning",
        "chief data officer", "cdo", "caio", "chief ai",
        "chief technology", "cto", "chief digital",
        "data science", "analytics", "digital transformation",
        # added (retail/manufacturing language)
        "generative ai", "genai", "large language model", "llm", "deep learning",
        "computer vision", "natural language processing", "nlp",
        "optimization", "operations research", "forecasting", "demand forecasting",
        "supply chain", "logistics", "fulfillment", "last mile", "warehouse automation",
        "inventory", "pricing analytics", "recommendation", "personalization",
        "automation", "robotics", "industrial iot", "predictive maintenance",
        "decision science", "data & analytics", "data and analytics", "advanced analytics",
        "cloud platform", "mlops", "model risk", "responsible ai",
    ]

    TECH_COMMITTEE_NAMES = [
        # existing
        "technology committee", "digital committee",
        "innovation committee", "it committee",
        "technology and cybersecurity",
        # added variants
        "technology & cybersecurity",
        "technology and cyber",
        "cybersecurity committee",
        "data committee",
        "ai committee",
        "artificial intelligence committee",
        "e-commerce committee",
        "ecommerce committee",
        "omnichannel committee",
        "supply chain committee",
    ]

    DATA_OFFICER_TITLES = [
        # existing
        "chief data officer", "cdo",
        "chief ai officer", "caio",
        "chief analytics officer", "cao",
        "chief digital officer",
        # added (common in retail)
        "chief information officer", "cio",
        "svp data", "vp data", "head of data", "head of analytics",
        "chief technology officer", "cto",
    ]

    # risk committee tech oversight keywords
    RISK_TECH_WORDS = ["technology", "cyber", "digital", "information security", "security", "data", "privacy", "ai"]

    AI_STRATEGY_KEYWORDS = [
        "artificial intelligence", "machine learning",
        "generative ai", "genai", "ai strategy", "ai-driven",
        "automation", "intelligent", "advanced analytics",
        "data science", "computer vision", "optimization",
        "supply chain", "fulfillment", "logistics",
    ]

    def analyze_board(
        self,
        company_id: str,
        ticker: str,
        members: List[BoardMember],
        committees: List[str],
        strategy_text: str = "",
    ) -> GovernanceSignal:
        """
        Score board governance indicators per Case Study 3 Table 3.
        """
        score = 20.0  # Base score
        relevant_comms: List[str] = []

        committees_low = [c.lower() for c in committees]

        # 1) Technology/Digital committee exists (+15)
        has_tech = any(any(tc in c for tc in self.TECH_COMMITTEE_NAMES) for c in committees_low)
        if has_tech:
            score += 15.0
            for c in committees:
                cl = c.lower()
                if any(tc in cl for tc in self.TECH_COMMITTEE_NAMES):
                    relevant_comms.append(c)

        # 2) Board member with AI/ML expertise (+20)
        ai_experts: List[str] = []
        for m in members:
            combined = f"{m.title} {m.bio}".lower()
            if any(kw in combined for kw in self.AI_EXPERTISE_KEYWORDS):
                ai_experts.append(m.name)
        has_ai_expertise = len(ai_experts) > 0
        if has_ai_expertise:
            score += 20.0

        # 3) CAIO, CDO, or CTO on executive team (+15)
        # We approximate by scanning extracted members' titles/bios for these roles.
        has_data_officer = False
        for m in members:
            combined = f"{m.title} {m.bio}".lower()
            if any(dt in combined for dt in self.DATA_OFFICER_TITLES):
                # only count it if it looks like an executive role (avoid random committee mentions)
                if any(exec_word in combined for exec_word in ["chief", "officer", "vp", "svp", "head", "president"]):
                    has_data_officer = True
                    break
        if has_data_officer:
            score += 15.0

        # 4) Independent director ratio > 0.5 (+10)
        independent_ratio = 0.0
        if members:
            independent_ratio = sum(1 for m in members if m.is_independent) / len(members)
        if independent_ratio > 0.5:
            score += 10.0

        # 5) Risk committee with tech oversight (+10)
        has_risk_tech = False
        for c in committees:
            cl = c.lower()
            if "risk" in cl and any(w in cl for w in self.RISK_TECH_WORDS):
                has_risk_tech = True
                relevant_comms.append(c)
                break
        # also check member committees extracted from bios
        if not has_risk_tech:
            for m in members:
                for mc in m.committees:
                    ml = mc.lower()
                    if "risk" in ml and any(w in ml for w in self.RISK_TECH_WORDS):
                        has_risk_tech = True
                        break
                if has_risk_tech:
                    break
        if has_risk_tech:
            score += 10.0

        # 6) AI mentioned in strategic priorities (+10)
        has_ai_in_strategy = False
        if strategy_text and any(kw in strategy_text.lower() for kw in self.AI_STRATEGY_KEYWORDS):
            has_ai_in_strategy = True
            score += 10.0

        score = min(score, 100.0)

        # Confidence: simple heuristic (matches your earlier style)
        confidence = min(0.50 + (len(members) / 20.0), 0.95)

        # De-dup committees in output
        relevant_comms = list(dict.fromkeys(relevant_comms))

        return GovernanceSignal(
            company_id=company_id,
            ticker=ticker.upper(),
            has_tech_committee=has_tech,
            has_ai_expertise=has_ai_expertise,
            has_data_officer=has_data_officer,
            has_risk_tech_oversight=has_risk_tech,
            has_ai_in_strategy=has_ai_in_strategy,
            tech_expertise_count=len(ai_experts),
            independent_ratio=round(independent_ratio, 4),
            governance_score=float(round(score, 2)),
            confidence=float(round(confidence, 4)),
            ai_experts=ai_experts[:25],
            relevant_committees=relevant_comms[:25],
            board_members=[
                {
                    "name": m.name,
                    "title": m.title,
                    "is_independent": m.is_independent,
                    "tenure_years": m.tenure_years,
                    "committees": m.committees[:6],
                }
                for m in members[:12]
            ],
        )

    def scrape_and_analyze(
        self,
        ticker: str,
        company_id: Optional[str] = None,
        use_cache: bool = True,
    ) -> GovernanceSignal:
        ticker = ticker.upper()
        info = CompanyRegistry.get(ticker)
        cid = company_id or ticker

        logger.info(f"=== Board analysis: {ticker} ({info['name']}) ===")

        raw_html = fetch_proxy_html(ticker, use_cache=use_cache)
        plain_text = strip_html(raw_html)
        logger.info(f"[{ticker}] Proxy text: {len(plain_text):,} chars")

        committees = extract_committees(plain_text)
        logger.info(f"[{ticker}] Committees found: {committees[:12]}{'...' if len(committees) > 12 else ''}")

        members = extract_board_members_from_proxy(plain_text)
        for m in members[:3]:
            logger.info(f"[{ticker}]   {m.name} | {m.title} | indep={m.is_independent}")

        strategy_text = extract_strategy_text(plain_text)
        logger.info(f"[{ticker}] Strategy text window: {len(strategy_text):,} chars")

        return self.analyze_board(cid, ticker, members, committees, strategy_text)

    def analyze_multiple(
        self,
        tickers: List[str],
        use_cache: bool = True,
        delay: float = 1.0,
    ) -> Dict[str, GovernanceSignal]:
        results: Dict[str, GovernanceSignal] = {}
        for i, t in enumerate(tickers):
            ticker = t.upper()
            try:
                signal = self.scrape_and_analyze(ticker, use_cache=use_cache)
                results[ticker] = signal
            except Exception as e:
                logger.error(f"[{ticker}] FAILED: {e}")
            if i < len(tickers) - 1:
                time.sleep(delay)
        return results


# ────────────────────────────────────────────────────────────────
# Output Helpers
# ────────────────────────────────────────────────────────────────

def print_signal(signal: GovernanceSignal):
    info = CompanyRegistry.get(signal.ticker)
    print(f"\n{'=' * 60}")
    print(f"  BOARD GOVERNANCE — {signal.ticker} ({info['name']})")
    print(f"{'=' * 60}")
    print(f"  Governance Score:       {signal.governance_score}/100")
    print(f"  Confidence:             {signal.confidence}")
    print(f"  Independent Ratio:      {signal.independent_ratio}")
    print(f"  Tech Expertise Count:   {signal.tech_expertise_count}")
    print()
    print("  Indicators:                 Points")
    print(f"    Base:                     20")
    print(f"    Tech Committee:           {'YES (+15)' if signal.has_tech_committee else 'NO  (+0)'}")
    print(f"    AI Expertise:             {'YES (+20)' if signal.has_ai_expertise else 'NO  (+0)'}")
    print(f"    CAIO/CDO/CTO Present:      {'YES (+15)' if signal.has_data_officer else 'NO  (+0)'}")
    print(f"    Risk+Tech Oversight:      {'YES (+10)' if signal.has_risk_tech_oversight else 'NO  (+0)'}")
    print(f"    AI in Strategy:           {'YES (+10)' if signal.has_ai_in_strategy else 'NO  (+0)'}")
    print(f"    Independent > 50%:        {'YES (+10)' if signal.independent_ratio > 0.5 else 'NO  (+0)'}")

    if signal.ai_experts:
        print(f"\n  AI/Tech Experts (sample): {', '.join(signal.ai_experts[:8])}")
    if signal.relevant_committees:
        print(f"  Relevant Committees:      {', '.join(signal.relevant_committees[:8])}")


def save_signal(signal: GovernanceSignal, out_dir: str = "results") -> Path:
    d = Path(out_dir)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{signal.ticker.lower()}_governance.json"
    path.write_text(json.dumps(signal.__dict__, indent=2), encoding="utf-8")
    logger.info(f"[{signal.ticker}] Saved → {path}")
    return path


def print_summary_table(results: Dict[str, GovernanceSignal]):
    if not results:
        return
    print(f"\n{'=' * 60}")
    print(f"  {'Ticker':<8} {'Score':>6} {'Indep%':>7} {'AI Exp':>7} {'Conf':>6}")
    print(f"  {'-'*8} {'-'*6} {'-'*7} {'-'*7} {'-'*6}")
    for t, s in sorted(results.items(), key=lambda x: x[1].governance_score, reverse=True):
        print(
            f"  {t:<8} {s.governance_score:>5.0f}  "
            f"{s.independent_ratio*100:>5.1f}%  "
            f"{s.tech_expertise_count:>5}   "
            f"{s.confidence:>5.2f}"
        )
    print(f"{'=' * 60}")


# ────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────

DEFAULT_5 = ["NVDA", "JPM", "WMT", "GE", "DG"]

def main():
    parser = argparse.ArgumentParser(description="CS3 Board Composition Analyzer")
    parser.add_argument("tickers", nargs="*", help="Tickers to analyze (e.g., NVDA JPM WMT)")
    parser.add_argument("--all", action="store_true", help="Analyze all registered companies")
    parser.add_argument("--no-cache", action="store_true", help="Bypass local cache")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between companies (seconds)")
    args = parser.parse_args()

    if args.all:
        tickers = CompanyRegistry.all_tickers()
    elif args.tickers:
        tickers = [t.upper() for t in args.tickers]
    else:
        # Per your request: do all 5 tickers once by default
        tickers = DEFAULT_5

    analyzer = BoardCompositionAnalyzer()
    results = analyzer.analyze_multiple(tickers, use_cache=not args.no_cache, delay=args.delay)

    print("\n\n" + "#" * 60)
    print(f"#  GOVERNANCE ANALYSIS — {len(results)} companies")
    print("#" * 60)

    for _, signal in results.items():
        print_signal(signal)
        save_signal(signal)

    if len(results) > 1:
        print_summary_table(results)


if __name__ == "__main__":
    main()