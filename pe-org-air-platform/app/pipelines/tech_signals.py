# """
# Tech Stack Signal Analysis — Digital Presence
# app/pipelines/tech_signals.py

# Collects ACTUAL technology stack data from company websites using:
#   1. BuiltWith Free API  — technology group counts & categories
#   2. Wappalyzer (python-Wappalyzer) — specific technology names

# This is the Digital Presence signal source for CS2/CS3.
# It answers: "What technologies does this company actually run?"

# NOTE: This is SEPARATE from job_signals.py which answers
#       "Who are they hiring?" (technology_hiring signal).
# """
# from __future__ import annotations

# import asyncio
# import hashlib
# import httpx
# import json
# import logging
# from dataclasses import dataclass, field
# from datetime import datetime, timezone
# from typing import Any, Dict, List, Optional, Set

# from app.config import settings, COMPANY_NAME_MAPPINGS

# logger = logging.getLogger(__name__)


# # ---------------------------------------------------------------------------
# # Data Models
# # ---------------------------------------------------------------------------

# @dataclass
# class TechnologyDetection:
#     """A detected technology from website scanning."""
#     name: str
#     category: str
#     source: str          # "builtwith" or "wappalyzer"
#     is_ai_related: bool
#     confidence: float


# @dataclass
# class TechStackResult:
#     """Complete tech stack analysis for a company."""
#     company_id: str
#     ticker: str
#     domain: str

#     # Raw detections
#     technologies: List[TechnologyDetection] = field(default_factory=list)

#     # BuiltWith data
#     builtwith_groups: List[Dict[str, Any]] = field(default_factory=list)
#     builtwith_total_live: int = 0
#     builtwith_total_categories: int = 0

#     # Wappalyzer data
#     wappalyzer_techs: Dict[str, List[str]] = field(default_factory=dict)

#     # Scores
#     score: float = 0.0
#     ai_tools_score: float = 0.0
#     infra_score: float = 0.0
#     breadth_score: float = 0.0
#     confidence: float = 0.5

#     # Metadata
#     collected_at: str = ""
#     errors: List[str] = field(default_factory=list)


# # ---------------------------------------------------------------------------
# # AI Technology Classification
# # ---------------------------------------------------------------------------

# AI_SPECIFIC_TECHNOLOGIES = {
#     # Cloud ML platforms
#     "amazon sagemaker", "aws sagemaker", "sagemaker",
#     "azure machine learning", "azure ml",
#     "google vertex ai", "vertex ai",
#     "databricks", "databricks ml",
#     "amazon bedrock", "bedrock",
#     # ML frameworks
#     "tensorflow", "tensorflow.js", "pytorch", "keras",
#     "scikit-learn", "sklearn",
#     # AI APIs / providers
#     "openai", "anthropic", "hugging face", "huggingface",
#     "cohere", "replicate",
#     # MLOps
#     "mlflow", "kubeflow", "ray", "seldon",
#     "bentoml", "weights & biases", "wandb",
#     # Vector DBs
#     "pinecone", "weaviate", "milvus", "qdrant", "chroma",
#     # LLM tooling
#     "langchain", "llamaindex",
# }

# AI_INFRASTRUCTURE = {
#     # Compute / orchestration
#     "kubernetes", "k8s", "docker",
#     "apache spark", "spark", "pyspark",
#     "apache kafka", "kafka",
#     "apache airflow", "airflow",
#     "apache flink", "flink",
#     # Data platforms
#     "snowflake", "bigquery", "redshift", "clickhouse",
#     "dbt", "fivetran", "airbyte",
#     "elasticsearch", "opensearch",
#     # GPU / HPC
#     "nvidia", "cuda",
#     # Monitoring
#     "grafana", "prometheus", "datadog",
#     "new relic", "splunk",
# }

# # (BuiltWith free API only gives group-level counts, not specific tech names,
# #  so AI detection comes only from Wappalyzer specific tech matches)


# # ---------------------------------------------------------------------------
# # BuiltWith Free API Client
# # ---------------------------------------------------------------------------

# class BuiltWithClient:
#     """Client for BuiltWith Free API."""

#     BASE_URL = "https://api.builtwith.com/free1/api.json"

#     def __init__(self, api_key: Optional[str] = None):
#         self.api_key = api_key or getattr(settings, "BUILTWITH_API_KEY", None)
#         self._enabled = bool(self.api_key)

#     @property
#     def is_enabled(self) -> bool:
#         return self._enabled

#     async def lookup_domain(self, domain: str) -> Optional[Dict[str, Any]]:
#         """
#         Look up a domain using BuiltWith Free API.

#         Returns raw JSON response with technology group counts.
#         Rate limit: 1 request per second.
#         """
#         if not self._enabled:
#             logger.warning("BuiltWith API key not configured — skipping")
#             return None

#         try:
#             async with httpx.AsyncClient(timeout=30.0) as client:
#                 resp = await client.get(
#                     self.BASE_URL,
#                     params={"KEY": self.api_key, "LOOKUP": domain},
#                 )
#                 resp.raise_for_status()
#                 data = resp.json()

#                 # Free API returns groups with live/dead counts
#                 if "groups" not in data and "Errors" in data:
#                     logger.error(f"BuiltWith error for {domain}: {data['Errors']}")
#                     return None

#                 return data

#         except httpx.HTTPStatusError as e:
#             logger.error(f"BuiltWith HTTP error for {domain}: {e.response.status_code}")
#             return None
#         except Exception as e:
#             logger.error(f"BuiltWith request failed for {domain}: {e}")
#             return None


# # ---------------------------------------------------------------------------
# # Wappalyzer Client (python-Wappalyzer open-source library)
# # ---------------------------------------------------------------------------

# class WappalyzerClient:
#     """Client using python-Wappalyzer for real-time website tech detection.

#     FIX (v2): Uses requests with configurable timeout + retry instead of
#     WebPage.new_from_url which has a hardcoded 10s timeout. This fixes
#     the CAT (caterpillar.com) timeout issue where Wappalyzer returned
#     nothing and confidence dropped to 0.70.
#     """

#     DEFAULT_TIMEOUT = 20       # seconds (was 10 via new_from_url)
#     DEFAULT_RETRIES = 2
#     USER_AGENT = (
#         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#         "AppleWebKit/537.36 (KHTML, like Gecko) "
#         "Chrome/120.0.0.0 Safari/537.36"
#     )

#     def __init__(self):
#         self._available = False
#         self._wappalyzer_cls = None
#         self._webpage_cls = None
#         try:
#             import importlib
#             import warnings
#             with warnings.catch_warnings():
#                 warnings.simplefilter("ignore", DeprecationWarning)
#                 if importlib.util.find_spec("pkg_resources") is None:
#                     import setuptools  # noqa: F401
#                 import pkg_resources  # noqa: F401
#                 from Wappalyzer import Wappalyzer, WebPage
#             self._wappalyzer_cls = Wappalyzer
#             self._webpage_cls = WebPage
#             self._available = True
#             logger.info("✅ Wappalyzer loaded successfully")
#         except Exception as e:
#             logger.warning(
#                 f"python-Wappalyzer not available: {e}. "
#                 "Run: pip install python-Wappalyzer && pip install 'setuptools<81'"
#             )

#     @property
#     def is_available(self) -> bool:
#         return self._available

#     def analyze_url(
#         self,
#         url: str,
#         timeout: int = None,
#         retries: int = None,
#     ) -> Dict[str, List[str]]:
#         """
#         Analyze a URL and return detected technologies with categories.

#         Uses requests library with configurable timeout and retry logic
#         instead of WebPage.new_from_url (which has a hardcoded 10s timeout
#         that causes failures on slow corporate sites like caterpillar.com).

#         Args:
#             url: Full URL to scan (e.g. "https://www.caterpillar.com")
#             timeout: Request timeout in seconds (default 20)
#             retries: Number of retry attempts on timeout (default 2)

#         Returns:
#             Dict like {"React": ["JavaScript frameworks"], "Node.js": ["Web servers"]}
#         """
#         if not self._available:
#             return {}

#         import requests as req_lib

#         timeout = timeout or self.DEFAULT_TIMEOUT
#         retries = retries or self.DEFAULT_RETRIES

#         for attempt in range(1, retries + 1):
#             try:
#                 wappalyzer = self._wappalyzer_cls.latest()

#                 # Fetch page with explicit timeout + realistic User-Agent
#                 try:
#                     response = req_lib.get(
#                         url,
#                         timeout=timeout,
#                         headers={"User-Agent": self.USER_AGENT},
#                         allow_redirects=True,
#                         verify=True,
#                     )
#                     webpage = self._webpage_cls.new_from_response(response)

#                 except req_lib.exceptions.Timeout:
#                     if attempt < retries:
#                         logger.warning(
#                             f"Wappalyzer timeout for {url} "
#                             f"(attempt {attempt}/{retries}), retrying with "
#                             f"timeout={timeout + 5}s..."
#                         )
#                         timeout += 5  # increase timeout on retry
#                         continue
#                     logger.error(
#                         f"Wappalyzer timed out for {url} after {retries} attempts"
#                     )
#                     return {}

#                 except req_lib.exceptions.ConnectionError as e:
#                     if attempt < retries:
#                         logger.warning(
#                             f"Wappalyzer connection error for {url} "
#                             f"(attempt {attempt}/{retries}): {e}, retrying..."
#                         )
#                         continue
#                     logger.error(f"Wappalyzer connection failed for {url}: {e}")
#                     return {}

#                 except req_lib.exceptions.RequestException as e:
#                     logger.error(f"Wappalyzer request failed for {url}: {e}")
#                     return {}

#                 # Analyze technologies
#                 results = wappalyzer.analyze_with_categories(webpage)

#                 # Flatten: {tech_name: {categories: [...]}} -> {tech_name: [cat_names]}
#                 tech_categories = {}
#                 for tech_name, info in results.items():
#                     cats = info.get("categories", [])
#                     if isinstance(cats, list):
#                         tech_categories[tech_name] = cats
#                     elif isinstance(cats, dict):
#                         tech_categories[tech_name] = list(cats.values())
#                     else:
#                         tech_categories[tech_name] = [str(cats)]

#                 return tech_categories

#             except Exception as e:
#                 if attempt < retries:
#                     logger.warning(
#                         f"Wappalyzer error for {url} "
#                         f"(attempt {attempt}/{retries}): {e}, retrying..."
#                     )
#                     continue
#                 logger.error(f"Wappalyzer analysis failed for {url}: {e}")
#                 return {}
#     # def analyze_url(self, url: str) -> Dict[str, List[str]]:
#     #     """
#     #     Analyze a URL and return detected technologies with categories.

#     #     Returns:
#     #         Dict like {"React": ["JavaScript frameworks"], "Node.js": ["Web servers"], ...}
#     #     """
#     #     if not self._available:
#     #         return {}

#     #     try:
#     #         wappalyzer = self._wappalyzer_cls.latest()
#     #         webpage = self._webpage_cls.new_from_url(url)
#     #         results = wappalyzer.analyze_with_categories(webpage)

#     #         # Flatten: {tech_name: {categories: [...]}} -> {tech_name: [cat_names]}
#     #         tech_categories = {}
#     #         for tech_name, info in results.items():
#     #             cats = info.get("categories", [])
#     #             if isinstance(cats, list):
#     #                 tech_categories[tech_name] = cats
#     #             elif isinstance(cats, dict):
#     #                 tech_categories[tech_name] = list(cats.values())
#     #             else:
#     #                 tech_categories[tech_name] = [str(cats)]

#     #         return tech_categories

#     #     except Exception as e:
#     #         logger.error(f"Wappalyzer analysis failed for {url}: {e}")
#     #         return {}


# # ---------------------------------------------------------------------------
# # Main Collector
# # ---------------------------------------------------------------------------

# class TechStackCollector:
#     """
#     Collect digital presence signals from company websites.

#     Uses BuiltWith (breadth) + Wappalyzer (specific tech names)
#     to score a company's technology sophistication for the
#     digital_presence signal category.
#     """

#     def __init__(self):
#         self.builtwith = BuiltWithClient()
#         self.wappalyzer = WappalyzerClient()

#     async def analyze_company(
#         self,
#         company_id: str,
#         ticker: str,
#         domain: Optional[str] = None,
#         company_name: Optional[str] = None,
#     ) -> TechStackResult:
#         """
#         Full tech stack analysis for a single company.

#         Args:
#             company_id: Company UUID
#             ticker: Stock ticker
#             domain: Company website domain (auto-resolved from config if None)
#             company_name: Official company name (used to derive domain if not in config)

#         Returns:
#             TechStackResult with scores and detections
#         """
#         # Resolve domain
#         if not domain:
#             mapping = COMPANY_NAME_MAPPINGS.get(ticker.upper(), {})
#             domain = mapping.get("domain")
#         if not domain and company_name:
#             # Derive a best-guess domain from company name
#             import re as _re
#             # Strip common legal suffixes
#             clean = _re.sub(
#                 r",?\s*(Inc\.?|Corp\.?|LLC\.?|Ltd\.?|Co\.?|Holdings?|Group|PLC|N\.A\.?|S\.A\.?|AG)\.?\s*$",
#                 "", company_name, flags=_re.IGNORECASE,
#             ).strip()
#             # Keep only alphanumeric chars, lowercase
#             clean = _re.sub(r"[^a-zA-Z0-9]", "", clean).lower()
#             if clean:
#                 domain = f"{clean}.com"
#                 logger.info(f"No domain configured for {ticker}; derived fallback: {domain}")
#         if not domain:
#             logger.error(f"No domain configured for {ticker}")
#             return TechStackResult(
#                 company_id=company_id, ticker=ticker, domain="unknown",
#                 errors=[f"No domain configured for {ticker}"],
#                 collected_at=datetime.now(timezone.utc).isoformat(),
#             )

#         logger.info(f"🌐 Analyzing tech stack for {ticker} ({domain})")

#         result = TechStackResult(
#             company_id=company_id,
#             ticker=ticker,
#             domain=domain,
#             collected_at=datetime.now(timezone.utc).isoformat(),
#         )

#         # --- Source 1: BuiltWith Free API ---
#         if self.builtwith.is_enabled:
#             logger.info(f"  📡 Querying BuiltWith for {domain}...")
#             bw_data = await self.builtwith.lookup_domain(domain)
#             if bw_data:
#                 self._process_builtwith(result, bw_data)
#             else:
#                 result.errors.append("BuiltWith lookup returned no data")
#             # Respect rate limit
#             await asyncio.sleep(1.1)
#         else:
#             result.errors.append("BuiltWith API key not configured")

#         # --- Source 2: Wappalyzer ---
#         if self.wappalyzer.is_available:
#             logger.info(f"  🔍 Scanning {domain} with Wappalyzer...")
#             url = f"https://www.{domain}"
#             tech_cats = self.wappalyzer.analyze_url(url)
#             if tech_cats:
#                 self._process_wappalyzer(result, tech_cats)
#             else:
#                 result.errors.append("Wappalyzer returned no technologies")
#         else:
#             result.errors.append("python-Wappalyzer not installed")

#         # --- Score ---
#         self._calculate_score(result)

#         logger.info(
#             f"  ✅ {ticker}: {result.score:.1f}/100 "
#             f"(ai_tools={result.ai_tools_score:.0f}, "
#             f"infra={result.infra_score:.0f}, "
#             f"breadth={result.breadth_score:.0f}) "
#             f"| {len(result.technologies)} techs detected"
#         )

#         return result

#     # ------------------------------------------------------------------
#     # Processing helpers
#     # ------------------------------------------------------------------

#     def _process_builtwith(self, result: TechStackResult, data: Dict) -> None:
#         """Extract technology info from BuiltWith Free API response."""
#         groups = data.get("groups", [])
#         result.builtwith_groups = groups

#         total_live = 0
#         total_categories = 0

#         for group in groups:
#             name = group.get("name", "").lower()
#             live = group.get("live", 0)
#             total_live += live

#             categories = group.get("categories", [])
#             total_categories += len(categories)

#             # Create a detection for each BuiltWith group
#             # BuiltWith free API only gives group names, not specific tools
#             # So we never flag these as AI — only Wappalyzer detects specific AI tools
#             result.technologies.append(
#                 TechnologyDetection(
#                     name=f"bw:{name}",
#                     category=name,
#                     source="builtwith",
#                     is_ai_related=False,
#                     confidence=0.85,
#                 )
#             )

#         result.builtwith_total_live = total_live
#         result.builtwith_total_categories = total_categories

#     def _process_wappalyzer(
#         self, result: TechStackResult, tech_cats: Dict[str, List[str]]
#     ) -> None:
#         """Process Wappalyzer detections."""
#         result.wappalyzer_techs = tech_cats

#         for tech_name, categories in tech_cats.items():
#             tech_lower = tech_name.lower()
#             is_ai = (
#                 tech_lower in AI_SPECIFIC_TECHNOLOGIES
#                 or tech_lower in AI_INFRASTRUCTURE
#             )

#             result.technologies.append(
#                 TechnologyDetection(
#                     name=tech_name,
#                     category=categories[0] if categories else "unknown",
#                     source="wappalyzer",
#                     is_ai_related=is_ai,
#                     confidence=0.90,
#                 )
#             )

#     # ------------------------------------------------------------------
#     # Scoring
#     # ------------------------------------------------------------------

#     def _calculate_score(self, result: TechStackResult) -> None:
#         """
#         Calculate digital presence score (0-100).
#         Uses BuiltWith live tech counts as primary differentiator.
#         """
#         import math

#         # --- Wappalyzer detections ---
#         wappalyzer_names = {
#             t.name.lower() for t in result.technologies if t.source == "wappalyzer"
#         }
#         ai_tools_found = wappalyzer_names & AI_SPECIFIC_TECHNOLOGIES
#         infra_found = wappalyzer_names & AI_INFRASTRUCTURE

#         # --- BuiltWith analysis ---
#         bw_group_names = {
#             g.get("name", "").lower() for g in result.builtwith_groups
#         }
#         bw_total_live = result.builtwith_total_live
#         bw_total_dead = sum(g.get("dead", 0) for g in result.builtwith_groups)
#         bw_total_categories = result.builtwith_total_categories
#         bw_group_count = len(result.builtwith_groups)

#         # Key infra groups
#         key_infra_groups = {"cdn", "cdns", "ssl", "analytics", "mx",
#                            "payment", "shop", "cms", "mobile", "mapping"}
#         key_groups_found = bw_group_names & key_infra_groups

#         # === Component 1: Technology Sophistication (max 40) ===
#         if bw_total_live > 0:
#             log_live = math.log2(bw_total_live + 1)
#             live_score = min(log_live * 2.1, 25)
#         else:
#             live_score = 0
#         wp_ai_score = min(len(ai_tools_found) * 5, 15)
#         result.ai_tools_score = round(live_score + wp_ai_score, 1)

#         # === Component 2: Infrastructure Maturity (max 30) ===
#         group_score = min(bw_group_count * 0.6, 15)
#         key_infra_score = min(len(key_groups_found) * 1.5, 15)
#         wp_infra = min(len(infra_found) * 2, 5)
#         result.infra_score = round(min(group_score + key_infra_score + wp_infra, 30), 1)

#         # === Component 3: Technology Breadth (max 30) ===
#         cat_score = min(bw_total_categories * 0.15, 15)
#         total_all = bw_total_live + bw_total_dead
#         if total_all > 0:
#             active_ratio = bw_total_live / total_all
#             maintenance_score = active_ratio * 15
#         else:
#             maintenance_score = 0
#         result.breadth_score = round(min(cat_score + maintenance_score, 30), 1)

#         # === Total ===
#         result.score = round(
#             result.ai_tools_score + result.infra_score + result.breadth_score, 1
#         )
#         result.score = min(result.score, 100.0)

#         # Confidence
#         sources_active = sum([
#             bool(result.builtwith_groups),
#             bool(result.wappalyzer_techs),
#         ])
#         if sources_active == 2:
#             result.confidence = 0.90
#         elif sources_active == 1:
#             result.confidence = 0.70
#         else:
#             result.confidence = 0.40

#     # ------------------------------------------------------------------
#     # Bulk analysis
#     # ------------------------------------------------------------------

#     async def analyze_companies(
#         self,
#         companies: List[Dict[str, Any]],
#     ) -> Dict[str, TechStackResult]:
#         """Analyze tech stacks for multiple companies."""
#         results = {}
#         for company in companies:
#             cid = company.get("id", "")
#             ticker = company.get("ticker", "")
#             company_name = company.get("name")
#             try:
#                 result = await self.analyze_company(cid, ticker, company_name=company_name)
#                 results[cid] = result
#             except Exception as e:
#                 logger.error(f"Failed to analyze {ticker}: {e}")
#                 results[cid] = TechStackResult(
#                     company_id=cid, ticker=ticker, domain="unknown",
#                     errors=[str(e)],
#                     collected_at=datetime.now(timezone.utc).isoformat(),
#                 )
#         return results

#     # ------------------------------------------------------------------
#     # Serialization (for S3 storage)
#     # ------------------------------------------------------------------

#     @staticmethod
#     def result_to_dict(r: TechStackResult) -> Dict[str, Any]:
#         """Convert TechStackResult to JSON-serializable dict."""
#         return {
#             "company_id": r.company_id,
#             "ticker": r.ticker,
#             "domain": r.domain,
#             "score": r.score,
#             "ai_tools_score": r.ai_tools_score,
#             "infra_score": r.infra_score,
#             "breadth_score": r.breadth_score,
#             "confidence": r.confidence,
#             "builtwith_total_live": r.builtwith_total_live,
#             "builtwith_total_categories": r.builtwith_total_categories,
#             "wappalyzer_techs": {
#                 k: v for k, v in r.wappalyzer_techs.items()
#             },
#             "ai_technologies_detected": [
#                 t.name for t in r.technologies if t.is_ai_related
#             ],
#             "all_technologies": [
#                 {"name": t.name, "category": t.category,
#                  "source": t.source, "is_ai_related": t.is_ai_related}
#                 for t in r.technologies
#             ],
#             "collected_at": r.collected_at,
#             "errors": r.errors,
#         }

"""
Tech Stack Signal Analysis — Digital Presence
app/pipelines/tech_signals.py

Collects ACTUAL technology stack data from company websites using:
  1. BuiltWith Free API  — technology group counts & categories
  2. Wappalyzer (python-Wappalyzer) — specific technology names
  3. Groq Subdomain Discovery — when primary domain is blocked, Groq returns a
     list of real technical subdomains (e.g. cloud.google.com) which are then
     scanned by BuiltWith and Wappalyzer for REAL evidence.
  4. LLM Score Fallback (last resort) — only if ALL discovered subdomains also
     fail scraping. Score is capped at confidence=0.30 and flagged `unverified`.

SUBDOMAIN DISCOVERY RATIONALE:
  Major tech companies (Google, Meta, Apple, etc.) actively block tech-fingerprinting
  on their primary homepage. BuiltWith returns Code -8 for google.com. But subdomains
  like cloud.google.com, firebase.google.com, developers.google.com are publicly
  scannable and return rich tech stacks — Kubernetes, gRPC, Go, BigQuery, etc.

  Groq's role is DOMAIN DISCOVERY ONLY — it never touches the score.
  All scores come from real BuiltWith + Wappalyzer scraper data.

SNOWFLAKE EVIDENCE STORAGE:
  Every scanned domain + every detected technology is persisted to Snowflake:
    - DIGITAL_PRESENCE_DOMAINS     — one row per scanned domain per run
    - DIGITAL_PRESENCE_TECHNOLOGIES — one row per detected tech per domain per run
  This makes digital_presence scores fully auditable and evidence-backed.

  Schema: app/database/digital_presence_schema.sql
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from app.config import settings, COMPANY_NAME_MAPPINGS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class TechnologyDetection:
    """A detected technology from website scanning."""
    name: str
    category: str
    source: str          # "builtwith", "wappalyzer", or "llm_knowledge"
    is_ai_related: bool
    confidence: float


@dataclass
class DomainScanResult:
    """Evidence from scanning a single domain or subdomain."""
    domain: str
    domain_type: str              # 'primary' | 'subdomain'
    discovery_source: str         # 'config' | 'groq_discovery' | 'hardcoded_fallback'
    technologies: List["TechnologyDetection"] = field(default_factory=list)
    builtwith_groups: List[Dict[str, Any]] = field(default_factory=list)
    builtwith_total_live: int = 0
    builtwith_total_categories: int = 0
    wappalyzer_techs: Dict[str, List[str]] = field(default_factory=dict)
    scraper_score: float = 0.0
    scan_success: bool = False
    scan_error: str = ""


@dataclass
class TechStackResult:
    """Complete tech stack analysis for a company."""
    company_id: str
    ticker: str
    domain: str

    # Raw detections (aggregated across all scanned domains)
    technologies: List[TechnologyDetection] = field(default_factory=list)

    # Per-domain scan evidence (NEW — stored to Snowflake)
    domain_scans: List[DomainScanResult] = field(default_factory=list)
    subdomains_discovered: List[str] = field(default_factory=list)   # from Groq
    subdomains_successful: List[str] = field(default_factory=list)   # actually returned data

    # BuiltWith data (aggregated)
    builtwith_groups: List[Dict[str, Any]] = field(default_factory=list)
    builtwith_total_live: int = 0
    builtwith_total_categories: int = 0

    # Wappalyzer data (aggregated)
    wappalyzer_techs: Dict[str, List[str]] = field(default_factory=dict)

    # Scores
    score: float = 0.0
    ai_tools_score: float = 0.0
    infra_score: float = 0.0
    breadth_score: float = 0.0
    confidence: float = 0.5

    # Metadata
    collected_at: str = ""
    errors: List[str] = field(default_factory=list)

    # LLM fallback tracking — only True if ALL subdomains also failed scraping
    llm_fallback_used: bool = False
    llm_fallback_reasoning: str = ""
    evidence_source: str = "scraper"   # 'scraper' | 'subdomain_scraper' | 'llm_unverified'


# ---------------------------------------------------------------------------
# AI Technology Classification
# ---------------------------------------------------------------------------

AI_SPECIFIC_TECHNOLOGIES = {
    # Cloud ML platforms
    "amazon sagemaker", "aws sagemaker", "sagemaker",
    "azure machine learning", "azure ml",
    "google vertex ai", "vertex ai",
    "databricks", "databricks ml",
    "amazon bedrock", "bedrock",
    # ML frameworks
    "tensorflow", "tensorflow.js", "pytorch", "keras",
    "scikit-learn", "sklearn",
    # AI APIs / providers
    "openai", "anthropic", "hugging face", "huggingface",
    "cohere", "replicate",
    # MLOps
    "mlflow", "kubeflow", "ray", "seldon",
    "bentoml", "weights & biases", "wandb",
    # Vector DBs
    "pinecone", "weaviate", "milvus", "qdrant", "chroma",
    # LLM tooling
    "langchain", "llamaindex",
}

AI_INFRASTRUCTURE = {
    # Compute / orchestration
    "kubernetes", "k8s", "docker",
    "apache spark", "spark", "pyspark",
    "apache kafka", "kafka",
    "apache airflow", "airflow",
    "apache flink", "flink",
    # Data platforms
    "snowflake", "bigquery", "redshift", "clickhouse",
    "dbt", "fivetran", "airbyte",
    "elasticsearch", "opensearch",
    # GPU / HPC
    "nvidia", "cuda",
    # Monitoring
    "grafana", "prometheus", "datadog",
    "new relic", "splunk",
}

# Score below which subdomain discovery is triggered
LLM_FALLBACK_THRESHOLD = 10.0

# Score below which even after subdomain scanning, LLM score fallback triggers
# (last resort — marked as unverified)
LLM_SCORE_FALLBACK_THRESHOLD = 10.0


# ---------------------------------------------------------------------------
# Groq Subdomain Discovery
# ---------------------------------------------------------------------------

def _groq_discover_subdomains(ticker: str, company_name: str, primary_domain: str) -> List[str]:
    """
    Ask Groq to return real technical subdomains for a company whose primary
    domain blocks fingerprinting tools.

    Returns a list of subdomain strings to scan with BuiltWith + Wappalyzer.
    Groq's role is DISCOVERY ONLY — it never influences the score.

    Example output for GOOGL:
      ["cloud.google.com", "firebase.google.com", "developers.google.com",
       "ai.google.dev", "research.google"]
    """
    import os

    groq_api_key = os.getenv("GROQ_API_KEY", "")
    if not groq_api_key:
        logger.warning("GROQ_API_KEY not set — cannot discover subdomains")
        return []

    prompt = f"""The primary website "{primary_domain}" for company "{company_name}" (ticker: {ticker}) 
blocks web technology fingerprinting tools like BuiltWith and Wappalyzer.

List the real technical subdomains or sister domains for this company that:
1. Are publicly accessible (not behind auth)
2. Would reveal actual technology stack (developer portals, cloud consoles, API docs, product sites)
3. Can be scanned by BuiltWith or Wappalyzer

Return ONLY valid JSON — a list of domain strings, no markdown, no explanation:
["subdomain1.example.com", "subdomain2.example.com", ...]

Rules:
- Return 3-6 domains maximum
- Only include real, publicly known domains for {company_name}
- Do not include internal tools, login pages, or CDN edge nodes
- Prefer developer-facing or product-facing subdomains"""

    try:
        import httpx as _httpx
        _client = _httpx
    except ImportError:
        _client = None

    try:
        headers = {
            "Authorization": f"Bearer {groq_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "llama-3.1-8b-instant",
            "messages": [
                {"role": "system", "content": "You are a domain research assistant. Respond only with valid JSON arrays."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 200,
        }

        import requests as _req
        resp = _req.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers, json=payload, timeout=20.0,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()

        # Strip markdown fences
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            logger.warning(f"Groq subdomain discovery returned non-list for {ticker}: {type(parsed)}")
            return []

        # Sanitize: only return strings that look like domains
        domains = []
        for item in parsed:
            if isinstance(item, str) and "." in item and len(item) < 100:
                # Strip protocol if Groq included it
                clean = item.replace("https://", "").replace("http://", "").rstrip("/")
                domains.append(clean)

        logger.info(f"  🔍 Groq discovered {len(domains)} subdomains for {ticker}: {domains}")
        return domains[:6]   # Cap at 6 to avoid excessive API calls

    except json.JSONDecodeError as e:
        logger.error(f"Groq subdomain discovery JSON error for {ticker}: {e}")
        return []
    except Exception as e:
        logger.error(f"Groq subdomain discovery failed for {ticker}: {e}")
        return []


# ---------------------------------------------------------------------------
# BuiltWith Free API Client
# ---------------------------------------------------------------------------

class BuiltWithClient:
    """Client for BuiltWith Free API."""

    BASE_URL = "https://api.builtwith.com/free1/api.json"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or getattr(settings, "BUILTWITH_API_KEY", None)
        self._enabled = bool(self.api_key)

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    async def lookup_domain(self, domain: str) -> Optional[Dict[str, Any]]:
        """
        Look up a domain using BuiltWith Free API.
        Rate limit: 1 request per second.
        """
        if not self._enabled:
            logger.warning("BuiltWith API key not configured — skipping")
            return None

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(
                    self.BASE_URL,
                    params={"KEY": self.api_key, "LOOKUP": domain},
                )
                resp.raise_for_status()
                data = resp.json()

                if "groups" not in data and "Errors" in data:
                    logger.error(f"BuiltWith error for {domain}: {data['Errors']}")
                    return None

                return data

        except httpx.HTTPStatusError as e:
            logger.error(f"BuiltWith HTTP error for {domain}: {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"BuiltWith request failed for {domain}: {e}")
            return None


# ---------------------------------------------------------------------------
# Wappalyzer Client
# ---------------------------------------------------------------------------

class WappalyzerClient:
    """Client using python-Wappalyzer for real-time website tech detection.

    Uses requests with configurable timeout + retry instead of
    WebPage.new_from_url which has a hardcoded 10s timeout.
    """

    DEFAULT_TIMEOUT = 20
    DEFAULT_RETRIES = 2
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    def __init__(self):
        self._available = False
        self._wappalyzer_cls = None
        self._webpage_cls = None
        try:
            import importlib
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                if importlib.util.find_spec("pkg_resources") is None:
                    import setuptools  # noqa: F401
                import pkg_resources  # noqa: F401
                from Wappalyzer import Wappalyzer, WebPage
            self._wappalyzer_cls = Wappalyzer
            self._webpage_cls = WebPage
            self._available = True
            logger.info("✅ Wappalyzer loaded successfully")
        except Exception as e:
            logger.warning(
                f"python-Wappalyzer not available: {e}. "
                "Run: pip install python-Wappalyzer && pip install 'setuptools<81'"
            )

    @property
    def is_available(self) -> bool:
        return self._available

    def analyze_url(self, url: str, timeout: int = None, retries: int = None) -> Dict[str, List[str]]:
        """
        Analyze a URL and return detected technologies with categories.
        Uses requests library with configurable timeout and retry logic.
        """
        if not self._available:
            return {}

        import requests as req_lib

        timeout = timeout or self.DEFAULT_TIMEOUT
        retries = retries or self.DEFAULT_RETRIES

        for attempt in range(1, retries + 1):
            try:
                wappalyzer = self._wappalyzer_cls.latest()

                try:
                    response = req_lib.get(
                        url,
                        timeout=timeout,
                        headers={"User-Agent": self.USER_AGENT},
                        allow_redirects=True,
                        verify=True,
                    )
                    webpage = self._webpage_cls.new_from_response(response)

                except req_lib.exceptions.Timeout:
                    if attempt < retries:
                        logger.warning(
                            f"Wappalyzer timeout for {url} "
                            f"(attempt {attempt}/{retries}), retrying with "
                            f"timeout={timeout + 5}s..."
                        )
                        timeout += 5
                        continue
                    logger.error(f"Wappalyzer timed out for {url} after {retries} attempts")
                    return {}

                except req_lib.exceptions.ConnectionError as e:
                    if attempt < retries:
                        logger.warning(
                            f"Wappalyzer connection error for {url} "
                            f"(attempt {attempt}/{retries}): {e}, retrying..."
                        )
                        continue
                    logger.error(f"Wappalyzer connection failed for {url}: {e}")
                    return {}

                except req_lib.exceptions.RequestException as e:
                    logger.error(f"Wappalyzer request failed for {url}: {e}")
                    return {}

                results = wappalyzer.analyze_with_categories(webpage)

                tech_categories = {}
                for tech_name, info in results.items():
                    cats = info.get("categories", [])
                    if isinstance(cats, list):
                        tech_categories[tech_name] = cats
                    elif isinstance(cats, dict):
                        tech_categories[tech_name] = list(cats.values())
                    else:
                        tech_categories[tech_name] = [str(cats)]

                return tech_categories

            except Exception as e:
                if attempt < retries:
                    logger.warning(
                        f"Wappalyzer error for {url} "
                        f"(attempt {attempt}/{retries}): {e}, retrying..."
                    )
                    continue
                logger.error(f"Wappalyzer analysis failed for {url}: {e}")
                return {}

        return {}


# ---------------------------------------------------------------------------
# Groq LLM Fallback
# ---------------------------------------------------------------------------

# ===========================================================================
# TODO: SWITCH TO CLAUDE — when ready, do these 3 things:
#
#   1. In this file (tech_signals.py):
#      - Comment out _groq_tech_stack_score() below
#      - Uncomment _claude_tech_stack_score() below it
#      - In analyze_company(), swap the one call from:
#            _groq_tech_stack_score(ticker, company_name or ticker, domain)
#        to:
#            _claude_tech_stack_score(ticker, company_name or ticker, domain)
#
#   2. In app/services/llm/router.py:
#      - Uncomment "tech_stack_fallback" entry in _TASK_ROUTING (already added there)
#      - Uncomment "claude-sonnet-4-20250514" in _MODEL_CONFIGS
#      - Uncomment "claude-sonnet-4-20250514" in _MODEL_COST_PER_1K
#      - Comment out the TESTING _TASK_ROUTING block, uncomment PRODUCTION block
#
#   3. In .env:
#      - Add: ANTHROPIC_API_KEY=sk-ant-...
#
#   That's it. Everything else (scoring, S3 storage, result parsing) stays identical.
# ===========================================================================


def _groq_tech_stack_score(ticker: str, company_name: str, domain: str) -> Optional[Dict[str, Any]]:
    """
    [CURRENT — TESTING] Use Groq (llama-3.1-8b-instant) to synthesize a digital
    presence score when BuiltWith/Wappalyzer return near-zero results.

    Reuses the same _chat() pattern as groq_enrichment.py — direct httpx POST
    to Groq API. No new LLM dependencies or patterns introduced.

    Only called when scraper score < LLM_FALLBACK_THRESHOLD (10.0).
    Tagged as llm_fallback_used=True in result metadata for auditability.
    Confidence capped at 0.70 (lower than scraper-based 0.90).

    Returns a dict with score components, or None if Groq call fails.
    """
    import os

    groq_api_key = os.getenv("GROQ_API_KEY", "")
    if not groq_api_key:
        logger.warning("GROQ_API_KEY not set — cannot run LLM fallback for tech stack")
        return None

    prompt = f"""For the publicly-traded company "{company_name}" (ticker: {ticker}), website {domain},
web scraping tools returned near-zero results — this company likely blocks fingerprinting tools.

Using your knowledge of {company_name}'s publicly documented technology stack (engineering blogs,
conference talks, open-source contributions, SEC filings), provide a digital presence score.

Score these three components and respond ONLY with valid JSON, no markdown:

{{
  "score": <total 0-100, sum of the three components below>,
  "ai_tools_score": <0-40, AI/ML platforms and tools in active use>,
  "infra_score": <0-30, infrastructure maturity: cloud scale, data pipelines, orchestration>,
  "breadth_score": <0-30, technology breadth and diversity across the org>,
  "confidence": <0.50-0.70, your confidence based on public information only>,
  "reasoning": "<1-2 sentences explaining the score>",
  "known_technologies": ["<specific tech names this company is publicly known to use>"]
}}

Scoring guidance:
- Google/Microsoft/Meta with massive ML infra, custom chips, proprietary ML platforms: 80-95
- JPMorgan/Goldman with strong data infrastructure but less public AI tooling: 50-70
- Walmart/Target with growing but nascent tech investment: 35-55
- Only score what you have reasonable public evidence for
- known_technologies: list specific tools e.g. "TensorFlow", "Kubernetes", "BigQuery" """

    try:
        headers = {
            "Authorization": f"Bearer {groq_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "llama-3.1-8b-instant",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a technology analyst. Respond only with valid JSON.",
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
            "max_tokens": 600,
        }

        response = httpx.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30.0,
        )
        response.raise_for_status()
        raw = response.json()["choices"][0]["message"]["content"].strip()

        # Strip markdown fences if Groq adds them despite instructions
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        parsed = json.loads(raw)

        return {
            "score":          round(min(max(float(parsed.get("score", 0)), 0), 100), 1),
            "ai_tools_score": round(min(max(float(parsed.get("ai_tools_score", 0)), 0), 40), 1),
            "infra_score":    round(min(max(float(parsed.get("infra_score", 0)), 0), 30), 1),
            "breadth_score":  round(min(max(float(parsed.get("breadth_score", 0)), 0), 30), 1),
            "confidence":     round(min(max(float(parsed.get("confidence", 0.60)), 0.50), 0.70), 2),
            "reasoning":      str(parsed.get("reasoning", "")),
            "known_technologies": [str(t) for t in parsed.get("known_technologies", [])],
        }

    except json.JSONDecodeError as e:
        logger.error(f"Groq LLM fallback JSON parse error for {ticker}: {e} | raw: {raw[:200]}")
        return None
    except httpx.HTTPStatusError as e:
        logger.error(f"Groq LLM fallback HTTP error for {ticker}: {e.response.status_code}")
        return None
    except Exception as e:
        logger.error(f"Groq LLM fallback failed for {ticker}: {e}")
        return None


# ===========================================================================
# TODO: SWITCH TO CLAUDE — uncomment this entire function when switching.
#       Then in analyze_company() swap _groq_tech_stack_score → _claude_tech_stack_score
#       See the full checklist in the TODO block above _groq_tech_stack_score().
# ===========================================================================

# def _claude_tech_stack_score(ticker: str, company_name: str, domain: str) -> Optional[Dict[str, Any]]:
#     """
#     [PRODUCTION] Use Claude Sonnet via ModelRouter to synthesize a digital
#     presence score when BuiltWith/Wappalyzer return near-zero results.
#
#     Uses the existing ModelRouter with task="tech_stack_fallback" which routes
#     to claude-sonnet-4-20250514 (primary) → groq/llama-3.1-8b-instant (fallback).
#     Requires ANTHROPIC_API_KEY in .env and router.py production block uncommented.
#
#     Claude Sonnet is preferred over Groq for this task because:
#       - Stronger factual recall on public company tech stacks
#       - More reliable structured JSON output (fewer parse errors)
#       - Better calibrated confidence scores
#     """
#     from app.services.llm.router import ModelRouter
#
#     prompt = f"""For the publicly-traded company "{company_name}" (ticker: {ticker}), website {domain},
# web scraping tools returned near-zero results — this company likely blocks fingerprinting tools.
#
# Using your knowledge of {company_name}'s publicly documented technology stack (engineering blogs,
# conference talks, open-source contributions, SEC filings), provide a digital presence score.
#
# Score these three components and respond ONLY with valid JSON, no markdown:
#
# {{
#   "score": <total 0-100, sum of the three components below>,
#   "ai_tools_score": <0-40, AI/ML platforms and tools in active use>,
#   "infra_score": <0-30, infrastructure maturity: cloud scale, data pipelines, orchestration>,
#   "breadth_score": <0-30, technology breadth and diversity across the org>,
#   "confidence": <0.50-0.80, your confidence based on public information only>,
#   "reasoning": "<1-2 sentences explaining the score>",
#   "known_technologies": ["<specific tech names this company is publicly known to use>"]
# }}
#
# Scoring guidance:
# - Google/Microsoft/Meta with massive ML infra, custom chips, proprietary ML platforms: 80-95
# - JPMorgan/Goldman with strong data infrastructure but less public AI tooling: 50-70
# - Walmart/Target with growing but nascent tech investment: 35-55
# - Only score what you have reasonable public evidence for
# - known_technologies: list specific tools e.g. "TensorFlow", "Kubernetes", "BigQuery" """
#
#     try:
#         router = ModelRouter()
#         raw = router.complete(
#             task="tech_stack_fallback",
#             messages=[
#                 {"role": "system", "content": "You are a technology analyst. Respond only with valid JSON."},
#                 {"role": "user", "content": prompt},
#             ],
#         )
#
#         # Strip markdown fences if present
#         raw = raw.strip()
#         if raw.startswith("```"):
#             raw = raw.split("```")[1]
#             if raw.startswith("json"):
#                 raw = raw[4:]
#         raw = raw.strip()
#
#         parsed = json.loads(raw)
#
#         return {
#             "score":          round(min(max(float(parsed.get("score", 0)), 0), 100), 1),
#             "ai_tools_score": round(min(max(float(parsed.get("ai_tools_score", 0)), 0), 40), 1),
#             "infra_score":    round(min(max(float(parsed.get("infra_score", 0)), 0), 30), 1),
#             "breadth_score":  round(min(max(float(parsed.get("breadth_score", 0)), 0), 30), 1),
#             "confidence":     round(min(max(float(parsed.get("confidence", 0.65)), 0.50), 0.80), 2),
#             "reasoning":      str(parsed.get("reasoning", "")),
#             "known_technologies": [str(t) for t in parsed.get("known_technologies", [])],
#         }
#
#     except json.JSONDecodeError as e:
#         logger.error(f"Claude LLM fallback JSON parse error for {ticker}: {e} | raw: {raw[:200]}")
#         return None
#     except Exception as e:
#         logger.error(f"Claude LLM fallback failed for {ticker}: {e}")
#         return None


# ---------------------------------------------------------------------------
# Main Collector
# ---------------------------------------------------------------------------

class TechStackCollector:
    """
    Collect digital presence signals from company websites.

    Pipeline:
      1. BuiltWith Free API — technology breadth (group counts)
      2. Wappalyzer — specific technology names (AI detection)
      3. Groq LLM fallback — triggered when scraper score < 10.0
         (handles companies that block fingerprinting: Google, Meta, Apple, etc.)
    """

    def __init__(self):
        self.builtwith = BuiltWithClient()
        self.wappalyzer = WappalyzerClient()

    async def analyze_company(
        self,
        company_id: str,
        ticker: str,
        domain: Optional[str] = None,
        company_name: Optional[str] = None,
    ) -> TechStackResult:
        """
        Full tech stack analysis for a single company.

        Flow:
          1. Scan primary domain with BuiltWith + Wappalyzer
          2. If score < threshold → ask Groq to discover technical subdomains
          3. Scan each discovered subdomain with BuiltWith + Wappalyzer
          4. Aggregate all real scraper evidence → compute final score
          5. Store every domain scan + technology to Snowflake for auditability
          6. Only if ALL scans fail → LLM score fallback (marked 'unverified')
        """
        # -- Resolve domain ------------------------------------------------
        if not domain:
            mapping = COMPANY_NAME_MAPPINGS.get(ticker.upper(), {})
            domain = mapping.get("domain")

        if not domain and company_name:
            import re as _re
            clean = _re.sub(
                r",?\s*(Inc\.?|Corp\.?|LLC\.?|Ltd\.?|Co\.?|Holdings?|Group|PLC|N\.A\.?|S\.A\.?|AG)\.?\s*$",
                "", company_name, flags=_re.IGNORECASE,
            ).strip()
            clean = _re.sub(r"[^a-zA-Z0-9]", "", clean).lower()
            if clean:
                domain = f"{clean}.com"
                logger.info(f"No domain configured for {ticker}; derived fallback: {domain}")

        if not domain:
            logger.error(f"No domain configured for {ticker}")
            return TechStackResult(
                company_id=company_id, ticker=ticker, domain="unknown",
                errors=[f"No domain configured for {ticker}"],
                collected_at=datetime.now(timezone.utc).isoformat(),
            )

        logger.info(f"🌐 Analyzing tech stack for {ticker} ({domain})")

        result = TechStackResult(
            company_id=company_id,
            ticker=ticker,
            domain=domain,
            collected_at=datetime.now(timezone.utc).isoformat(),
        )

        # -- Step 1: Scan primary domain ------------------------------------
        primary_scan = await self._scan_single_domain(
            domain=domain,
            domain_type="primary",
            discovery_source="config",
        )
        result.domain_scans.append(primary_scan)
        self._merge_scan_into_result(result, primary_scan)
        self._calculate_score(result)

        logger.info(
            f"  📡 Primary domain ({domain}) score: {result.score:.1f} "
            f"| techs: {len(result.technologies)}"
        )

        # -- Step 2: If primary fails, discover + scan subdomains ----------
        if result.score < LLM_FALLBACK_THRESHOLD:
            logger.info(
                f"  🔍 Primary score {result.score:.1f} < {LLM_FALLBACK_THRESHOLD} "
                f"for {ticker} — querying Groq for technical subdomains..."
            )

            discovered = await asyncio.to_thread(
                _groq_discover_subdomains, ticker, company_name or ticker, domain
            )
            result.subdomains_discovered = discovered

            if discovered:
                logger.info(f"  🌐 Scanning {len(discovered)} subdomains for {ticker}...")
                for subdomain in discovered:
                    await asyncio.sleep(1.1)   # Respect BuiltWith rate limit
                    sub_scan = await self._scan_single_domain(
                        domain=subdomain,
                        domain_type="subdomain",
                        discovery_source="groq_discovery",
                    )
                    result.domain_scans.append(sub_scan)
                    if sub_scan.scan_success:
                        result.subdomains_successful.append(subdomain)
                        self._merge_scan_into_result(result, sub_scan)
                        logger.info(
                            f"    ✅ {subdomain}: {sub_scan.builtwith_total_live} BW techs, "
                            f"{sub_scan.wappalyzer_count} Wappalyzer techs"
                        )
                    else:
                        logger.warning(f"    ⚠️  {subdomain}: no data ({sub_scan.scan_error})")

                # Recalculate score with all aggregated evidence
                self._calculate_score(result)
                logger.info(
                    f"  📊 After subdomain scanning: score={result.score:.1f} "
                    f"| successful subdomains: {result.subdomains_successful}"
                )

                if result.score >= LLM_FALLBACK_THRESHOLD:
                    result.evidence_source = "subdomain_scraper"

        # -- Step 3: LLM score fallback ONLY if everything failed ----------
        if result.score < LLM_SCORE_FALLBACK_THRESHOLD:
            logger.info(
                f"  ⚠️  All domains failed for {ticker} — invoking LLM score fallback "
                f"(will be marked unverified, confidence capped at 0.30)"
            )
            llm_result = await asyncio.to_thread(
                _groq_tech_stack_score, ticker, company_name or ticker, domain
            )
            if llm_result:
                result.score                  = llm_result["score"]
                result.ai_tools_score         = llm_result["ai_tools_score"]
                result.infra_score            = llm_result["infra_score"]
                result.breadth_score          = llm_result["breadth_score"]
                result.confidence             = min(llm_result["confidence"], 0.30)  # Hard cap
                result.llm_fallback_used      = True
                result.llm_fallback_reasoning = llm_result.get("reasoning", "")
                result.evidence_source        = "llm_unverified"

                for tech in llm_result.get("known_technologies", []):
                    tech_lower = tech.lower()
                    result.technologies.append(TechnologyDetection(
                        name=tech,
                        category="llm_knowledge",
                        source="llm_knowledge",
                        is_ai_related=(
                            tech_lower in AI_SPECIFIC_TECHNOLOGIES
                            or tech_lower in AI_INFRASTRUCTURE
                        ),
                        confidence=0.30,
                    ))

                logger.warning(
                    f"  ⚠️  {ticker}: LLM unverified score {result.score:.1f}/100 "
                    f"(confidence: {result.confidence:.2f}) — no real scraper evidence found"
                )
            else:
                logger.error(f"  ❌ LLM fallback also failed for {ticker} — score remains 0")

        # -- Step 4: Store evidence to Snowflake ---------------------------
        await asyncio.to_thread(self._store_domain_evidence, result)

        logger.info(
            f"  ✅ {ticker}: {result.score:.1f}/100 "
            f"(ai_tools={result.ai_tools_score:.0f}, "
            f"infra={result.infra_score:.0f}, "
            f"breadth={result.breadth_score:.0f}) "
            f"| {len(result.technologies)} techs | source={result.evidence_source}"
            + (" [⚠️ LLM UNVERIFIED]" if result.llm_fallback_used else "")
        )

        return result

    # ------------------------------------------------------------------
    # Domain scanning helper — scans ONE domain with both scrapers
    # ------------------------------------------------------------------

    async def _scan_single_domain(
        self,
        domain: str,
        domain_type: str,
        discovery_source: str,
    ) -> DomainScanResult:
        """Run BuiltWith + Wappalyzer on a single domain and return raw evidence."""
        scan = DomainScanResult(
            domain=domain,
            domain_type=domain_type,
            discovery_source=discovery_source,
        )

        # BuiltWith
        if self.builtwith.is_enabled:
            bw_data = await self.builtwith.lookup_domain(domain)
            if bw_data:
                groups = bw_data.get("groups", [])
                scan.builtwith_groups = groups
                scan.builtwith_total_live = sum(g.get("live", 0) for g in groups)
                scan.builtwith_total_categories = sum(len(g.get("categories", [])) for g in groups)
                for group in groups:
                    name = group.get("name", "").lower()
                    scan.technologies.append(TechnologyDetection(
                        name=f"bw:{name}", category=name,
                        source="builtwith", is_ai_related=False, confidence=0.85,
                    ))
                if scan.builtwith_total_live > 0:
                    scan.scan_success = True
            else:
                scan.scan_error = "BuiltWith returned no data"
        else:
            scan.scan_error = "BuiltWith API key not configured"

        # Wappalyzer
        if self.wappalyzer.is_available:
            url = f"https://www.{domain}" if not domain.startswith("http") else domain
            tech_cats = self.wappalyzer.analyze_url(url)
            if tech_cats:
                scan.wappalyzer_techs = tech_cats
                scan.wappalyzer_count = len(tech_cats)
                for tech_name, categories in tech_cats.items():
                    tech_lower = tech_name.lower()
                    is_ai = (
                        tech_lower in AI_SPECIFIC_TECHNOLOGIES
                        or tech_lower in AI_INFRASTRUCTURE
                    )
                    scan.technologies.append(TechnologyDetection(
                        name=tech_name,
                        category=categories[0] if categories else "unknown",
                        source="wappalyzer",
                        is_ai_related=is_ai,
                        confidence=0.90,
                    ))
                if tech_cats:
                    scan.scan_success = True

        return scan

    # ------------------------------------------------------------------
    # Merge a DomainScanResult into the aggregate TechStackResult
    # ------------------------------------------------------------------

    def _merge_scan_into_result(self, result: TechStackResult, scan: DomainScanResult) -> None:
        """Aggregate a single domain scan into the top-level result."""
        result.technologies.extend(scan.technologies)
        result.builtwith_groups.extend(scan.builtwith_groups)
        result.builtwith_total_live += scan.builtwith_total_live
        result.builtwith_total_categories += scan.builtwith_total_categories
        # Merge wappalyzer (new keys only — avoid duplicates)
        for tech, cats in scan.wappalyzer_techs.items():
            if tech not in result.wappalyzer_techs:
                result.wappalyzer_techs[tech] = cats

    # ------------------------------------------------------------------
    # Snowflake evidence storage
    # ------------------------------------------------------------------

    def _store_domain_evidence(self, result: TechStackResult) -> None:
        """
        Persist every domain scan + technology to Snowflake.
        Tables: DIGITAL_PRESENCE_DOMAINS, DIGITAL_PRESENCE_TECHNOLOGIES

        Uses MERGE (upsert) on the UNIQUE constraint so re-runs on the same
        day update rather than duplicate rows.
        """
        try:
            from app.database.connection import get_snowflake_connection
            import uuid

            conn = get_snowflake_connection()
            cur = conn.cursor()
            run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            for scan in result.domain_scans:
                domain_id = str(uuid.uuid4())

                # MERGE into DIGITAL_PRESENCE_DOMAINS
                cur.execute("""
                    MERGE INTO DIGITAL_PRESENCE_DOMAINS AS target
                    USING (SELECT %s AS ticker, %s AS scanned_domain, %s AS run_date) AS src
                    ON target.ticker = src.ticker
                       AND target.scanned_domain = src.scanned_domain
                       AND target.run_date = src.run_date
                    WHEN MATCHED THEN UPDATE SET
                        builtwith_live       = %s,
                        builtwith_categories = %s,
                        wappalyzer_count     = %s,
                        scraper_score        = %s,
                        scan_success         = %s,
                        scan_error           = %s,
                        scanned_at           = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                        id, ticker, company_id, primary_domain, scanned_domain,
                        domain_type, discovery_source,
                        builtwith_live, builtwith_categories, wappalyzer_count,
                        scraper_score, scan_success, scan_error, run_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    # USING clause
                    result.ticker, scan.domain, run_date,
                    # UPDATE SET
                    scan.builtwith_total_live, scan.builtwith_total_categories,
                    scan.wappalyzer_count, scan.scraper_score,
                    scan.scan_success, scan.scan_error[:500] if scan.scan_error else "",
                    # INSERT VALUES
                    domain_id, result.ticker, result.company_id,
                    result.domain, scan.domain,
                    scan.domain_type, scan.discovery_source,
                    scan.builtwith_total_live, scan.builtwith_total_categories,
                    scan.wappalyzer_count, scan.scraper_score,
                    scan.scan_success, scan.scan_error[:500] if scan.scan_error else "",
                    run_date,
                ))

                # Insert technologies (skip llm_knowledge — not real evidence)
                real_techs = [t for t in scan.technologies if t.source != "llm_knowledge"]
                for tech in real_techs:
                    tech_id = str(uuid.uuid4())
                    cur.execute("""
                        MERGE INTO DIGITAL_PRESENCE_TECHNOLOGIES AS target
                        USING (SELECT %s AS ticker, %s AS scanned_domain,
                                      %s AS tech_name, %s AS run_date) AS src
                        ON target.ticker = src.ticker
                           AND target.scanned_domain = src.scanned_domain
                           AND target.tech_name = src.tech_name
                           AND target.run_date = src.run_date
                        WHEN MATCHED THEN UPDATE SET
                            category         = %s,
                            detection_source = %s,
                            is_ai_related    = %s,
                            confidence       = %s,
                            detected_at      = CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED THEN INSERT (
                            id, ticker, scanned_domain, tech_name, category,
                            detection_source, is_ai_related, confidence, run_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        # USING clause
                        result.ticker, scan.domain, tech.name, run_date,
                        # UPDATE SET
                        tech.category, tech.source, tech.is_ai_related, tech.confidence,
                        # INSERT VALUES
                        tech_id, result.ticker, scan.domain, tech.name,
                        tech.category, tech.source, tech.is_ai_related,
                        tech.confidence, run_date,
                    ))

            conn.commit()
            cur.close()
            conn.close()
            logger.info(
                f"  💾 Stored {len(result.domain_scans)} domain scans + "
                f"{sum(len(s.technologies) for s in result.domain_scans)} technologies "
                f"to Snowflake for {result.ticker}"
            )

        except Exception as e:
            # Storage failure is non-fatal — pipeline continues, S3 still has the data
            logger.error(
                f"  ⚠️  Failed to store domain evidence to Snowflake for {result.ticker}: {e}. "
                f"S3 storage is still intact."
            )

            result.technologies.append(
                TechnologyDetection(
                    name=tech_name,
                    category=categories[0] if categories else "unknown",
                    source="wappalyzer",
                    is_ai_related=is_ai,
                    confidence=0.90,
                )
            )

    # ------------------------------------------------------------------
    # Scoring (unchanged from original)
    # ------------------------------------------------------------------

    def _calculate_score(self, result: TechStackResult) -> None:
        """Calculate digital presence score (0-100) from scraper data only."""
        import math

        wappalyzer_names = {
            t.name.lower() for t in result.technologies if t.source == "wappalyzer"
        }
        ai_tools_found = wappalyzer_names & AI_SPECIFIC_TECHNOLOGIES
        infra_found    = wappalyzer_names & AI_INFRASTRUCTURE

        bw_group_names  = {g.get("name", "").lower() for g in result.builtwith_groups}
        bw_total_live   = result.builtwith_total_live
        bw_total_dead   = sum(g.get("dead", 0) for g in result.builtwith_groups)
        bw_total_cats   = result.builtwith_total_categories
        bw_group_count  = len(result.builtwith_groups)

        key_infra_groups = {"cdn", "cdns", "ssl", "analytics", "mx",
                            "payment", "shop", "cms", "mobile", "mapping"}
        key_groups_found = bw_group_names & key_infra_groups

        # Component 1: Technology Sophistication (max 40)
        live_score  = min(math.log2(bw_total_live + 1) * 2.1, 25) if bw_total_live > 0 else 0
        wp_ai_score = min(len(ai_tools_found) * 5, 15)
        result.ai_tools_score = round(live_score + wp_ai_score, 1)

        # Component 2: Infrastructure Maturity (max 30)
        group_score     = min(bw_group_count * 0.6, 15)
        key_infra_score = min(len(key_groups_found) * 1.5, 15)
        wp_infra        = min(len(infra_found) * 2, 5)
        result.infra_score = round(min(group_score + key_infra_score + wp_infra, 30), 1)

        # Component 3: Technology Breadth (max 30)
        cat_score = min(bw_total_cats * 0.15, 15)
        total_all = bw_total_live + bw_total_dead
        maintenance_score = (bw_total_live / total_all) * 15 if total_all > 0 else 0
        result.breadth_score = round(min(cat_score + maintenance_score, 30), 1)

        result.score = round(
            min(result.ai_tools_score + result.infra_score + result.breadth_score, 100.0), 1
        )

        sources_active = sum([
            bool(result.builtwith_groups),
            bool(result.wappalyzer_techs),
        ])
        result.confidence = 0.90 if sources_active == 2 else 0.70 if sources_active == 1 else 0.40

    # ------------------------------------------------------------------
    # Bulk analysis
    # ------------------------------------------------------------------

    async def analyze_companies(
        self,
        companies: List[Dict[str, Any]],
    ) -> Dict[str, TechStackResult]:
        """Analyze tech stacks for multiple companies."""
        results = {}
        for company in companies:
            cid  = company.get("id", "")
            ticker = company.get("ticker", "")
            company_name = company.get("name")
            try:
                result = await self.analyze_company(cid, ticker, company_name=company_name)
                results[cid] = result
            except Exception as e:
                logger.error(f"Failed to analyze {ticker}: {e}")
                results[cid] = TechStackResult(
                    company_id=cid, ticker=ticker, domain="unknown",
                    errors=[str(e)],
                    collected_at=datetime.now(timezone.utc).isoformat(),
                )
        return results

    # ------------------------------------------------------------------
    # Serialization (for S3 storage)
    # ------------------------------------------------------------------

    @staticmethod
    def result_to_dict(r: TechStackResult) -> Dict[str, Any]:
        """Convert TechStackResult to JSON-serializable dict."""
        return {
            "company_id":               r.company_id,
            "ticker":                   r.ticker,
            "domain":                   r.domain,
            "score":                    r.score,
            "ai_tools_score":           r.ai_tools_score,
            "infra_score":              r.infra_score,
            "breadth_score":            r.breadth_score,
            "confidence":               r.confidence,
            "evidence_source":          r.evidence_source,
            # Subdomain evidence
            "subdomains_discovered":    r.subdomains_discovered,
            "subdomains_successful":    r.subdomains_successful,
            "domains_scanned": [
                {
                    "domain":            s.domain,
                    "domain_type":       s.domain_type,
                    "discovery_source":  s.discovery_source,
                    "scan_success":      s.scan_success,
                    "builtwith_live":    s.builtwith_total_live,
                    "wappalyzer_count":  s.wappalyzer_count,
                    "scraper_score":     s.scraper_score,
                    "technologies":      [t.name for t in s.technologies if t.source != "llm_knowledge"],
                }
                for s in r.domain_scans
            ],
            # Aggregated tech data
            "builtwith_total_live":     r.builtwith_total_live,
            "builtwith_total_categories": r.builtwith_total_categories,
            "wappalyzer_techs":         {k: v for k, v in r.wappalyzer_techs.items()},
            "ai_technologies_detected": [
                t.name for t in r.technologies if t.is_ai_related and t.source != "llm_knowledge"
            ],
            "all_technologies": [
                {"name": t.name, "category": t.category,
                 "source": t.source, "is_ai_related": t.is_ai_related}
                for t in r.technologies
            ],
            # LLM fallback (last resort only)
            "llm_fallback_used":       r.llm_fallback_used,
            "llm_fallback_reasoning":  r.llm_fallback_reasoning,
            "collected_at":            r.collected_at,
            "errors":                  r.errors,
        }