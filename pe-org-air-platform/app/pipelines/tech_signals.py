"""
Tech Stack Signal Analysis — Digital Presence
app/pipelines/tech_signals.py

Collects ACTUAL technology stack data from company websites using:
  1. BuiltWith Free API  — technology group counts & categories
  2. Wappalyzer (python-Wappalyzer) — specific technology names

This is the Digital Presence signal source for CS2/CS3.
It answers: "What technologies does this company actually run?"

NOTE: This is SEPARATE from job_signals.py which answers
      "Who are they hiring?" (technology_hiring signal).
"""
from __future__ import annotations

import asyncio
import hashlib
import httpx
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

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
    source: str          # "builtwith" or "wappalyzer"
    is_ai_related: bool
    confidence: float


@dataclass
class TechStackResult:
    """Complete tech stack analysis for a company."""
    company_id: str
    ticker: str
    domain: str

    # Raw detections
    technologies: List[TechnologyDetection] = field(default_factory=list)

    # BuiltWith data
    builtwith_groups: List[Dict[str, Any]] = field(default_factory=list)
    builtwith_total_live: int = 0
    builtwith_total_categories: int = 0

    # Wappalyzer data
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

# (BuiltWith free API only gives group-level counts, not specific tech names,
#  so AI detection comes only from Wappalyzer specific tech matches)


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

        Returns raw JSON response with technology group counts.
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

                # Free API returns groups with live/dead counts
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
# Wappalyzer Client (python-Wappalyzer open-source library)
# ---------------------------------------------------------------------------

class WappalyzerClient:
    """Client using python-Wappalyzer for real-time website tech detection.

    FIX (v2): Uses requests with configurable timeout + retry instead of
    WebPage.new_from_url which has a hardcoded 10s timeout. This fixes
    the CAT (caterpillar.com) timeout issue where Wappalyzer returned
    nothing and confidence dropped to 0.70.
    """

    DEFAULT_TIMEOUT = 20       # seconds (was 10 via new_from_url)
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

    def analyze_url(
        self,
        url: str,
        timeout: int = None,
        retries: int = None,
    ) -> Dict[str, List[str]]:
        """
        Analyze a URL and return detected technologies with categories.

        Uses requests library with configurable timeout and retry logic
        instead of WebPage.new_from_url (which has a hardcoded 10s timeout
        that causes failures on slow corporate sites like caterpillar.com).

        Args:
            url: Full URL to scan (e.g. "https://www.caterpillar.com")
            timeout: Request timeout in seconds (default 20)
            retries: Number of retry attempts on timeout (default 2)

        Returns:
            Dict like {"React": ["JavaScript frameworks"], "Node.js": ["Web servers"]}
        """
        if not self._available:
            return {}

        import requests as req_lib

        timeout = timeout or self.DEFAULT_TIMEOUT
        retries = retries or self.DEFAULT_RETRIES

        for attempt in range(1, retries + 1):
            try:
                wappalyzer = self._wappalyzer_cls.latest()

                # Fetch page with explicit timeout + realistic User-Agent
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
                        timeout += 5  # increase timeout on retry
                        continue
                    logger.error(
                        f"Wappalyzer timed out for {url} after {retries} attempts"
                    )
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

                # Analyze technologies
                results = wappalyzer.analyze_with_categories(webpage)

                # Flatten: {tech_name: {categories: [...]}} -> {tech_name: [cat_names]}
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
    # def analyze_url(self, url: str) -> Dict[str, List[str]]:
    #     """
    #     Analyze a URL and return detected technologies with categories.

    #     Returns:
    #         Dict like {"React": ["JavaScript frameworks"], "Node.js": ["Web servers"], ...}
    #     """
    #     if not self._available:
    #         return {}

    #     try:
    #         wappalyzer = self._wappalyzer_cls.latest()
    #         webpage = self._webpage_cls.new_from_url(url)
    #         results = wappalyzer.analyze_with_categories(webpage)

    #         # Flatten: {tech_name: {categories: [...]}} -> {tech_name: [cat_names]}
    #         tech_categories = {}
    #         for tech_name, info in results.items():
    #             cats = info.get("categories", [])
    #             if isinstance(cats, list):
    #                 tech_categories[tech_name] = cats
    #             elif isinstance(cats, dict):
    #                 tech_categories[tech_name] = list(cats.values())
    #             else:
    #                 tech_categories[tech_name] = [str(cats)]

    #         return tech_categories

    #     except Exception as e:
    #         logger.error(f"Wappalyzer analysis failed for {url}: {e}")
    #         return {}


# ---------------------------------------------------------------------------
# Main Collector
# ---------------------------------------------------------------------------

class TechStackCollector:
    """
    Collect digital presence signals from company websites.

    Uses BuiltWith (breadth) + Wappalyzer (specific tech names)
    to score a company's technology sophistication for the
    digital_presence signal category.
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

        Args:
            company_id: Company UUID
            ticker: Stock ticker
            domain: Company website domain (auto-resolved from config if None)
            company_name: Official company name (used to derive domain if not in config)

        Returns:
            TechStackResult with scores and detections
        """
        # Resolve domain
        if not domain:
            mapping = COMPANY_NAME_MAPPINGS.get(ticker.upper(), {})
            domain = mapping.get("domain")
        if not domain and company_name:
            # Derive a best-guess domain from company name
            import re as _re
            # Strip common legal suffixes
            clean = _re.sub(
                r",?\s*(Inc\.?|Corp\.?|LLC\.?|Ltd\.?|Co\.?|Holdings?|Group|PLC|N\.A\.?|S\.A\.?|AG)\.?\s*$",
                "", company_name, flags=_re.IGNORECASE,
            ).strip()
            # Keep only alphanumeric chars, lowercase
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

        # --- Source 1: BuiltWith Free API ---
        if self.builtwith.is_enabled:
            logger.info(f"  📡 Querying BuiltWith for {domain}...")
            bw_data = await self.builtwith.lookup_domain(domain)
            if bw_data:
                self._process_builtwith(result, bw_data)
            else:
                result.errors.append("BuiltWith lookup returned no data")
            # Respect rate limit
            await asyncio.sleep(1.1)
        else:
            result.errors.append("BuiltWith API key not configured")

        # --- Source 2: Wappalyzer ---
        if self.wappalyzer.is_available:
            logger.info(f"  🔍 Scanning {domain} with Wappalyzer...")
            url = f"https://www.{domain}"
            tech_cats = self.wappalyzer.analyze_url(url)
            if tech_cats:
                self._process_wappalyzer(result, tech_cats)
            else:
                result.errors.append("Wappalyzer returned no technologies")
        else:
            result.errors.append("python-Wappalyzer not installed")

        # --- Score ---
        self._calculate_score(result)

        logger.info(
            f"  ✅ {ticker}: {result.score:.1f}/100 "
            f"(ai_tools={result.ai_tools_score:.0f}, "
            f"infra={result.infra_score:.0f}, "
            f"breadth={result.breadth_score:.0f}) "
            f"| {len(result.technologies)} techs detected"
        )

        return result

    # ------------------------------------------------------------------
    # Processing helpers
    # ------------------------------------------------------------------

    def _process_builtwith(self, result: TechStackResult, data: Dict) -> None:
        """Extract technology info from BuiltWith Free API response."""
        groups = data.get("groups", [])
        result.builtwith_groups = groups

        total_live = 0
        total_categories = 0

        for group in groups:
            name = group.get("name", "").lower()
            live = group.get("live", 0)
            total_live += live

            categories = group.get("categories", [])
            total_categories += len(categories)

            # Create a detection for each BuiltWith group
            # BuiltWith free API only gives group names, not specific tools
            # So we never flag these as AI — only Wappalyzer detects specific AI tools
            result.technologies.append(
                TechnologyDetection(
                    name=f"bw:{name}",
                    category=name,
                    source="builtwith",
                    is_ai_related=False,
                    confidence=0.85,
                )
            )

        result.builtwith_total_live = total_live
        result.builtwith_total_categories = total_categories

    def _process_wappalyzer(
        self, result: TechStackResult, tech_cats: Dict[str, List[str]]
    ) -> None:
        """Process Wappalyzer detections."""
        result.wappalyzer_techs = tech_cats

        for tech_name, categories in tech_cats.items():
            tech_lower = tech_name.lower()
            is_ai = (
                tech_lower in AI_SPECIFIC_TECHNOLOGIES
                or tech_lower in AI_INFRASTRUCTURE
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
    # Scoring
    # ------------------------------------------------------------------

    def _calculate_score(self, result: TechStackResult) -> None:
        """
        Calculate digital presence score (0-100).
        Uses BuiltWith live tech counts as primary differentiator.
        """
        import math

        # --- Wappalyzer detections ---
        wappalyzer_names = {
            t.name.lower() for t in result.technologies if t.source == "wappalyzer"
        }
        ai_tools_found = wappalyzer_names & AI_SPECIFIC_TECHNOLOGIES
        infra_found = wappalyzer_names & AI_INFRASTRUCTURE

        # --- BuiltWith analysis ---
        bw_group_names = {
            g.get("name", "").lower() for g in result.builtwith_groups
        }
        bw_total_live = result.builtwith_total_live
        bw_total_dead = sum(g.get("dead", 0) for g in result.builtwith_groups)
        bw_total_categories = result.builtwith_total_categories
        bw_group_count = len(result.builtwith_groups)

        # Key infra groups
        key_infra_groups = {"cdn", "cdns", "ssl", "analytics", "mx",
                           "payment", "shop", "cms", "mobile", "mapping"}
        key_groups_found = bw_group_names & key_infra_groups

        # === Component 1: Technology Sophistication (max 40) ===
        if bw_total_live > 0:
            log_live = math.log2(bw_total_live + 1)
            live_score = min(log_live * 2.1, 25)
        else:
            live_score = 0
        wp_ai_score = min(len(ai_tools_found) * 5, 15)
        result.ai_tools_score = round(live_score + wp_ai_score, 1)

        # === Component 2: Infrastructure Maturity (max 30) ===
        group_score = min(bw_group_count * 0.6, 15)
        key_infra_score = min(len(key_groups_found) * 1.5, 15)
        wp_infra = min(len(infra_found) * 2, 5)
        result.infra_score = round(min(group_score + key_infra_score + wp_infra, 30), 1)

        # === Component 3: Technology Breadth (max 30) ===
        cat_score = min(bw_total_categories * 0.15, 15)
        total_all = bw_total_live + bw_total_dead
        if total_all > 0:
            active_ratio = bw_total_live / total_all
            maintenance_score = active_ratio * 15
        else:
            maintenance_score = 0
        result.breadth_score = round(min(cat_score + maintenance_score, 30), 1)

        # === Total ===
        result.score = round(
            result.ai_tools_score + result.infra_score + result.breadth_score, 1
        )
        result.score = min(result.score, 100.0)

        # Confidence
        sources_active = sum([
            bool(result.builtwith_groups),
            bool(result.wappalyzer_techs),
        ])
        if sources_active == 2:
            result.confidence = 0.90
        elif sources_active == 1:
            result.confidence = 0.70
        else:
            result.confidence = 0.40

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
            cid = company.get("id", "")
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
            "company_id": r.company_id,
            "ticker": r.ticker,
            "domain": r.domain,
            "score": r.score,
            "ai_tools_score": r.ai_tools_score,
            "infra_score": r.infra_score,
            "breadth_score": r.breadth_score,
            "confidence": r.confidence,
            "builtwith_total_live": r.builtwith_total_live,
            "builtwith_total_categories": r.builtwith_total_categories,
            "wappalyzer_techs": {
                k: v for k, v in r.wappalyzer_techs.items()
            },
            "ai_technologies_detected": [
                t.name for t in r.technologies if t.is_ai_related
            ],
            "all_technologies": [
                {"name": t.name, "category": t.category,
                 "source": t.source, "is_ai_related": t.is_ai_related}
                for t in r.technologies
            ],
            "collected_at": r.collected_at,
            "errors": r.errors,
        }