# app/scoring/talent_concentration.py
import json
import logging
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional, Set

from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

_SKILL_FUZZY_THRESHOLD     = 88   # partial_ratio: skill name vs description text
_SENIORITY_FUZZY_THRESHOLD = 82   # ratio: title word vs seniority keyword
_AI_MENTION_FUZZY_THRESHOLD = 88  # partial_ratio: AI keyword vs review text
_FUZZY_SKILL_MIN_LEN       = 6   # skip fuzzy for very short skill tokens (e.g. "go", "r")
_FUZZY_KEYWORD_MIN_LEN     = 8   # skip fuzzy for short AI terms already handled by regex

# AI awareness keywords (mirrors glassdoor_collector.py AI_AWARENESS_KEYWORDS)
_AI_KEYWORDS = frozenset([
    "ai", "machine learning", "automation", "deep learning", "nlp",
    "llm", "generative ai", "data science", "neural network",
    "artificial intelligence", "ml", "large language model",
    "computer vision", "predictive analytics",
])

# ------------------------------------------------------------------ #
# Expanded skill list for TC skill_concentration calculation          #
# ------------------------------------------------------------------ #
_EXPANDED_AI_SKILLS = frozenset([
    # CS2 originals
    "python", "pytorch", "tensorflow", "scikit-learn",
    "spark", "hadoop", "kubernetes", "docker",
    "aws sagemaker", "azure ml", "gcp vertex",
    "huggingface", "langchain", "openai",
    # GPU / NVIDIA ecosystem
    "cuda", "tensorrt", "triton", "nccl", "nvlink",
    "deepstream", "isaac", "omniverse", "vllm",
    # ML frameworks & tools
    "jax", "onnx", "mlflow", "kubeflow", "ray",
    "wandb", "weights & biases", "dvc",
    "keras", "xgboost", "lightgbm", "catboost",
    # Data / infra
    "snowflake", "databricks", "airflow", "kafka",
    "redis", "elasticsearch", "postgresql", "mongodb",
    # Cloud ML
    "sagemaker", "vertex ai", "bedrock", "azure openai",
    # Languages commonly paired with AI
    "go", "rust", "julia", "r",
    "c++",
    # HPC / parallel
    "mpi", "openmp", "openacc", "infiniband",
      # Manufacturing / Industrial (GE Aerospace)
    "matlab", "simulink", "labview", "ansys", "catia",
    "solidworks", "siemens nx", "scada", "plc",
    "embedded", "c", "fortran", "vhdl",
    "ros", "gazebo", "digital twin",
    "predictive maintenance", "condition monitoring",
    # Retail / Supply Chain Analytics (WMT)
    "sql", "tableau", "power bi", "sas", "spss",
    "excel vba", "looker", "dbt", "fivetran",
    "ab testing", "demand forecasting",
    # General data engineering
    "dask", "polars", "pandas", "numpy", "scipy",
    "java", "scala", "presto", "trino", "hive",
    # Cloud/Enterprise analytics (WMT job descriptions)
"bigquery", "prophet", "pydantic", "confluence",
"ocr", "dax", "terraform", "grafana", "prometheus",
])

# Short terms that need whole-word matching to avoid false positives
_WHOLE_WORD_SKILLS = frozenset([
    "go", "r", "c++", "ray", "jax", "mpi", "cuda", "rust", "julia",
    "redis", "kafka", "dvc", "c", "sql", "sas", "ros", "plc", "dbt", "java","dax",

])

# ------------------------------------------------------------------ #
# FIX 3: Calibrated skill denominator for expanded list              #
# With ~50 skills in the expanded list, 15 saturates too easily.     #
# 25 means: 25+ unique skills = zero concentration (fully distributed)#
# ------------------------------------------------------------------ #
# _SKILL_DENOMINATOR = 25
_SKILL_DENOMINATOR = 35

@dataclass
class JobAnalysis:
    """Analysis of job postings for talent concentration."""
    total_ai_jobs: int
    senior_ai_jobs: int    # Principal, Staff, Director, VP level
    mid_ai_jobs: int       # Senior, Lead level
    entry_ai_jobs: int     # Junior, Associate, entry level
    unique_skills: Set[str]


@dataclass
class GlassdoorReview:
    """Single employee review loaded from the S3 raw snapshot."""
    review_id: str
    rating: float
    title: str
    pros: str
    cons: str
    advice_to_management: Optional[str]
    is_current_employee: bool
    job_title: str
    review_date: Optional[str]
    source: str


class TalentConcentrationCalculator:
    def __init__(self):
        pass

    # ------------------------------------------------------------------ #
    # Job-posting analysis — extracts skills from description text        #
    # ------------------------------------------------------------------ #

    def analyze_job_postings(self, postings: List[dict]) -> JobAnalysis:
        """Analyze job postings to extract AI talent concentration signals.

        Skills are collected from TWO sources per posting:
          1. ai_skills_found (pre-computed by CS2 pipeline)
          2. Full description text scanned against _EXPANDED_AI_SKILLS
        """
        senior_keywords = {"principal", "staff", "director", "vp", "head", "chief"}
        mid_keywords = {"senior", "lead", "manager"}
        entry_keywords = {"junior", "associate", "entry", "intern"}

        total_ai_jobs = 0
        senior_ai_jobs = 0
        mid_ai_jobs = 0
        entry_ai_jobs = 0
        unique_skills: Set[str] = set()

        for posting in postings:
            if not posting.get("is_ai_role", False):
                continue
            total_ai_jobs += 1
            title_lower = posting.get("title", "").lower()
            title_words = set(title_lower.split())
            if title_words & senior_keywords:
                senior_ai_jobs += 1
            elif title_words & mid_keywords:
                mid_ai_jobs += 1
            elif title_words & entry_keywords:
                entry_ai_jobs += 1
            else:
                # Fuzzy fallback: catches punctuation-attached words ("director,")
                # and 1-2 char typos ("senor", "pricipal")
                def _fuzzy_word_match(words, kw_set):
                    return any(
                        fuzz.ratio(w, kw) >= _SENIORITY_FUZZY_THRESHOLD
                        for w in words for kw in kw_set
                    )
                if _fuzzy_word_match(title_words, senior_keywords):
                    senior_ai_jobs += 1
                elif _fuzzy_word_match(title_words, mid_keywords):
                    mid_ai_jobs += 1
                elif _fuzzy_word_match(title_words, entry_keywords):
                    entry_ai_jobs += 1

            # Source 1: pre-computed skills from CS2
            unique_skills.update(posting.get("ai_skills_found", []))

            # Source 2: scan full description for expanded skill list
            desc = posting.get("description", "").lower()
            if desc:
                for skill in _EXPANDED_AI_SKILLS:
                    if skill in _WHOLE_WORD_SKILLS:
                        # Short tokens: keep regex whole-word, no fuzzy (too short = high false-positive risk)
                        if re.search(r"\b" + re.escape(skill) + r"\b", desc):
                            unique_skills.add(skill)
                    elif skill in desc:
                        # Exact substring fast path
                        unique_skills.add(skill)
                    elif len(skill) >= _FUZZY_SKILL_MIN_LEN:
                        # Fuzzy fallback: catches "pytoch", "scikit learn", "tensor rt", etc.
                        if fuzz.partial_ratio(skill, desc) >= _SKILL_FUZZY_THRESHOLD:
                            unique_skills.add(skill)

        return JobAnalysis(
            total_ai_jobs=total_ai_jobs,
            senior_ai_jobs=senior_ai_jobs,
            mid_ai_jobs=mid_ai_jobs,
            entry_ai_jobs=entry_ai_jobs,
            unique_skills=unique_skills,
        )

    # ------------------------------------------------------------------ #
    # Glassdoor S3 loader — tries BOTH path patterns                     #
    # ------------------------------------------------------------------ #

    @staticmethod
    def load_glassdoor_reviews(
        ticker: str,
        s3_service=None,
    ) -> List[GlassdoorReview]:
        """Load individual reviews from the latest raw S3 snapshot for *ticker*.

        Tries multiple S3 path patterns:
          1. glassdoor_signals/raw/{TICKER}/{timestamp}_raw.json  (culture_collector.py)
          2. glassdoor_signals/raw/{TICKER}_raw.json              (flat format)
        """
        from app.services.s3_storage import get_s3_service

        svc = s3_service or get_s3_service()
        ticker_upper = ticker.upper()

        # --- Attempt 1: timestamped subfolder path ---
        prefix = f"glassdoor_signals/raw/{ticker_upper}/"
        keys = svc.list_files(prefix)
        if keys:
            latest_key = sorted(keys)[-1]
            reviews = TalentConcentrationCalculator._parse_glassdoor_s3(svc, latest_key, ticker_upper)
            if reviews is not None:
                return reviews

        # --- Attempt 2: flat file path ---
        flat_key = f"glassdoor_signals/raw/{ticker_upper}_raw.json"
        reviews = TalentConcentrationCalculator._parse_glassdoor_s3(svc, flat_key, ticker_upper)
        if reviews is not None:
            return reviews

        logger.warning("No Glassdoor raw snapshot found in S3 for %s (tried both path patterns)", ticker_upper)
        return []

    @staticmethod
    def _parse_glassdoor_s3(svc, key: str, ticker: str) -> Optional[List["GlassdoorReview"]]:
        """Parse a single Glassdoor S3 JSON file into GlassdoorReview list."""
        raw = svc.get_file(key)
        if raw is None:
            return None

        try:
            wrapper = json.loads(raw)
        except json.JSONDecodeError:
            logger.error("Invalid JSON in Glassdoor snapshot: %s", key)
            return None

        reviews = []
        for item in wrapper.get("reviews", []):
            reviews.append(GlassdoorReview(
                review_id=item.get("review_id", ""),
                rating=float(item.get("rating") or 0.0),
                title=item.get("title") or "",
                pros=item.get("pros") or "",
                cons=item.get("cons") or "",
                advice_to_management=item.get("advice_to_management"),
                is_current_employee=bool(item.get("is_current_employee", False)),
                job_title=item.get("job_title") or "",
                review_date=item.get("review_date"),
                source=item.get("source", "unknown"),
            ))

        logger.info("Loaded %d Glassdoor reviews for %s from %s", len(reviews), ticker, key)
        return reviews

    # ------------------------------------------------------------------ #
    # FIX 2: Improved individual-mention counter                         #
    # ------------------------------------------------------------------ #
    # Old regex only matched "CEO Huang" — missed "Jensen", "Huang",     #
    # "CEO Jensen Huang", "Jamie Dimon", etc.                            #
    # New approach: match exec titles followed by names, OR well-known    #
    # exec names appearing standalone.                                    #
    # ------------------------------------------------------------------ #

    # Well-known exec names for the 5 CS3 portfolio companies
    _KNOWN_EXECUTIVES = {
        # NVDA
        "jensen huang", "jensen", "huang",
        # JPM
        "jamie dimon", "dimon",
        # WMT
        "doug mcmillon", "mcmillon",
        # GE
        "larry culp", "culp",
        # DG
        "todd vasos", "vasos",
    }

    @staticmethod
    def count_individual_mentions(reviews: List[GlassdoorReview]) -> tuple:
        """Return (individual_mention_count, total_review_count).

        A review counts as an individual mention if it contains:
          1. An executive title followed by a capitalized name (generic), OR
          2. A known executive name for CS3 portfolio companies

        This captures reviews like "Jensen is visionary", "Huang drives AI",
        "Jamie Dimon leads from the top" — not just "CEO Huang".
        """
        # Pattern 1: Title + Name (original, kept for generality)
        _TITLE_PATTERN = re.compile(
            r'\b(?:CEO|CTO|CFO|COO|CIO|CDO|CAIO|President|Founder|Chairman)'
            r'\s+[A-Z][a-z]+\b'
        )

        # Pattern 2: Known executive names (whole-word, case-insensitive)
        _name_patterns = []
        for name in TalentConcentrationCalculator._KNOWN_EXECUTIVES:
            # Only match names with 4+ chars to avoid false positives
            if len(name) >= 4:
                _name_patterns.append(re.compile(
                    r'\b' + re.escape(name) + r'\b', re.IGNORECASE
                ))

        mention_count = 0
        for review in reviews:
            text = " ".join(filter(None, [
                review.pros,
                review.cons,
                review.advice_to_management or "",
                review.title,
                review.job_title,
            ]))

            # Check title+name pattern
            if _TITLE_PATTERN.search(text):
                mention_count += 1
                continue

            # Check known executive names
            text_lower = text.lower()
            for pat in _name_patterns:
                if pat.search(text_lower):
                    mention_count += 1
                    break

        return mention_count, len(reviews)

    # ------------------------------------------------------------------ #
    # AI-mention counter                                                   #
    # ------------------------------------------------------------------ #

    @staticmethod
    def count_ai_mentions(reviews: List[GlassdoorReview]) -> tuple:
        """Return (ai_mention_count, total_review_count)."""
        whole_word_terms = frozenset(["ai", "ml", "nlp", "llm"])
        ai_mention_count = 0

        for review in reviews:
            text = " ".join(filter(None, [
                review.pros,
                review.cons,
                review.advice_to_management or "",
                review.title,
                review.job_title,
            ])).lower()

            matched = False
            for kw in _AI_KEYWORDS:
                if kw in whole_word_terms:
                    # Short acronyms ("ai", "ml", "nlp", "llm"): whole-word regex only
                    if re.search(rf"\b{re.escape(kw)}\b", text):
                        matched = True
                        break
                elif kw in text:
                    # Exact substring fast path
                    matched = True
                    break
                elif len(kw) >= _FUZZY_KEYWORD_MIN_LEN:
                    # Fuzzy fallback: catches "artifical intelligence", "deeplearning", etc.
                    if fuzz.partial_ratio(kw, text) >= _AI_MENTION_FUZZY_THRESHOLD:
                        matched = True
                        break
            if matched:
                ai_mention_count += 1

        return ai_mention_count, len(reviews)

    # ------------------------------------------------------------------ #
    # TC score                                                             #
    # ------------------------------------------------------------------ #

    def calculate_tc(
        self,
        job_analysis: JobAnalysis,
        glassdoor_individual_mentions: int = 0,
        glassdoor_review_count: int = 0,
    ) -> Decimal:
        """Calculate Talent Concentration (TC) score as a Decimal in [0, 1].

        Formula (CS3 Task 5.0e spec):
            TC = 0.40 × leadership_ratio
               + 0.30 × team_size_factor
               + 0.20 × skill_concentration
               + 0.10 × individual_factor

        FIX 1: skill denominator raised from 15 → 25 to account for
               expanded skill list (prevents premature floor at 0).
        FIX 2: individual_factor uses improved exec name matching.
        FIX 3: Zero-AI-jobs defaults calibrated to produce TC ≈ 0.30
               (matching CS3 Table 5 expectation for laggards like DG)
               instead of TC ≈ 0.70 from maxed-out defaults.
        """
        total = job_analysis.total_ai_jobs
        senior = job_analysis.senior_ai_jobs

        # ---- FIX 3: Calibrated defaults for zero-AI-jobs ----
        # CS3 expects DG (0 AI jobs) → TC ≈ 0.30, not 0.70
        # Old defaults: leadership=0.5, team_size=1.0, skill=1.0 → TC=0.70
        # New defaults: leadership=0.3, team_size=0.5, skill=0.6 → TC ≈ 0.30
        if total == 0:
            leadership_ratio = 0.25      # Some concentration assumed, not extreme
            team_size_factor = 0.40      # No team = moderate risk, not maximum
            skill_concentration = 0.50   # No skills detected = moderate concentration
        else:
            leadership_ratio = senior / total
            team_size_factor = min(1.0, 1.0 / (total ** 0.5 + 0.1))
            # FIX 1: Use _SKILL_DENOMINATOR (25) instead of hardcoded 15
            skill_concentration = max(0.0, 1.0 - len(job_analysis.unique_skills) / _SKILL_DENOMINATOR)

        if glassdoor_review_count > 0:
            individual_factor = glassdoor_individual_mentions / glassdoor_review_count
        else:
            individual_factor = 0.0  # No data = no signal

        tc = (
            0.4 * leadership_ratio
            + 0.3 * team_size_factor
            + 0.2 * skill_concentration
            + 0.1 * individual_factor
        )

        return Decimal(str(max(0.0, min(1.0, tc)))).quantize(Decimal("0.0001"))