# """Application configuration with comprehensive validation."""
# from typing import Optional, Literal, List, Dict
# from functools import lru_cache
# from decimal import Decimal
# from pydantic import Field, field_validator, model_validator, SecretStr
# from pydantic_settings import BaseSettings, SettingsConfigDict


# # =============================================================================
# # COMPANY NAME MAPPINGS
# # =============================================================================
# # Maps ticker -> search name and aliases for job scraping and fuzzy matching
# # - "search": Primary name to use when searching job sites
# # - "aliases": All valid variations for fuzzy matching (includes search name)
# # =============================================================================
# # For companies with multiple historical names, use a list.

# COMPANY_NAME_MAPPINGS = {
#     "CAT": {
#         "official": "Caterpillar Inc.",
#         "search": "Caterpillar",
#         "patent_search": "Caterpillar Inc.",  # Exact match in PatentsView
#         "domain": "caterpillar.com",
#         "aliases": ["Caterpillar", "Caterpillar Inc", "Caterpillar Inc.", "CAT"],
#     },
#     "DE": {
#         "official": "Deere & Company",
#         "search": "John Deere",
#         "patent_search": "Deere & Company",  # Exact match in PatentsView
#         "domain": "deere.com",
#         "aliases": ["John Deere", "Deere", "Deere & Company", "JD"],
#     },
#     "UNH": {
#         "official": "UnitedHealth Group Incorporated",
#         "search": "UnitedHealth",
#         "patent_search": "UnitedHealth Group",  # PatentsView: without "Incorporated"
#         "domain": "unitedhealthgroup.com",
#         "aliases": ["UnitedHealth", "UnitedHealth Group", "United Health", "UnitedHealthcare", "UHG"],
#     },
#     "HCA": {
#         "official": "HCA Healthcare, Inc.",
#         "search": "HCA Healthcare",
#         "patent_search": "HCA Healthcare",  # Few patents expected
#         "domain": "hcahealthcare.com",
#         "aliases": ["HCA Healthcare", "HCA", "HCA Inc", "Hospital Corporation of America"],
#     },
#     "ADP": {
#         "official": "Automatic Data Processing, Inc.",
#         "search": "ADP",
#         "patent_search": "Automatic Data Processing",  # PatentsView: without "Inc."
#         "domain": "adp.com",
#         "aliases": ["ADP", "Automatic Data Processing", "ADP Inc"],
#     },
#     "PAYX": {
#         "official": "Paychex, Inc.",
#         "search": "Paychex",
#         "patent_search": "Paychex",  # Very few patents expected
#         "domain": "paychex.com",
#         "aliases": ["Paychex", "Paychex Inc", "Paychex Inc."],
#     },
#     "WMT": {
#         "official": "Walmart Inc.",
#         "search": "Walmart",
#         "patent_search": "Walmart Apollo",  # Post-2018: all new patents under "Walmart Apollo, LLC"
#         "domain": "walmart.com",
#         "aliases": ["Walmart", "Walmart Inc", "Walmart Inc.", "Wal-Mart", "Wal Mart"],
#     },
#     "TGT": {
#         "official": "Target Corporation",
#         "search": "Target",
#         "patent_search": "Target Corporation",  # Also "Target Brands" for some
#         "domain": "target.com",
#         "aliases": ["Target", "Target Corporation", "Target Corp"],
#     },
#     "JPM": {
#         "official": "JPMorgan Chase & Co.",
#         "search": "JPMorgan Chase",
#         "patent_search": "JPMorgan Chase",  # Already works
#         "domain": "jpmorganchase.com",
#         "aliases": ["JPMorgan Chase", "JPMorgan", "JP Morgan", "Chase", "J.P. Morgan", "JPMC"],
#     },
#     "GS": {
#         "official": "The Goldman Sachs Group, Inc.",
#         "search": "Goldman Sachs",
#         "patent_search": "Goldman Sachs",  # Already works
#         "domain": "goldmansachs.com",
#         "aliases": ["Goldman Sachs", "Goldman", "GS", "Goldman Sachs Group"],
#     },
# }


# def get_company_search_name(ticker: str) -> Optional[str]:
#     """
#     Get the search name for a company ticker.

#     Args:
#         ticker: Company ticker symbol (e.g., "DE", "ADP")

#     Returns:
#         Search name to use for job sites, or None if not mapped
#     """
#     ticker = ticker.upper()
#     mapping = COMPANY_NAME_MAPPINGS.get(ticker)
#     return mapping["search"] if mapping else None


# def get_company_aliases(ticker: str) -> List[str]:
#     """
#     Get all valid name aliases for a company ticker.

#     Args:
#         ticker: Company ticker symbol

#     Returns:
#         List of valid name variations for fuzzy matching
#     """
#     ticker = ticker.upper()
#     mapping = COMPANY_NAME_MAPPINGS.get(ticker)
#     return mapping["aliases"] if mapping else []


# def get_search_name_by_official(official_name: str) -> Optional[str]:
#     """
#     Get search name from official company name.

#     Args:
#         official_name: Official company name (e.g., "Deere & Company")

#     Returns:
#         Search name to use for job sites, or None if not mapped
#     """
#     official_lower = official_name.lower().strip()
#     for ticker, mapping in COMPANY_NAME_MAPPINGS.items():
#         if mapping["official"].lower() == official_lower:
#             return mapping["search"]
#     return None


# def get_aliases_by_official(official_name: str) -> List[str]:
#     """
#     Get all valid name aliases from official company name.

#     Args:
#         official_name: Official company name

#     Returns:
#         List of valid name variations for fuzzy matching
#     """
#     official_lower = official_name.lower().strip()
#     for ticker, mapping in COMPANY_NAME_MAPPINGS.items():
#         if mapping["official"].lower() == official_lower:
#             return mapping["aliases"]
#     return []

# # Step 2: Add this helper function to config.py (after existing helpers)

# def get_patent_search_name(ticker: str) -> Optional[str]:
#     """
#     Get the patent search name for PatentsView API.
#     Falls back to official name if no patent_search override exists.
#     """
#     ticker = ticker.upper()
#     mapping = COMPANY_NAME_MAPPINGS.get(ticker)
#     if not mapping:
#         return None
#     return mapping.get("patent_search", mapping.get("official"))


# class Settings(BaseSettings):
#     """Application settings with production-grade validation."""
    
#     model_config = SettingsConfigDict(
#         env_file=".env",
#         env_file_encoding="utf-8",
#         case_sensitive=False,
#         extra="ignore",
#     )
    
#     # Application
#     APP_NAME: str = "PE Org-AI-R Platform"
#     APP_VERSION: str = "4.0.0"
#     APP_ENV: Literal["development", "staging", "production"] = "development"
#     DEBUG: bool = False
#     LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
#     LOG_FORMAT: Literal["json", "console"] = "json"
#     SECRET_KEY: SecretStr
    
#     # API
#     API_V1_PREFIX: str = "/api/v1"
#     RATE_LIMIT_PER_MINUTE: int = Field(default=60, ge=1, le=1000)
    
#     # Parameter Version
#     PARAM_VERSION: Literal["v1.0", "v2.0"] = "v2.0"
    
#     # Snowflake
#     SNOWFLAKE_ACCOUNT: str
#     SNOWFLAKE_USER: str
#     SNOWFLAKE_PASSWORD: SecretStr
#     SNOWFLAKE_DATABASE: str 
#     SNOWFLAKE_SCHEMA: str 
#     SNOWFLAKE_WAREHOUSE: str 
#     SNOWFLAKE_ROLE: str 
    
#     # AWS S3
#     AWS_ACCESS_KEY_ID: SecretStr
#     AWS_SECRET_ACCESS_KEY: SecretStr
#     AWS_REGION: str = "us-east-2"
#     S3_BUCKET: str
    
#     # SEC EDGAR Configuration (NEW)
#     SEC_USER_AGENT: str = Field(
#         default="MyCompany admin@mycompany.com",
#         description="SEC requires a valid User-Agent with company name and email"
#     )
#     SEC_RATE_LIMIT: int = Field(
#         default=10, 
#         ge=1, 
#         le=10,
#         description="SEC limits to 10 requests per second"
#     )
#     # BuiltWith Free API
#     BUILTWITH_API_KEY: Optional[str] = Field(
#         default=None,
#         description="BuiltWith Free API key for tech stack detection"
#     )

#     # Redis
#     REDIS_URL: str = "redis://localhost:6379/0"
#     CACHE_TTL_SECTORS: int = 86400  # 24 hours
#     CACHE_TTL_SCORES: int = 3600    # 1 hour
    
#     # LLM Providers (Multi-provider via LiteLLM)
#     OPENAI_API_KEY: Optional[SecretStr] = None
#     ANTHROPIC_API_KEY: Optional[SecretStr] = None
#     DEFAULT_LLM_MODEL: str = "gpt-4o-2024-08-06"
#     FALLBACK_LLM_MODEL: str = "claude-sonnet-4-20250514"
    
#     # Cost Management
#     DAILY_COST_BUDGET_USD: float = Field(default=500.0, ge=0)
#     COST_ALERT_THRESHOLD_PCT: float = Field(default=0.8, ge=0, le=1)
    
#     # Scoring Parameters (v2.0)
#     ALPHA_VR_WEIGHT: float = Field(default=0.60, ge=0.55, le=0.70)
#     BETA_SYNERGY_WEIGHT: float = Field(default=0.12, ge=0.08, le=0.20)
#     LAMBDA_PENALTY: float = Field(default=0.25, ge=0, le=0.50)
#     DELTA_POSITION: float = Field(default=0.15, ge=0.10, le=0.20)
    
#     # Dimension Weights
#     W_DATA_INFRA: float = Field(default=0.18, ge=0.0, le=1.0)
#     W_AI_GOVERNANCE: float = Field(default=0.15, ge=0.0, le=1.0)
#     W_TECH_STACK: float = Field(default=0.15, ge=0.0, le=1.0)
#     W_TALENT: float = Field(default=0.17, ge=0.0, le=1.0)
#     W_LEADERSHIP: float = Field(default=0.13, ge=0.0, le=1.0)
#     W_USE_CASES: float = Field(default=0.12, ge=0.0, le=1.0)
#     W_CULTURE: float = Field(default=0.10, ge=0.0, le=1.0)
    
#     # HITL Thresholds
#     HITL_SCORE_CHANGE_THRESHOLD: float = Field(default=15.0, ge=5, le=30)
#     HITL_EBITDA_PROJECTION_THRESHOLD: float = Field(default=10.0, ge=5, le=25)
    
#     # Job Signals Pipeline Constants
#     JOBSPY_REQUEST_DELAY: float = Field(default=6.0, ge=1.0, le=30.0)
#     JOBSPY_DEFAULT_SITES: List[str] = Field(default=["linkedin", "indeed", "glassdoor"])
#     JOBSPY_RESULTS_WANTED: int = Field(default=100, ge=10, le=1000)
#     JOBSPY_HOURS_OLD: int = Field(default=72, ge=1, le=720)
#     JOBSPY_FUZZY_MATCH_THRESHOLD: float = Field(default=75.0, ge=50.0, le=100.0)
#     JOBSPY_AI_SCORE_MULTIPLIER: float = Field(default=15.0, ge=5.0, le=50.0)
#     JOBSPY_RATIO_SCORE_WEIGHT: float = Field(default=50.0, ge=10.0, le=100.0)
#     JOBSPY_VOLUME_BONUS_MAX: float = Field(default=30.0, ge=10.0, le=50.0)
#     JOBSPY_VOLUME_BONUS_MULTIPLIER: float = Field(default=3.0, ge=1.0, le=10.0)
#     JOBSPY_DIVERSITY_SCORE_MAX: float = Field(default=20.0, ge=10.0, le=50.0)
#     JOBSPY_DIVERSITY_SCORE_MULTIPLIER: float = Field(default=2.0, ge=1.0, le=5.0)
#     JOBSPY_MAX_SCORE: float = Field(default=100.0, ge=50.0, le=200.0)
#     JOBSPY_AI_KEYWORDS_THRESHOLD_WITH_DESC: int = Field(default=2, ge=1, le=5)
#     JOBSPY_AI_KEYWORDS_THRESHOLD_NO_DESC: int = Field(default=1, ge=1, le=3)
#     JOBSPY_DIVERSITY_DENOMINATOR: float = Field(default=10.0, ge=5.0, le=20.0)
#     JOBSPY_TOP_KEYWORDS_LIMIT: int = Field(default=20, ge=5, le=50)
    
#     # Celery
#     CELERY_BROKER_URL: str = "redis://localhost:6379/1"
#     CELERY_RESULT_BACKEND: str = "redis://localhost:6379/2"
    
#     # Observability
#     OTEL_EXPORTER_OTLP_ENDPOINT: Optional[str] = None
#     OTEL_SERVICE_NAME: str = "pe-orgair"
    
#     @field_validator("OPENAI_API_KEY")
#     @classmethod
#     def validate_openai_key(cls, v: Optional[SecretStr]) -> Optional[SecretStr]:
#         if v is not None and not v.get_secret_value().startswith("sk-"):
#             raise ValueError("Invalid OpenAI API key format")
#         return v
    
#     @model_validator(mode="after")
#     def validate_dimension_weights(self):
#         """Validate dimension weights sum to 1.0."""
#         weights = [
#             self.W_DATA_INFRA, self.W_AI_GOVERNANCE, self.W_TECH_STACK,
#             self.W_TALENT, self.W_LEADERSHIP, self.W_USE_CASES, self.W_CULTURE
#         ]
#         total = sum(weights)
#         if abs(total - 1.0) > 0.001:
#             raise ValueError(f"Dimension weights must sum to 1.0, got {total}")
#         return self
    
#     @model_validator(mode="after")
#     def validate_production_settings(self):
#         """Ensure production has required security settings."""
#         if self.APP_ENV == "production":
#             if self.DEBUG:
#                 raise ValueError("DEBUG must be False in production")
#             if len(self.SECRET_KEY.get_secret_value()) < 32:
#                 raise ValueError("SECRET_KEY must be ≥32 characters in production")
#             if not self.OPENAI_API_KEY and not self.ANTHROPIC_API_KEY:
#                 raise ValueError("At least one LLM API key required in production")
#         return self
    
#     @property
#     def dimension_weights(self) -> List[float]:
#         """Get dimension weights as list."""
#         return [
#             self.W_DATA_INFRA, self.W_AI_GOVERNANCE, self.W_TECH_STACK,
#             self.W_TALENT, self.W_LEADERSHIP, self.W_USE_CASES, self.W_CULTURE
#         ]

# @lru_cache
# def get_settings() -> Settings:
#     return Settings()

# settings = get_settings()


"""Application configuration with comprehensive validation."""
from typing import Optional, Literal, List, Dict
from functools import lru_cache
from decimal import Decimal
from pydantic import Field, field_validator, model_validator, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


# =============================================================================
# COMPANY NAME MAPPINGS
# =============================================================================
# Maps ticker -> search name and aliases for job scraping and fuzzy matching
# - "search": Primary name to use when searching job sites
# - "aliases": All valid variations for fuzzy matching (includes search name)
# - "patent_search": List of assignee names to query on PatentsView API
#     PatentsView treats each exact spelling as a SEPARATE entity.
#     e.g. "Walmart Inc." returns 0 patents; "Walmart Apollo, LLC" returns 1000+
# =============================================================================
# =============================================================================

COMPANY_NAME_MAPPINGS = {
    "CAT": {
        "official": "Caterpillar Inc.",
        "search": "Caterpillar",
        "job_search_names": ["Caterpillar"],
        "patent_search": ["Caterpillar Inc."],
        "domain": "caterpillar.com",
        "aliases": ["Caterpillar", "Caterpillar Inc", "Caterpillar Inc.", "CAT"],
    },
   "DE": {
    "official": "Deere & Company",
    "search": "John Deere",
    "job_search_names": ["John Deere", "Blue River Technology"],
    "patent_search": ["Deere & Company"],
    "domain": "deere.com",
    "aliases": ["John Deere", "Deere", "Deere & Company", "JD",
                "Blue River Technology"],
},
    "UNH": {
        "official": "UnitedHealth Group Incorporated",
        "search": "UnitedHealth",
        "job_search_names": ["UnitedHealth Group", "Optum"],  # Optum is UNH's tech arm
        "patent_search": [
            "UnitedHealth Group Incorporated",
            "Optum, Inc.",
            "Optum Services, Inc.",
        ],
        "domain": "unitedhealthgroup.com",
        "aliases": ["UnitedHealth", "UnitedHealth Group", "United Health",
                     "UnitedHealthcare", "UHG", "Optum"],
    },
    "HCA": {
        "official": "HCA Healthcare, Inc.",
        "search": "HCA Healthcare",
        "job_search_names": ["HCA Healthcare"],
        "patent_search": ["HCA Healthcare, Inc."],
        "domain": "hcahealthcare.com",
        "aliases": ["HCA Healthcare", "HCA", "HCA Inc",
                     "Hospital Corporation of America"],
    },
    "ADP": {
        "official": "Automatic Data Processing, Inc.",
        "search": "ADP",
        "job_search_names": ["ADP"],
        "patent_search": [
            "ADP, Inc.",
            "Automatic Data Processing, Inc.",
        ],
        "domain": "adp.com",
        "aliases": ["ADP", "Automatic Data Processing", "ADP Inc"],
    },
    "PAYX": {
        "official": "Paychex, Inc.",
        "search": "Paychex",
        "job_search_names": ["Paychex"],
        "patent_search": ["Paychex, Inc."],
        "domain": "paychex.com",
        "aliases": ["Paychex", "Paychex Inc", "Paychex Inc."],
    },
    "WMT": {
        "official": "Walmart Inc.",
        "search": "Walmart",
        "job_search_names": ["Walmart"],
        "patent_search": [
            "Walmart Apollo, LLC",
            "Wal-Mart Stores, Inc.",
        ],
        "domain": "walmart.com",
        "aliases": ["Walmart", "Walmart Inc", "Walmart Inc.",
                     "Wal-Mart", "Wal Mart"],
    },
    "TGT": {
        "official": "Target Corporation",
        "search": "Target",                     # FIXED: was "Target Corporation"
        "job_search_names": ["Target"],          # Job boards list as just "Target"
        "patent_search": ["Target Brands, Inc."],
        "domain": "target.com",
        "aliases": ["Target", "Target Corporation", "Target Corp"],
    },
    "JPM": {
        "official": "JPMorgan Chase & Co.",
        "search": "JPMorgan Chase",
        "job_search_names": ["JPMorgan Chase"],
        "patent_search": ["JPMorgan Chase Bank, N.A."],
        "domain": "jpmorganchase.com",
        "aliases": ["JPMorgan Chase", "JPMorgan", "JP Morgan",
                     "Chase", "J.P. Morgan", "JPMC"],
    },
    "GS": {
        "official": "The Goldman Sachs Group, Inc.",
        "search": "Goldman Sachs",
        "job_search_names": ["Goldman Sachs"],
        "patent_search": [
            "Goldman Sachs & Co. LLC",
            "Goldman Sachs & Co.",
        ],
        "domain": "goldmansachs.com",
        "aliases": [
            "Goldman Sachs", "Goldman", "GS", "Goldman Sachs Group",
            "Goldman Sachs & Co", "Goldman Sachs & Co. LLC",
            "Goldman Sachs Bank", "Goldman Sachs Bank USA",
            "Goldman Sachs Asset Management",
            "Goldman Sachs International",
            "Marcus by Goldman Sachs",
        ],
    },
    # =========================================================================
    # CS3-ONLY COMPANIES (NVDA, GE, DG — JPM and WMT already above)
    # =========================================================================
    "NVDA": {
        "official": "NVIDIA Corporation",
        "search": "NVIDIA",
        "job_search_names": ["NVIDIA"],
        "patent_search": ["NVIDIA Corporation"],
        "domain": "nvidia.com",
        "aliases": ["NVIDIA", "NVIDIA Corporation", "Nvidia"],
    },
    "GE": {
        "official": "General Electric Company",
        "search": "GE Aerospace",               # FIXED: was "General Electric"
        "job_search_names": [                    # GE split in 2024 into 3 companies
            "GE Aerospace",                      # PRIMARY — kept GE ticker, most AI/tech jobs
            "GE Vernova",                        # Energy company — also has digital tech roles
        ],
        "patent_search": ["General Electric Company"],
        "domain": "ge.com",
        "aliases": ["General Electric", "GE", "General Electric Company",
                     "GE Aerospace", "GE Vernova", "GE HealthCare"],
    },
    "DG": {
        "official": "Dollar General Corporation",
        "search": "Dollar General",
        "job_search_names": ["Dollar General"],  # Legitimately low tech hiring
        "patent_search": ["Dollar General Corporation"],
        "domain": "dollargeneral.com",
        "aliases": ["Dollar General", "Dollar General Corporation", "DG"],
    },
}



def get_company_search_name(ticker: str) -> Optional[str]:
    """
    Get the search name for a company ticker.

    Args:
        ticker: Company ticker symbol (e.g., "DE", "ADP")

    Returns:
        Search name to use for job sites, or None if not mapped
    """
    ticker = ticker.upper()
    mapping = COMPANY_NAME_MAPPINGS.get(ticker)
    return mapping["search"] if mapping else None


def get_job_search_names(ticker: str) -> List[str]:
    """
    Get ALL job search names for a company.

    Returns a list of search terms to try on job boards.
    For most companies this is a single name, but for companies
    that split (GE) or have tech subsidiaries (UNH/Optum),
    multiple names are returned.

    The pipeline should search each name and combine + deduplicate results.
    """
    ticker = ticker.upper()
    mapping = COMPANY_NAME_MAPPINGS.get(ticker)
    if not mapping:
        return []
    names = mapping.get("job_search_names", [])
    if not names:
        # Fallback to single search name
        search = mapping.get("search")
        return [search] if search else []
    return names

def get_company_aliases(ticker: str) -> List[str]:
    """
    Get all valid name aliases for a company ticker.

    Args:
        ticker: Company ticker symbol

    Returns:
        List of valid name variations for fuzzy matching
    """
    ticker = ticker.upper()
    mapping = COMPANY_NAME_MAPPINGS.get(ticker)
    return mapping["aliases"] if mapping else []


def get_search_name_by_official(official_name: str) -> Optional[str]:
    """
    Get search name from official company name.

    Args:
        official_name: Official company name (e.g., "Deere & Company")

    Returns:
        Search name to use for job sites, or None if not mapped
    """
    official_lower = official_name.lower().strip()
    for ticker, mapping in COMPANY_NAME_MAPPINGS.items():
        if mapping["official"].lower() == official_lower:
            return mapping["search"]
    return None


def get_aliases_by_official(official_name: str) -> List[str]:
    """
    Get all valid name aliases from official company name.

    Args:
        official_name: Official company name

    Returns:
        List of valid name variations for fuzzy matching
    """
    official_lower = official_name.lower().strip()
    for ticker, mapping in COMPANY_NAME_MAPPINGS.items():
        if mapping["official"].lower() == official_lower:
            return mapping["aliases"]
    return []


def get_patent_search_names(ticker: str) -> List[str]:
    """
    Get ALL patent assignee names to search on PatentsView API.

    PatentsView treats each exact assignee spelling as a separate entity.
    This returns a list of all known names to search and combine results.

    Args:
        ticker: Company ticker symbol

    Returns:
        List of assignee organization names to query (deduplicate results!)
    """
    ticker = ticker.upper()
    mapping = COMPANY_NAME_MAPPINGS.get(ticker)
    if not mapping:
        return []
    patent_names = mapping.get("patent_search", [])
    # Handle legacy configs where patent_search might be a single string
    if isinstance(patent_names, str):
        return [patent_names]
    return patent_names


# Keep backward-compatible single-name helper (returns first/primary name)
def get_patent_search_name(ticker: str) -> Optional[str]:
    """
    Get the PRIMARY patent search name for PatentsView API.
    For multi-name search, use get_patent_search_names() instead.
    """
    names = get_patent_search_names(ticker)
    return names[0] if names else None


class Settings(BaseSettings):
    """Application settings with production-grade validation."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    
    # Application
    APP_NAME: str = "PE Org-AI-R Platform"
    APP_VERSION: str = "4.0.0"
    APP_ENV: Literal["development", "staging", "production"] = "development"
    DEBUG: bool = False
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    LOG_FORMAT: Literal["json", "console"] = "json"
    SECRET_KEY: SecretStr
    
    # API
    API_V1_PREFIX: str = "/api/v1"
    RATE_LIMIT_PER_MINUTE: int = Field(default=60, ge=1, le=1000)
    
    # Parameter Version
    PARAM_VERSION: Literal["v1.0", "v2.0"] = "v2.0"
    
    # Snowflake
    SNOWFLAKE_ACCOUNT: str
    SNOWFLAKE_USER: str
    SNOWFLAKE_PASSWORD: SecretStr
    SNOWFLAKE_DATABASE: str 
    SNOWFLAKE_SCHEMA: str 
    SNOWFLAKE_WAREHOUSE: str 
    SNOWFLAKE_ROLE: str 
    
    # AWS S3
    AWS_ACCESS_KEY_ID: SecretStr
    AWS_SECRET_ACCESS_KEY: SecretStr
    AWS_REGION: str = "us-east-2"
    S3_BUCKET: str
    
    # SEC EDGAR Configuration (NEW)
    SEC_USER_AGENT: str = Field(
        default="MyCompany admin@mycompany.com",
        description="SEC requires a valid User-Agent with company name and email"
    )
    SEC_RATE_LIMIT: int = Field(
        default=10, 
        ge=1, 
        le=10,
        description="SEC limits to 10 requests per second"
    )
    # BuiltWith Free API
    BUILTWITH_API_KEY: Optional[str] = Field(
        default=None,
        description="BuiltWith Free API key for tech stack detection"
    )

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    CACHE_TTL_SECTORS: int = 86400  # 24 hours
    CACHE_TTL_SCORES: int = 3600    # 1 hour
    
    # LLM Providers (Multi-provider via LiteLLM)
    OPENAI_API_KEY: Optional[SecretStr] = None
    ANTHROPIC_API_KEY: Optional[SecretStr] = None
    DEFAULT_LLM_MODEL: str = "gpt-4o-2024-08-06"
    FALLBACK_LLM_MODEL: str = "claude-sonnet-4-20250514"
    
    # Cost Management
    DAILY_COST_BUDGET_USD: float = Field(default=500.0, ge=0)
    COST_ALERT_THRESHOLD_PCT: float = Field(default=0.8, ge=0, le=1)
    
    # Scoring Parameters (v2.0)
    ALPHA_VR_WEIGHT: float = Field(default=0.60, ge=0.55, le=0.70)
    BETA_SYNERGY_WEIGHT: float = Field(default=0.12, ge=0.08, le=0.20)
    LAMBDA_PENALTY: float = Field(default=0.25, ge=0, le=0.50)
    DELTA_POSITION: float = Field(default=0.15, ge=0.10, le=0.20)
    
    # Dimension Weights
    W_DATA_INFRA: float = Field(default=0.18, ge=0.0, le=1.0)
    W_AI_GOVERNANCE: float = Field(default=0.15, ge=0.0, le=1.0)
    W_TECH_STACK: float = Field(default=0.15, ge=0.0, le=1.0)
    W_TALENT: float = Field(default=0.17, ge=0.0, le=1.0)
    W_LEADERSHIP: float = Field(default=0.13, ge=0.0, le=1.0)
    W_USE_CASES: float = Field(default=0.12, ge=0.0, le=1.0)
    W_CULTURE: float = Field(default=0.10, ge=0.0, le=1.0)
    
    # HITL Thresholds
    HITL_SCORE_CHANGE_THRESHOLD: float = Field(default=15.0, ge=5, le=30)
    HITL_EBITDA_PROJECTION_THRESHOLD: float = Field(default=10.0, ge=5, le=25)
    
    # Job Signals Pipeline Constants
    JOBSPY_REQUEST_DELAY: float = Field(default=6.0, ge=1.0, le=30.0)
    JOBSPY_DEFAULT_SITES: List[str] = Field(default=["linkedin", "indeed", "glassdoor"])
    JOBSPY_RESULTS_WANTED: int = Field(default=100, ge=10, le=1000)
    JOBSPY_HOURS_OLD: int = Field(default=72, ge=1, le=720)
    JOBSPY_FUZZY_MATCH_THRESHOLD: float = Field(default=75.0, ge=50.0, le=100.0)
    JOBSPY_AI_SCORE_MULTIPLIER: float = Field(default=15.0, ge=5.0, le=50.0)
    JOBSPY_RATIO_SCORE_WEIGHT: float = Field(default=50.0, ge=10.0, le=100.0)
    JOBSPY_VOLUME_BONUS_MAX: float = Field(default=30.0, ge=10.0, le=50.0)
    JOBSPY_VOLUME_BONUS_MULTIPLIER: float = Field(default=3.0, ge=1.0, le=10.0)
    JOBSPY_DIVERSITY_SCORE_MAX: float = Field(default=20.0, ge=10.0, le=50.0)
    JOBSPY_DIVERSITY_SCORE_MULTIPLIER: float = Field(default=2.0, ge=1.0, le=5.0)
    JOBSPY_MAX_SCORE: float = Field(default=100.0, ge=50.0, le=200.0)
    JOBSPY_AI_KEYWORDS_THRESHOLD_WITH_DESC: int = Field(default=2, ge=1, le=5)
    JOBSPY_AI_KEYWORDS_THRESHOLD_NO_DESC: int = Field(default=1, ge=1, le=3)
    JOBSPY_DIVERSITY_DENOMINATOR: float = Field(default=10.0, ge=5.0, le=20.0)
    JOBSPY_TOP_KEYWORDS_LIMIT: int = Field(default=20, ge=5, le=50)
    
    # Celery
    CELERY_BROKER_URL: str = "redis://localhost:6379/1"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/2"
    
    # Observability
    OTEL_EXPORTER_OTLP_ENDPOINT: Optional[str] = None
    OTEL_SERVICE_NAME: str = "pe-orgair"
    
    @field_validator("OPENAI_API_KEY")
    @classmethod
    def validate_openai_key(cls, v: Optional[SecretStr]) -> Optional[SecretStr]:
        if v is not None and not v.get_secret_value().startswith("sk-"):
            raise ValueError("Invalid OpenAI API key format")
        return v
    
    @model_validator(mode="after")
    def validate_dimension_weights(self):
        """Validate dimension weights sum to 1.0."""
        weights = [
            self.W_DATA_INFRA, self.W_AI_GOVERNANCE, self.W_TECH_STACK,
            self.W_TALENT, self.W_LEADERSHIP, self.W_USE_CASES, self.W_CULTURE
        ]
        total = sum(weights)
        if abs(total - 1.0) > 0.001:
            raise ValueError(f"Dimension weights must sum to 1.0, got {total}")
        return self
    
    @model_validator(mode="after")
    def validate_production_settings(self):
        """Ensure production has required security settings."""
        if self.APP_ENV == "production":
            if self.DEBUG:
                raise ValueError("DEBUG must be False in production")
            if len(self.SECRET_KEY.get_secret_value()) < 32:
                raise ValueError("SECRET_KEY must be ≥32 characters in production")
            if not self.OPENAI_API_KEY and not self.ANTHROPIC_API_KEY:
                raise ValueError("At least one LLM API key required in production")
        return self
    
    @property
    def dimension_weights(self) -> List[float]:
        """Get dimension weights as list."""
        return [
            self.W_DATA_INFRA, self.W_AI_GOVERNANCE, self.W_TECH_STACK,
            self.W_TALENT, self.W_LEADERSHIP, self.W_USE_CASES, self.W_CULTURE
        ]

@lru_cache
def get_settings() -> Settings:
    return Settings()

settings = get_settings()