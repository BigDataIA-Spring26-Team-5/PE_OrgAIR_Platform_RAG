"""
AI Keywords and Tech Stack Keywords for Pipeline 2
app/pipelines/keywords.py

ALIGNED WITH:
  - CS2 PDF pages 14-16 (job keywords, skills, tech job filter)
  - CS2 PDF page 18 (patent keywords)
  - CS3 PDF pages 11-13 (rubric keywords for 7 dimensions)
  - CS3 PDF page 16 (Glassdoor culture keywords)

Changes (v3):
  - AI_KEYWORDS split into AI_KEYWORDS_STRONG and AI_KEYWORDS_CONTEXTUAL
    to reduce false positives from boilerplate "about us" mentions.
    STRONG: single match anywhere → AI role.
    CONTEXTUAL: must appear in job TITLE or 2+ times in description.
  - AI_KEYWORDS remains as the union for backward compat / diversity scoring.
  - PATENT_AI_CATEGORIES: "image" replaced with specific AI-image phrases
    to avoid false positives like "capture images of products".
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# TECH JOB TITLE KEYWORDS (CS2 PDF page 16)
# Used by _is_tech_job() to filter tech roles BEFORE calculating AI ratio.
# Matched against job TITLE only.
# ---------------------------------------------------------------------------
TECH_JOB_TITLE_KEYWORDS = [
    # --- CS2 PDF page 16 (exact) ---
    "engineer",
    "developer",
    "programmer",
    "software",
    "data",
    "analyst",
    "scientist",
    "technical",
    # --- Safe additions ---
    "architect",
    "devops",
    "sre",
    "machine learning",
    "cloud",
    "platform",
    "infrastructure",
    "security",
    "product manager",
    "technology",
]


# ---------------------------------------------------------------------------
# AI KEYWORDS — TWO-TIER SYSTEM
#
# STRONG: Unambiguous AI terms. A single mention anywhere in
#   title + description is sufficient to classify as an AI role.
#   These never appear in generic boilerplate paragraphs.
#
# CONTEXTUAL: Terms that CAN appear in "about us" boilerplate
#   (e.g. "we're a team of software engineers, data scientists...").
#   Must appear in the TITLE, or appear 2+ times in description,
#   to classify as AI. A single passing mention doesn't count.
#
# AI_KEYWORDS = union of both, kept for backward compat and
#   diversity scoring where we just need the full keyword set.
# ---------------------------------------------------------------------------

AI_KEYWORDS_STRONG = frozenset([
    # ---- CS2 PDF page 14 (unambiguous subset) ----
    "machine learning",
    "ml engineer",
    "artificial intelligence",
    "deep learning",
    "computer vision",
    "mlops",
    "ai engineer",
    "large language model",
    "large language models",

    # Core AI/ML
    "neural network",
    "neural networks",
    "reinforcement learning",
    "generative ai",
    "foundation model",
    "foundation models",
    "natural language processing",
    "self-supervised learning",

    # Specific model architectures (multi-word)
    "stable diffusion",
    "generative adversarial network",
    "convolutional neural network",
    "recurrent neural network",

    # ML techniques (multi-word)
    "supervised learning",
    "unsupervised learning",
    "semi-supervised learning",
    "transfer learning",
    "fine-tuning",
    "model training",
    "model deployment",
    "model serving",
    "hyperparameter tuning",
    "feature engineering",
    "distributed training",
    "prompt engineering",
    "retrieval augmented generation",

    # Specific frameworks/tools (unambiguous)
    "scikit-learn",
    "hugging face",
    "huggingface",
    "langchain",
    "llamaindex",
    "llama-index",
    "aws sagemaker",
    "azure openai",
    "google vertex ai",
    "vertex ai",

    # Roles (multi-word, unambiguous)
    "machine learning engineer",
    "ai researcher",
    "ml researcher",
    "computer vision engineer",
    "nlp engineer",
    "ml architect",
    "ai architect",
    "ai specialist",

    # NLP/Vision tasks (multi-word)
    "sentiment analysis",
    "named entity recognition",
    "object detection",
    "image recognition",
    "image classification",
    "image segmentation",
    "speech recognition",
    "text classification",
    "topic modeling",
    "anomaly detection",
    "recommendation system",
    "recommendation engine",
    "predictive modeling",
    "predictive analytics",
    "time series forecasting",

    # Infrastructure (multi-word)
    "vector database",
    "vector search",
    "semantic search",
    "ml pipeline",
    "ml pipelines",
    "training pipeline",
    "inference pipeline",
    "feature store",
    "model registry",
    "gpu cluster",
    "gpu clusters",
    "batch inference",
    "real-time inference",
    "serving infrastructure",
    "experiment tracking",
])

AI_KEYWORDS_CONTEXTUAL = frozenset([
    # ---- CS2 PDF page 14 terms that appear in boilerplate ----
    # "data scientist" appears in generic "about us" paragraphs:
    #   "we're a team of software engineers, data scientists..."
    # "pytorch"/"tensorflow" appear in shared-infra descriptions.
    # These require TITLE match or 2+ description mentions.
    "data scientist",
    "data science",
    "applied scientist",
    "research scientist",
    "research engineer",
    "nlp",
    "llm",
    "pytorch",
    "tensorflow",
])

# Combined set — used for diversity scoring and backward compat
AI_KEYWORDS = AI_KEYWORDS_STRONG | AI_KEYWORDS_CONTEXTUAL


# ---------------------------------------------------------------------------
# AI SKILLS (CS2 PDF page 15 — EXACT)
# Used for diversity scoring — counts unique skills found across all postings.
# Formula: min(len(skills) / 10, 1) * 20  (max 20 pts)
# ---------------------------------------------------------------------------
AI_SKILLS = frozenset([
    "python",
    "pytorch",
    "tensorflow",
    "scikit-learn",
    "spark",
    "hadoop",
    "kubernetes",
    "docker",
    "aws sagemaker",
    "azure ml",
    "gcp vertex",
    "huggingface",
    "langchain",
    "openai",
])


# ---------------------------------------------------------------------------
# AI TECHSTACK KEYWORDS
# Used by tech_signals.py for digital_presence scoring.
# Scanned in job descriptions for tech stack evidence.
# ---------------------------------------------------------------------------
AI_TECHSTACK_KEYWORDS = frozenset([
    # Cloud AI/ML platforms
    "aws sagemaker",
    "azure ml",
    "azure machine learning",
    "google vertex ai",
    "aws bedrock",
    "azure openai",
    "databricks",
    "snowflake",
    "snowflake cortex",
    "bigquery",

    # ML/data infrastructure
    "kubernetes",
    "docker",
    "mlflow",
    "kubeflow",
    "apache airflow",
    "prefect",
    "dagster",

    # Data processing
    "apache spark",
    "pyspark",
    "apache kafka",
    "apache flink",
    "apache beam",

    # Programming languages
    "python",
    "scala",
    "julia",

    # Databases
    "postgresql",
    "mongodb",
    "elasticsearch",
    "redis",
    "neo4j",

    # Vector databases
    "pinecone",
    "weaviate",
    "milvus",
    "qdrant",
    "chroma",

    # Model serving
    "triton inference server",
    "torchserve",
    "tensorflow serving",
    "bentoml",

    # Visualization/BI
    "tableau",
    "power bi",
    "looker",
])


# ---------------------------------------------------------------------------
# PATENT AI KEYWORDS (CS2 PDF page 18)
# Used by patent_signals.py classify_patent().
# CS2 PDF specifies 9 exact phrases. We keep those as the core set
# and add safe expansions that won't cause false positives in patent text.
# ---------------------------------------------------------------------------
PATENT_AI_KEYWORDS = frozenset([
    # --- CS2 PDF page 18 (exact 9 keywords) ---
    "machine learning",
    "neural network",
    "deep learning",
    "artificial intelligence",
    "natural language processing",
    "computer vision",
    "reinforcement learning",
    "predictive model",
    "classification algorithm",

    # --- Safe expansions ---
    "pattern recognition",
    "automated decision",
    "clustering algorithm",
    "recommendation engine",
    "speech recognition",
    "image processing",
    "data mining",
    "knowledge graph",
])


# ---------------------------------------------------------------------------
# PATENT AI CATEGORIES (CS2 PDF page 19 lines 86-94)
# Used by patent_signals.py classify_patent() for category assignment.
# CS2 PDF specifies exactly 4 categories.
#
# FIX (v3): "computer_vision" no longer uses bare "image" as a trigger.
# Bare "image" causes false positives on e-commerce patents like
# "capture images of products in a retail facility".
# Now requires specific AI-image phrases.
# ---------------------------------------------------------------------------
PATENT_AI_CATEGORIES = {
    "deep_learning": ["neural network", "deep learning"],
    "nlp": ["natural language"],
    "computer_vision": [
        "computer vision",
        "image recognition",
        "image classification",
        "image segmentation",
        "image processing",
        "image analysis",
        "object detection",
        "visual recognition",
        "convolutional neural",
    ],
    "predictive_analytics": ["predictive"],
}


# ---------------------------------------------------------------------------
# PATENT AI CPC CLASSES (CS2 PDF page 18 lines 28-32)
# ---------------------------------------------------------------------------
PATENT_AI_CLASSES = [
    "706",  # Data processing: AI
    "382",  # Image analysis
    "704",  # Speech processing
]


# ---------------------------------------------------------------------------
# AI LEADERSHIP KEYWORDS
# Used by leadership_analyzer.py for DEF 14A parsing.
# ---------------------------------------------------------------------------
AI_LEADERSHIP_KEYWORDS = frozenset([
    "chief data officer",
    "chief analytics officer",
    "chief ai officer",
    "chief technology officer",
    "vp of data",
    "vp of ai",
    "vp of engineering",
    "head of data science",
    "head of ai",
    "head of ml",
    "machine learning",
    "phd in computer science",
    "data science background",
    "ai experience",
    "computer science degree",
])


# ---------------------------------------------------------------------------
# CS3 RUBRIC KEYWORDS (CS3 PDF pages 11-13)
# Used by scoring/rubric_scorer.py to score 7 dimensions.
# Each dimension has keywords per level (5=best, 1=worst).
# ---------------------------------------------------------------------------
CS3_RUBRIC_KEYWORDS = {
    "data_infrastructure": {
        5: ["snowflake", "databricks", "lakehouse", "real-time", "api-first"],
        4: ["azure", "aws", "warehouse", "etl"],
        3: ["migration", "hybrid", "modernizing"],
        2: ["legacy", "silos", "on-premise"],
        1: ["mainframe", "spreadsheets", "manual"],
    },
    "ai_governance": {
        5: ["caio", "cdo", "board committee", "model risk"],
        4: ["vp data", "ai policy", "risk framework"],
        3: ["director", "guidelines", "it governance"],
        2: ["informal", "no policy", "ad-hoc"],
        1: ["none", "no oversight", "unmanaged"],
    },
    "technology_stack": {
        5: ["sagemaker", "mlops", "feature store"],
        4: ["mlflow", "kubeflow", "databricks ml"],
        3: ["jupyter", "notebooks", "manual deploy"],
        2: ["excel", "tableau only", "no ml"],
        1: ["manual", "no tools"],
    },
    "talent": {
        5: ["ml platform", "ai research", "large team", ">20 specialists"],
        4: ["data science team", "ml engineers", "10-20", "active hiring", "retention"],
        3: ["data scientist", "growing team"],
        2: ["junior", "contractor", "turnover"],
        1: ["no data scientist", "vendor only"],
    },
    "leadership": {
        5: ["ceo ai", "board committee", "ai strategy"],
        4: ["cto ai", "strategic priority"],
        3: ["vp sponsor", "department initiative"],
        2: ["it led", "limited awareness"],
        1: ["no sponsor", "not discussed"],
    },
    "use_case_portfolio": {
        5: ["production ai", "3x roi", "ai product"],
        4: ["production", "measured roi", "scaling"],
        3: ["pilot", "early production"],
        2: ["poc", "proof of concept"],
        1: ["exploring", "no use cases"],
    },
    "culture": {
        5: ["innovative", "data-driven", "fail-fast"],
        4: ["experimental", "learning culture"],
        3: ["open to change", "some resistance"],
        2: ["bureaucratic", "resistant", "slow"],
        1: ["hostile", "siloed", "no data culture"],
    },
}


# ---------------------------------------------------------------------------
# CS3 GLASSDOOR CULTURE KEYWORDS (CS3 PDF page 16, Table 2)
# Used by pipelines/glassdoor_collector.py
# ---------------------------------------------------------------------------
CS3_CULTURE_KEYWORDS = {
    "innovation_positive": [
        "innovative", "cutting-edge", "forward-thinking",
        "encourages new ideas", "experimental", "creative freedom",
        "startup mentality", "move fast", "disruptive",
    ],
    "innovation_negative": [
        "bureaucratic", "slow to change", "resistant",
        "outdated", "stuck in old ways", "red tape",
        "politics", "siloed", "hierarchical",
    ],
    "data_driven": [
        "data-driven", "metrics", "evidence-based",
        "analytical", "kpis", "dashboards", "data culture",
        "measurement", "quantitative",
    ],
    "ai_awareness": [
        "ai", "artificial intelligence", "machine learning",
        "automation", "data science", "ml", "algorithms",
        "predictive", "neural network",
    ],
    "change_positive": [
        "agile", "adaptive", "fast-paced", "embraces change",
        "continuous improvement", "growth mindset",
    ],
    "change_negative": [
        "rigid", "traditional", "slow", "risk-averse",
        "change resistant", "old school",
    ],
}


# ---------------------------------------------------------------------------
# CS3 BOARD GOVERNANCE KEYWORDS (CS3 PDF page 20, Table 3)
# Used by pipelines/board_analyzer.py
# ---------------------------------------------------------------------------
CS3_BOARD_KEYWORDS = {
    "ai_expertise": [
        "artificial intelligence", "machine learning",
        "chief data officer", "cdo", "caio", "chief ai",
        "chief technology", "cto", "chief digital",
        "data science", "analytics", "digital transformation",
    ],
    "tech_committee": [
        "technology committee", "digital committee",
        "innovation committee", "it committee",
        "technology and cybersecurity",
    ],
    "data_officer_titles": [
        "chief data officer", "cdo",
        "chief ai officer", "caio",
        "chief analytics officer", "cao",
        "chief digital officer",
    ],
}