# """
# AI Keywords and Tech Stack Keywords for Pipeline 2
# app/pipelines/keywords.py
# """

# from __future__ import annotations

# # AI-related keywords for job posting classification
# AI_KEYWORDS = frozenset([
#     # Core AI / ML terms
#     "artificial intelligence",
#     "machine learning",
#     "deep learning",
#     "neural network",
#     "neural networks",
#     "nlp",
#     "natural language processing",
#     "computer vision",
#     "reinforcement learning",
#     "generative ai",
#     "gen ai",
#     "genai",
#     "foundation model",
#     "foundation models",
#     "multimodal",
#     "self supervised learning",
#     "self-supervised learning",

#     # Shorthand terms (important for title-only matching)
#     "ai",
#     "ml",
#     "ai/ml",
#     "ml/ai",
#     "dl",
#     "cv",  # computer vision

#     # Models & architectures
#     "large language model",
#     "large language models",
#     "llm",
#     "llms",
#     "gpt",
#     "gpt-4",
#     "gpt-5",
#     "bert",
#     "t5",
#     "llama",
#     "mistral",
#     "claude",
#     "gemini",
#     "transformer",
#     "transformers",
#     "attention mechanism",
#     "diffusion model",
#     "diffusion models",
#     "stable diffusion",
#     "midjourney",
#     "dall-e",
#     "gan",
#     "gans",
#     "generative adversarial network",
#     "convolutional neural network",
#     "cnn",
#     "rnn",
#     "lstm",
#     "gru",
#     "vae",
#     "autoencoder",

#     # ML techniques
#     "supervised learning",
#     "unsupervised learning",
#     "semi supervised learning",
#     "transfer learning",
#     "fine tuning",
#     "fine-tuning",
#     "finetuning",
#     "post-training",
#     "post training",
#     "pretraining",
#     "pre-training",
#     "rlhf",
#     "hyperparameter tuning",
#     "feature engineering",
#     "model training",
#     "model evaluation",
#     "model deployment",
#     "model serving",
#     "model monitoring",
#     "model optimization",
#     "experiment tracking",
#     "ablation",

#     # Libraries & frameworks
#     "tensorflow",
#     "pytorch",
#     "torch",
#     "keras",
#     "scikit-learn",
#     "sklearn",
#     "hugging face",
#     "huggingface",
#     "transformers",
#     "langchain",
#     "llamaindex",
#     "llama-index",
#     "ray",
#     "mlflow",
#     "onnx",
#     "fastai",
#     "jax",
#     "flax",
#     "triton",
#     "tensorrt",
#     "vllm",
#     "tgi",
#     "deepspeed",
#     "megatron",
#     "nemo",
#     "paddlepaddle",
#     "mxnet",
#     "caffe",

#     # LLM / GenAI tooling
#     "prompt engineering",
#     "prompt engineer",
#     "rag",
#     "retrieval augmented generation",
#     "retrieval-augmented",
#     "vector database",
#     "vector db",
#     "vector search",
#     "embeddings",
#     "embedding",
#     "semantic search",
#     "faiss",
#     "pinecone",
#     "weaviate",
#     "chroma",
#     "milvus",
#     "qdrant",
#     "agents",
#     "agentic",
#     "tool use",
#     "function calling",

#     # Providers & platforms
#     "openai",
#     "anthropic",
#     "cohere",
#     "azure openai",
#     "aws sagemaker",
#     "sagemaker",
#     "google vertex ai",
#     "vertex ai",
#     "bedrock",
#     "replicate",
#     "together ai",
#     "anyscale",
#     "modal",
#     "runpod",
#     "lambda labs",

#     # Data science & analytics
#     "data science",
#     "data scientist",
#     "predictive analytics",
#     "predictive modeling",
#     "statistical modeling",
#     "time series forecasting",
#     "time series",
#     "forecasting",
#     "anomaly detection",
#     "recommendation system",
#     "recommendation engine",
#     "recommender",
#     "personalization",

#     # NLP / Vision tasks
#     "sentiment analysis",
#     "text classification",
#     "named entity recognition",
#     "ner",
#     "topic modeling",
#     "speech recognition",
#     "speech to text",
#     "text to speech",
#     "asr",
#     "tts",
#     "object detection",
#     "image recognition",
#     "image classification",
#     "image segmentation",
#     "semantic segmentation",
#     "ocr",
#     "document ai",
#     "document understanding",

#     # ML roles & job titles
#     "ml engineer",
#     "machine learning engineer",
#     "ai engineer",
#     "mlops",
#     "ml ops",
#     "aiops",
#     "ai ops",
#     "llmops",
#     "ai researcher",
#     "ml researcher",
#     "ai specialist",
#     "applied scientist",
#     "research scientist",
#     "research engineer",
#     "computer vision engineer",
#     "nlp engineer",
#     "data architect",
#     "ml architect",
#     "ai architect",
#     "ml scientist",
#     "ai scientist",

#     # Ops & infra
#     "ml pipeline",
#     "ml pipelines",
#     "training pipeline",
#     "inference pipeline",
#     "model registry",
#     "feature store",
#     "distributed training",
#     "model parallelism",
#     "data parallelism",
#     "tensor parallelism",
#     "pipeline parallelism",
#     "gpu",
#     "gpus",
#     "cuda",
#     "tpu",
#     "inference",
#     "batch inference",
#     "real-time inference",
#     "serving infrastructure",

#     # HPC / Performance (AI context)
#     "hpc",
#     "high performance computing",
#     "accelerated computing",
#     "gpu cluster",
#     "gpu clusters",
# ])

# # Tech stack keywords for AI presence scoring
# AI_TECHSTACK_KEYWORDS = frozenset([
#     # Cloud AI / ML platforms
#     "aws sagemaker",
#     "azure ml",
#     "azure machine learning",
#     "google vertex ai",
#     "aws bedrock",
#     "azure openai",
#     "google ai platform",
#     "databricks",
#     "databricks ml",
#     "snowflake",
#     "snowflake cortex",
#     "bigquery",
#     "redshift",

#     # ML / data infrastructure
#     "kubernetes",
#     "k8s",
#     "docker",
#     "containerization",
#     "mlflow",
#     "kubeflow",
#     "airflow",
#     "apache airflow",
#     "prefect",
#     "dagster",
#     "argo workflows",
#     "argo",

#     # Data processing & streaming
#     "spark",
#     "apache spark",
#     "pyspark",
#     "hadoop",
#     "kafka",
#     "apache kafka",
#     "flink",
#     "apache flink",
#     "beam",
#     "apache beam",
#     "dbt",
#     "fivetran",
#     "airbyte",

#     # Programming languages (AI-heavy usage)
#     "python",
#     "r programming",
#     "r language",
#     "julia",
#     "scala",

#     # Databases & storage
#     "postgresql",
#     "mysql",
#     "mongodb",
#     "elasticsearch",
#     "opensearch",
#     "redis",
#     "neo4j",
#     "cassandra",
#     "dynamodb",
#     "bigtable",

#     # Vector databases / semantic search
#     "vector database",
#     "vector db",
#     "pinecone",
#     "weaviate",
#     "milvus",
#     "qdrant",
#     "chroma",
#     "faiss",

#     # Model serving & inference
#     "model serving",
#     "model inference",
#     "torchserve",
#     "tensorflow serving",
#     "triton inference server",
#     "bentoml",
#     "seldon",
#     "kserve",

#     # Visualization / BI
#     "tableau",
#     "power bi",
#     "looker",
#     "superset",
#     "metabase",

#     # Version control / MLOps tooling
#     "git",
#     "github",
#     "gitlab",
#     "bitbucket",
#     "dvc",
#     "wandb",
#     "weights and biases",
#     "neptune",
#     "clearml",

#     # Experimentation & monitoring
#     "experiment tracking",
#     "model monitoring",
#     "data drift",
#     "concept drift",

#     # AI-specific compute & distributed systems
#     "gpu",
#     "gpu cluster",
#     "nvidia",
#     "cuda",
#     "cudnn",
#     "ray",
#     "dask",
#     "horovod",
#     "distributed training",
# ])

# # Leadership background keywords for DEF 14A parsing
# AI_LEADERSHIP_KEYWORDS = frozenset([
#     # Titles
#     "chief data officer",
#     "cdo",
#     "chief analytics officer",
#     "chief ai officer",
#     "caio",
#     "chief technology officer",
#     "cto",
#     "vp of data",
#     "vp of ai",
#     "vp of engineering",
#     "head of data science",
#     "head of ai",
#     "head of ml",
#     "data science background",
#     "ai experience",
#     "machine learning",
#     "phd in computer science",
#     "stanford ai",
#     "mit ai",
#     "google ai",
#     "meta ai",
#     "deepmind",
#     "openai",
#     "anthropic",
#     "computer science degree",
#     "statistics degree",
#     "mathematics degree",
#     "engineering degree",
# ])

# # Patent classification keywords for PatentsView
# PATENT_AI_KEYWORDS = frozenset([
#     "artificial intelligence",
#     "machine learning",
#     "neural network",
#     "deep learning",
#     "natural language processing",
#     "computer vision",
#     "pattern recognition",
#     "automated decision",
#     "predictive model",
#     "classification algorithm",
#     "clustering algorithm",
#     "recommendation engine",
#     "speech recognition",
#     "image processing",
#     "data mining",
#     "knowledge graph",
# ])

# # Top AI tools for bonus scoring in tech stack
# TOP_AI_TOOLS = frozenset([
#     "tensorflow",
#     "pytorch",
#     "kubernetes",
#     "spark",
#     "databricks",
#     "aws sagemaker",
#     "mlflow",
#     "hugging face",
# ])
# """
# AI Keywords and Tech Stack Keywords for Pipeline 2
# app/pipelines/keywords.py

# ALIGNED WITH CASE STUDY 2 PDF SPEC (pages 14-16).

# Key changes from previous version:
#   - AI_KEYWORDS now uses multi-word phrases to avoid false positives
#   - Removed single-word terms that match addresses/cities/generic business text
#     (e.g. "bert", "modal", "spark", "forecasting", "ray", "agents", "gpu", "ai", "ml")
#   - Added TECH_JOB_TITLE_KEYWORDS for _is_tech_job() filter (PDF page 16)
#   - AI_SKILLS list added per PDF spec (page 15) for diversity scoring
# """

# from __future__ import annotations


# # ---------------------------------------------------------------------------
# # TECH JOB TITLE KEYWORDS (PDF page 16)
# # Used by _is_tech_job() to filter tech roles BEFORE calculating AI ratio.
# # Matched against job TITLE only.
# # ---------------------------------------------------------------------------
# TECH_JOB_TITLE_KEYWORDS = [
#     "engineer",
#     "developer",
#     "programmer",
#     "software",
#     "data",
#     "analyst",
#     "scientist",
#     "technical",
#     "architect",
#     "devops",
#     "sre",
#     "machine learning",
#     "cloud",
#     "platform",
#     "infrastructure",
#     "security",
#     "product manager",
#     "technology",
# ]


# # ---------------------------------------------------------------------------
# # AI KEYWORDS (PDF page 14 — expanded with safe multi-word terms)
# # Used to classify a job posting as AI-related.
# # Matched against title + description.
# #
# # RULE: Every keyword must be specific enough to NOT match:
# #   - Street addresses (e.g. "BERT KOUNS BLVD")
# #   - City/state names (e.g. "Sparks, NV")
# #   - Generic business language (e.g. "budgeting and forecasting")
# #   - Training modalities for truck drivers (e.g. "leveraging different modalities")
# # ---------------------------------------------------------------------------
# AI_KEYWORDS = frozenset([
#     # ---- Core terms from PDF page 14 ----
#     "machine learning",
#     "ml engineer",
#     "data scientist",
#     "artificial intelligence",
#     "deep learning",
#     "natural language processing",
#     "computer vision",
#     "mlops",
#     "ai engineer",
#     "pytorch",
#     "tensorflow",
#     "large language model",
#     "large language models",

#     # ---- Safe multi-word expansions ----
#     # Core AI/ML
#     "neural network",
#     "neural networks",
#     "reinforcement learning",
#     "generative ai",
#     "foundation model",
#     "foundation models",
#     "self-supervised learning",

#     # Specific model names (multi-word or unambiguous)
#     "large language model",
#     "stable diffusion",
#     "generative adversarial network",
#     "convolutional neural network",
#     "recurrent neural network",

#     # ML techniques (multi-word only)
#     "supervised learning",
#     "unsupervised learning",
#     "semi-supervised learning",
#     "transfer learning",
#     "fine-tuning",
#     "model training",
#     "model deployment",
#     "model serving",
#     "hyperparameter tuning",
#     "feature engineering",
#     "distributed training",
#     "prompt engineering",
#     "retrieval augmented generation",

#     # Specific frameworks/tools (unambiguous)
#     "scikit-learn",
#     "hugging face",
#     "huggingface",
#     "langchain",
#     "llamaindex",
#     "llama-index",
#     "aws sagemaker",
#     "azure openai",
#     "google vertex ai",
#     "vertex ai",

#     # Roles (multi-word)
#     "machine learning engineer",
#     "ai researcher",
#     "ml researcher",
#     "applied scientist",
#     "research scientist",
#     "research engineer",
#     "computer vision engineer",
#     "nlp engineer",
#     "ml architect",
#     "ai architect",
#     "data science",
#     "ai specialist",

#     # NLP/Vision tasks (multi-word)
#     "sentiment analysis",
#     "named entity recognition",
#     "object detection",
#     "image recognition",
#     "image classification",
#     "image segmentation",
#     "speech recognition",
#     "text classification",
#     "topic modeling",
#     "anomaly detection",
#     "recommendation system",
#     "recommendation engine",
#     "predictive modeling",
#     "predictive analytics",
#     "time series forecasting",

#     # Infrastructure (multi-word, unambiguous)
#     "vector database",
#     "vector search",
#     "semantic search",
#     "ml pipeline",
#     "ml pipelines",
#     "training pipeline",
#     "inference pipeline",
#     "feature store",
#     "model registry",
#     "gpu cluster",
#     "gpu clusters",
#     "batch inference",
#     "real-time inference",
#     "serving infrastructure",
#     "experiment tracking",
# ])


# # ---------------------------------------------------------------------------
# # AI SKILLS (PDF page 15)
# # Used for diversity scoring — counts unique skills found across all postings.
# # These are specific tool/technology names unlikely to cause false positives.
# # ---------------------------------------------------------------------------
# AI_SKILLS = frozenset([
#     "python",
#     "pytorch",
#     "tensorflow",
#     "scikit-learn",
#     "spark",
#     "hadoop",
#     "kubernetes",
#     "docker",
#     "aws sagemaker",
#     "azure ml",
#     "gcp vertex",
#     "huggingface",
#     "langchain",
#     "openai",
# ])


# # ---------------------------------------------------------------------------
# # AI TECHSTACK KEYWORDS
# # Used by tech_signals.py for digital_presence scoring.
# # These are scanned in job descriptions for tech stack evidence.
# # Keep specific multi-word terms where possible.
# # ---------------------------------------------------------------------------
# AI_TECHSTACK_KEYWORDS = frozenset([
#     # Cloud AI/ML platforms
#     "aws sagemaker",
#     "azure ml",
#     "azure machine learning",
#     "google vertex ai",
#     "aws bedrock",
#     "azure openai",
#     "databricks",
#     "snowflake",
#     "snowflake cortex",
#     "bigquery",

#     # ML/data infrastructure
#     "kubernetes",
#     "docker",
#     "mlflow",
#     "kubeflow",
#     "apache airflow",
#     "prefect",
#     "dagster",

#     # Data processing
#     "apache spark",
#     "pyspark",
#     "apache kafka",
#     "apache flink",
#     "apache beam",

#     # Programming languages
#     "python",
#     "scala",
#     "julia",

#     # Databases
#     "postgresql",
#     "mongodb",
#     "elasticsearch",
#     "redis",
#     "neo4j",

#     # Vector databases
#     "pinecone",
#     "weaviate",
#     "milvus",
#     "qdrant",
#     "chroma",

#     # Model serving
#     "triton inference server",
#     "torchserve",
#     "tensorflow serving",
#     "bentoml",

#     # Visualization/BI
#     "tableau",
#     "power bi",
#     "looker",
# ])


# # ---------------------------------------------------------------------------
# # Leadership keywords for DEF 14A parsing
# # ---------------------------------------------------------------------------
# AI_LEADERSHIP_KEYWORDS = frozenset([
#     "chief data officer",
#     "chief analytics officer",
#     "chief ai officer",
#     "chief technology officer",
#     "vp of data",
#     "vp of ai",
#     "vp of engineering",
#     "head of data science",
#     "head of ai",
#     "head of ml",
#     "machine learning",
#     "phd in computer science",
#     "data science background",
#     "ai experience",
#     "computer science degree",
# ])


# # ---------------------------------------------------------------------------
# # Patent classification keywords for PatentsView
# # ---------------------------------------------------------------------------
# PATENT_AI_KEYWORDS = frozenset([
#     "artificial intelligence",
#     "machine learning",
#     "neural network",
#     "deep learning",
#     "natural language processing",
#     "computer vision",
#     "pattern recognition",
#     "automated decision",
#     "predictive model",
#     "classification algorithm",
#     "clustering algorithm",
#     "recommendation engine",
#     "speech recognition",
#     "image processing",
#     "data mining",
#     "knowledge graph",
# ])


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