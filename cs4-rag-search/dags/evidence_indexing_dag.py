"""Automated evidence indexing pipeline."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import structlog

logger = structlog.get_logger()

default_args = {
    "owner": "pe-analytics",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

dag = DAG(
    dag_id="pe_evidence_indexing",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # 2 AM daily
    start_date=datetime(2026, 2, 20),
    catchup=False,
)

def fetch_new_evidence(**context):
    """Fetch unindexed evidence from CS2."""
    import httpx
    response = httpx.get("http://cs2-api:8001/api/v1/evidence", params={"indexed": False})
    evidence = response.json()
    context["ti"].xcom_push(key="evidence", value=evidence)
    return len(evidence)

def index_evidence(**context):
    """Index evidence in vector store."""
    from services.retrieval.hybrid import HybridRetriever
    from services.retrieval.dimension_mapper import DimensionMapper

    evidence = context["ti"].xcom_pull(key="evidence", task_ids="fetch_evidence")
    if not evidence:
        return 0

    mapper = DimensionMapper()
    retriever = HybridRetriever()

    docs = []
    for e in evidence:
        primary_dim = mapper.get_primary_dimension(e["signal_category"])
        docs.append({
            "doc_id": e["evidence_id"],
            "content": e["content"],
            "metadata": {
                "company_id": e["company_id"],
                "source_type": e["source_type"],
                "dimension": primary_dim,
                "confidence": e["confidence"],
            }
        })

    return retriever.index_documents(docs)

t1 = PythonOperator(task_id="fetch_evidence", python_callable=fetch_new_evidence, dag=dag)
t2 = PythonOperator(task_id="index_evidence", python_callable=index_evidence, dag=dag)
t1 >> t2
