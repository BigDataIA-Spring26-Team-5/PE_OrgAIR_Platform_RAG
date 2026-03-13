"""
Airflow DAG — PE Job Signals Collection (CS4 Bonus)

Child DAG triggered by pe_signal_collection_parent.
Fetches all companies and scores technology hiring signals via JobSpy.

dag_id: pe_job_signals_collection
schedule: None (triggered by parent only)
"""
from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    _AIRFLOW_AVAILABLE = True
except ImportError:
    _AIRFLOW_AVAILABLE = False
    # Stub classes for import-time parsing without Airflow installed
    class DAG:  # type: ignore
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass

    class PythonOperator:  # type: ignore
        def __init__(self, *a, **kw): pass
        def __rshift__(self, other): return other


DEFAULT_ARGS = {
    "owner": "pe-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def fetch_companies(**context):
    """Task 1: Fetch all company tickers from the PE platform API."""
    import httpx

    base_url = "http://localhost:8000"
    resp = httpx.get(f"{base_url}/api/v1/companies/all", timeout=60.0)
    if resp.status_code != 200:
        print(f"Warning: companies endpoint returned {resp.status_code}")
        tickers = []
    else:
        data = resp.json()
        companies = data if isinstance(data, list) else data.get("companies", [])
        tickers = [c.get("ticker") or c.get("symbol") or c for c in companies if c]
        tickers = [t for t in tickers if isinstance(t, str) and t]

    print(f"Fetched {len(tickers)} company tickers: {tickers}")
    context["ti"].xcom_push(key="tickers", value=tickers)
    return len(tickers)


def score_hiring_signals(**context):
    """Task 2: Call hiring signal endpoint for each company ticker."""
    import httpx

    tickers = context["ti"].xcom_pull(key="tickers", task_ids="fetch_companies")
    if not tickers:
        print("No tickers to score.")
        context["ti"].xcom_push(key="hiring_results", value={})
        return 0

    base_url = "http://localhost:8000"
    results = {}

    for ticker in tickers:
        try:
            resp = httpx.post(
                f"{base_url}/api/v1/signals/score/{ticker}/hiring",
                timeout=300.0,
            )
            if resp.status_code != 200:
                print(f"[{ticker}] HTTP {resp.status_code} — skipping")
                results[ticker] = None
                continue

            data = resp.json()
            status = data.get("status")

            if status == "skipped":
                print(f"[{ticker}] Already scored today — skipping")
                results[ticker] = data.get("score")
            elif status == "success":
                score = data.get("score")
                results[ticker] = score
                print(f"[{ticker}] score={score:.4f}" if score is not None else f"[{ticker}] score=None")
            else:
                print(f"[{ticker}] status={status} error={data.get('error')}")
                results[ticker] = None

        except Exception as exc:
            print(f"[{ticker}] Exception: {exc}")
            results[ticker] = None

    scored = sum(1 for v in results.values() if v is not None)
    print(f"Scored {scored}/{len(tickers)} tickers successfully")
    context["ti"].xcom_push(key="hiring_results", value=results)
    return scored


def log_summary(**context):
    """Task 3: Print per-ticker scores and a pipeline summary line."""
    results = context["ti"].xcom_pull(key="hiring_results", task_ids="score_hiring_signals")
    if not results:
        print("No hiring results to summarize.")
        return

    print("=" * 60)
    print("HIRING SIGNAL SCORES")
    print("=" * 60)
    for ticker, score in sorted(results.items()):
        score_str = f"{score:.4f}" if score is not None else "N/A"
        print(f"  {ticker:<10} {score_str}")

    total = len(results)
    scored = sum(1 for v in results.values() if v is not None)
    failed = total - scored
    print("=" * 60)
    print(f"SUMMARY: {scored}/{total} scored, {failed} failed/skipped")
    print("=" * 60)


with DAG(
    dag_id="pe_job_signals_collection",
    default_args=DEFAULT_ARGS,
    description="Collect technology hiring signals for all PE portfolio companies",
    schedule_interval=None,
    catchup=False,
    tags=["pe-platform", "cs4", "signals", "jobs", "child"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_companies",
        python_callable=fetch_companies,
    )

    t2 = PythonOperator(
        task_id="score_hiring_signals",
        python_callable=score_hiring_signals,
    )

    t3 = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
    )

    t1 >> t2 >> t3
