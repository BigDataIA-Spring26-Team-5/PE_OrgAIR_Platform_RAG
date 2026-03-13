"""
Airflow DAG — PE Signal Collection Parent (CS4 Bonus)

Runs nightly at 3 AM and triggers the job signals child DAG.
Scheduled after evidence indexing (2 AM) to avoid overlap.

dag_id: pe_signal_collection_parent
schedule: 0 3 * * *
"""
from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

    class TriggerDagRunOperator:  # type: ignore
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


def log_pipeline_complete(**context):
    """Task 2: Log that the full signal collection pipeline finished."""
    run_id = context.get("run_id", "unknown")
    logical_date = context.get("logical_date") or context.get("execution_date")
    print("=" * 60)
    print("SIGNAL COLLECTION PIPELINE COMPLETE")
    print(f"  run_id        : {run_id}")
    print(f"  logical_date  : {logical_date}")
    print("  child DAGs    : pe_job_signals_collection")
    print("=" * 60)


with DAG(
    dag_id="pe_signal_collection_parent",
    default_args=DEFAULT_ARGS,
    description="Nightly parent DAG that triggers all PE signal collection child DAGs",
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["pe-platform", "cs4", "signals", "parent"],
) as dag:

    t1 = TriggerDagRunOperator(
        task_id="trigger_job_signals",
        trigger_dag_id="pe_job_signals_collection",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    t2 = PythonOperator(
        task_id="log_pipeline_complete",
        python_callable=log_pipeline_complete,
    )

    t1 >> t2
