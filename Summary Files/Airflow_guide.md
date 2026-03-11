# Airflow Setup Guide — PE Org-AI-R Platform

## Overview
This guide covers everything needed to make Airflow work correctly
with the PE Org-AI-R platform, including the mark_indexed fix.

---

## Step 1: Fix mark_indexed in cs2_client.py

The current `mark_indexed()` is a no-op. We need it to persist
indexed state to S3 so Airflow doesn't re-index the same evidence
every night.

### Change 1: Add these two methods to CS2Client

```python
def mark_indexed(self, evidence_ids: List[str]) -> int:
    """Mark evidence IDs as indexed by writing state to S3."""
    if not evidence_ids:
        return 0

    # Group by ticker (company_id is ticker in your implementation)
    from collections import defaultdict
    by_ticker: dict = defaultdict(list)
    for eid in evidence_ids:
        # evidence_id format: {type}_{ticker}_{hash} or similar
        # store all under a general index state file per ticker
        ticker = eid.split("_")[1] if "_" in eid else "unknown"
        by_ticker[ticker].append(eid)

    total = 0
    for ticker, ids in by_ticker.items():
        s3_key = f"indexed_state/{ticker}.json"
        # Load existing state
        existing = self._load_indexed_state(ticker)
        existing.update(ids)
        # Save updated state
        try:
            self._s3.upload_json({"indexed_ids": list(existing)}, s3_key)
            total += len(ids)
        except Exception as e:
            logger.warning("mark_indexed_failed ticker=%s error=%s", ticker, e)
    return total

def _load_indexed_state(self, ticker: str) -> set:
    """Load set of already-indexed evidence IDs from S3."""
    s3_key = f"indexed_state/{ticker}.json"
    try:
        raw = self._s3.get_file(s3_key)
        if raw:
            data = json.loads(raw)
            return set(data.get("indexed_ids", []))
    except Exception:
        pass
    return set()
```

### Change 2: Update get_evidence() to respect indexed filter

Add this at the end of `get_evidence()` before returning `result`:

```python
# Filter by indexed state if requested
if indexed is not None:
    already_indexed = self._load_indexed_state(resolved_ticker)
    if indexed is False:
        # Only return NOT yet indexed
        result = [e for e in result if e.evidence_id not in already_indexed]
    elif indexed is True:
        # Only return already indexed
        result = [e for e in result if e.evidence_id in already_indexed]
```

---

## Step 2: Update evidence_indexing_dag.py

Replace the HTTP-based fetch with direct CS2Client calls
(works better with your S3-based architecture):

```python
def fetch_evidence(**context):
    """Task 1: Fetch unindexed evidence using CS2Client directly."""
    import sys
    sys.path.insert(0, "/app")  # adjust to your project root

    from app.services.integration.cs2_client import CS2Client

    # Get tickers to process — reads from env or defaults
    import os
    tickers_env = os.getenv("AIRFLOW_TICKERS", "NVDA,JPM,WMT,GE,DG")
    tickers = [t.strip() for t in tickers_env.split(",")]

    cs2 = CS2Client()
    all_evidence = []

    for ticker in tickers:
        try:
            evidence = cs2.get_evidence(ticker=ticker, indexed=False)
            all_evidence.extend([{
                "evidence_id": e.evidence_id,
                "company_id": e.company_id,
                "source_type": e.source_type,
                "signal_category": e.signal_category,
                "content": e.content,
                "confidence": e.confidence,
            } for e in evidence])
            print(f"Fetched {len(evidence)} unindexed items for {ticker}")
        except Exception as e:
            print(f"Warning: failed to fetch evidence for {ticker}: {e}")

    print(f"Total unindexed evidence: {len(all_evidence)}")
    context["ti"].xcom_push(key="evidence_list", value=all_evidence)
    return len(all_evidence)


def mark_indexed(**context):
    """Task 3: Mark evidence as indexed via CS2Client directly."""
    import sys
    sys.path.insert(0, "/app")

    from app.services.integration.cs2_client import CS2Client

    evidence_ids = context["ti"].xcom_pull(
        key="indexed_ids", task_ids="index_evidence"
    )
    if not evidence_ids:
        print("No evidence IDs to mark.")
        return 0

    cs2 = CS2Client()
    count = cs2.mark_indexed(evidence_ids)
    print(f"Marked {count} evidence records as indexed")
    return count
```

---

## Step 3: Add Airflow to docker-compose.yml

Add these services to your existing `docker-compose.yml`.

**Important:** Airflow needs its own Postgres DB for metadata.
This adds ~1.5GB RAM usage. Only use this on machines with 16GB+ RAM.
For 8GB laptops — use Option B (local install) instead.

```yaml
  # Airflow Postgres metadata DB
  airflow-db:
    image: postgres:15-alpine
    container_name: pe_airflow_db
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - airflow_db_data:/var/lib/postgresql/data
    networks:
      - pe_orgair_network
    restart: unless-stopped

  # Airflow initialization (runs once)
  airflow-init:
    image: apache/airflow:2.8.1
    container_name: pe_airflow_init
    depends_on:
      - airflow-db
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-db/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      _AIRFLOW_WWW_USER_CREATE: "true"
      _AIRFLOW_WWW_USER_USERNAME: admin
      _AIRFLOW_WWW_USER_PASSWORD: admin
    volumes:
      - ./dags:/opt/airflow/dags
      - ./app:/app/app
    command: db migrate
    networks:
      - pe_orgair_network

  # Airflow Webserver (UI at localhost:8080)
  airflow-webserver:
    image: apache/airflow:2.8.1
    container_name: pe_airflow_webserver
    depends_on:
      - airflow-db
      - airflow-init
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-db/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      AIRFLOW__WEBSERVER__SECRET_KEY: ${AIRFLOW_SECRET_KEY:-your-secret-key-here}
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./app:/app/app
      - airflow_logs:/opt/airflow/logs
    env_file:
      - .env
    command: webserver
    networks:
      - pe_orgair_network
    restart: unless-stopped

  # Airflow Scheduler (triggers DAGs on schedule)
  airflow-scheduler:
    image: apache/airflow:2.8.1
    container_name: pe_airflow_scheduler
    depends_on:
      - airflow-db
      - airflow-init
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-db/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
    volumes:
      - ./dags:/opt/airflow/dags
      - ./app:/app/app
      - airflow_logs:/opt/airflow/logs
    env_file:
      - .env
    command: scheduler
    networks:
      - pe_orgair_network
    restart: unless-stopped

# Add these to your existing volumes section:
# airflow_db_data:
# airflow_logs:
```

---

## Step 4: Add to .env.example

```bash
# Airflow
AIRFLOW_SECRET_KEY=your-random-secret-key-here
AIRFLOW_TICKERS=NVDA,JPM,WMT,GE,DG  # tickers to index nightly
```

---

## Option B: Run Airflow Locally Without Docker (8GB laptops)

Use this instead of Step 3 if you have limited RAM:

```bash
# 1. Install
pip install apache-airflow==2.8.1

# 2. Set home directory (run from project root)
export AIRFLOW_HOME=$(pwd)/airflow

# 3. Initialize DB
airflow db migrate

# 4. Create admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# 5. Copy your DAG
mkdir -p airflow/dags
cp dags/evidence_indexing_dag.py airflow/dags/

# 6. Start (run each in a separate terminal)
airflow webserver --port 8080
airflow scheduler

# 7. Open UI at http://localhost:8080
#    Login: admin / admin
#    Enable the pe_evidence_indexing DAG
#    It will run nightly at 2 AM
```

---

## Step 5: Oracle Cloud VM Setup (for 24/7 running)

```bash
# After creating Oracle Cloud VM and SSH-ing in:

# 1. Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker ubuntu

# 2. Clone your repo
git clone <your-repo-url>
cd pe-org-air-platform

# 3. Copy your .env file (via scp from local)
scp .env ubuntu@<vm-ip>:~/pe-org-air-platform/

# 4. Start everything
docker compose up -d

# 5. Airflow UI available at:
#    http://<vm-ip>:8080
```

---

## Verification Checklist

- [ ] `cs2_client.py` — `mark_indexed()` writes to S3
- [ ] `cs2_client.py` — `get_evidence()` respects `indexed=False` filter
- [ ] `evidence_indexing_dag.py` — uses CS2Client directly (not HTTP)
- [ ] `docker-compose.yml` — Airflow services added (if using Docker)
- [ ] `.env` — `AIRFLOW_SECRET_KEY` and `AIRFLOW_TICKERS` added
- [ ] DAG enabled in Airflow UI at `http://localhost:8080`
- [ ] Test manual trigger: Airflow UI → pe_evidence_indexing → Trigger DAG

---

## How to Verify It's Working

```bash
# 1. Check Airflow scheduler is running
docker logs pe_airflow_scheduler

# 2. Manually trigger the DAG (don't wait for 2 AM)
#    Go to http://localhost:8080
#    Find pe_evidence_indexing
#    Click the play button → Trigger DAG

# 3. Check S3 for indexed state files
#    Should see: indexed_state/NVDA.json, indexed_state/JPM.json etc.

# 4. Run pipeline again — should index 0 new items
#    (everything already marked as indexed)
```