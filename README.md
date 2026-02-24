# 🏢 PE Org-AI-R Platform : CS4

## Setting up Docker

1. Run 
```bash
docker compose build --no-cache
```

2. Make sure to clear all Docker cache
```bash
docker compose up
```
## Refactor done from CS1,2,3 -> CS4

### Dead Code — Not Removed

The following router files exist on disk but are **not registered in `app/main.py`** and are intentionally inactive for CS4. They are preserved for reference only.

| File | Reason disabled |
|------|----------------|
| `app/routers/industries.py` | CS4 doesn't use the industry catalog; import + `include_router` both commented out in `main.py` |
| `app/routers/board_governance.py` | CS4 reads board composition data from S3 via the scoring router; direct collection endpoint not needed |
| `app/routers/glassdoor_signals.py` | CS4 reads culture signal data from S3 via the scoring router; direct collection endpoint not needed |
| `app/routers/tc_vr_scoring.py` | TC + V^R computation absorbed into `CompositeScoringService`; individual router not registered |
| `app/routers/position_factor.py` | PF computation absorbed into `CompositeScoringService`; individual router not registered |
| `app/routers/hr_scoring.py` | H^R computation absorbed into `CompositeScoringService`; individual router not registered |
| `app/routers/orgair_scoring.py` | Org-AI-R computation absorbed into `CompositeScoringService`; individual router not registered |
| `app/routers/property_tests.py` | Runs pytest via `subprocess.run()`; not needed as an HTTP endpoint in CS4 |
| `app/routers/pdf_parser.py` | Standalone PDF parse endpoint; underlying pipeline (`pipelines/pdf_parser.py`) was deleted in CS4 cleanup |


### Files Deleted
- **`app/services/snowflake.py`** — `get_snowflake_connection()` moved to `repositories/base.py`; `SnowflakeService` had no active callers
- **`app/scoring/integration_service.py`** — Dead code; used a self-HTTP pattern (`localhost:8000`) replaced by `CompositeScoringService`
- **`app/pipelines/pipeline2_runner.py`** — Batch CLI orchestrator for jobs+patents; no router or service imported it
- **`app/pipelines/registry.py`** — `DocumentRegistry` SHA256 flat-file dedup; superseded by `DocumentRepository.exists_by_hash()`
- **`app/pipelines/exporters.py`** — Debug JSON dumpers (`export_sample_json`, `export_parsed_document_json`, `export_chunks_json`) writing to local `data/processed/`; no active callers
- **`app/pipelines/pdf_parser.py`** — Standalone `PDFParser` for 10-K PDFs; only caller was `app/routers/pdf_parser.py` which is not registered in `main.py`
- **`app/pipelines/board analysis.py`** — Space in filename made it unimportable via standard Python `import`; unreachable by any module

### Files Created
- **`app/services/composite_scoring_service.py`** — New orchestrator for the entire TC→V^R→PF→H^R→Synergy→Org-AI-R pipeline. Absorbed all module-level constants, intermediate Pydantic response models, and business logic that was previously scattered across 4 router files.
- **`app/services/board_governance_service.py`** — Wraps `BoardCompositionAnalyzer`, owns S3 and signal persistence. Extracted from `board_governance.py` router.
- **`app/services/culture_signal_service.py`** — Wraps `CultureCollector`, owns S3 and dimension mapping upsert. Extracted from `glassdoor_signals.py` router.
- **`app/repositories/composite_scoring_repository.py`** — Owns all reads/writes for SCORING, TC_SCORING, VR_SCORING, PF_SCORING, HR_SCORING tables. Consolidated from inline SQL across 4 routers.
- **`app/repositories/scoring_read_repository.py`** — Re-export shim for backward compatibility with `CompositeScoringRepository`.

### Router Changes (making them thin wrappers)

**`app/routers/tc_vr_scoring.py`** — Deleted `_upsert_tc_scoring`, `_upsert_vr_scoring`, `_upsert_scoring_table`, `_compute_tc_vr`, `_save_tc_vr_result`, `_load_jobs_s3`, all associated Pydantic models, and all cross-router imports. Now just calls `CompositeScoringService`.

**`app/routers/position_factor.py`** — Deleted `_upsert_pf_scoring`, `_upsert_scoring_pf`, `_compute_position_factor`, `_save_pf_result`, a TEMPORARY block, associated models, and cross-router imports.

**`app/routers/hr_scoring.py`** — Deleted `_upsert_hr_scoring`, `_upsert_scoring_hr`, `_compute_hr`, `_save_hr_result`, a TEMPORARY block, associated models, and cross-router imports.

**`app/routers/orgair_scoring.py`** — Deleted `_upsert_scoring_orgair`, `_compute_orgair`, `_save_orgair_result`, `generate_results` body, a TEMPORARY block, and all associated models. Reduced from ~893 lines to ~250.

**`app/routers/signals.py`** — Deleted `get_s3_client()`, `delete_s3_prefix()`, raw boto3 import, and 4 module-level AWS env vars. S3 deletes now go through `S3StorageService.delete_prefix()`. `score_all_signals()` calls services directly instead of calling other route handler functions.

**`app/routers/board_governance.py`** — Deleted `_get_analyzer()`, `_resolve_company_id()`, `_board_save_to_s3()`, `_analyze_one()`. Replaced direct `BoardCompositionAnalyzer` instantiation with `BoardGovernanceService`.

**`app/routers/glassdoor_signals.py`** — Deleted `_upsert_culture_to_snowflake()`, `_load_latest_culture_json()`, `_load_latest_raw_json()`. Replaced direct S3/Snowflake logic with `CultureSignalService`.

**`app/routers/dimensionScores.py`** — Deleted dead code: `WeightValidationMixin`, `BulkDimensionScoreCreate`, and `model_validator` import.

### Repository Layer Changes

**`app/repositories/base.py`** — `get_snowflake_connection()` inlined here (was in `services/snowflake.py`). Now the sole definition in the codebase.

**`app/repositories/chunk_repository.py`** — Now extends `BaseRepository`. Replaced `self.conn` with `self.get_connection()`. Added `get_s3_keys_by_sections()` and `get_all_s3_keys()`.

**`app/repositories/signal_repository.py`** — Now extends `BaseRepository`. Removed `self.conn` singleton; all cursors use `with self.get_connection()`.

**`app/repositories/signal_scores_repository.py`** — Now extends `BaseRepository`. Removed `self.conn`. Deleted dead code: `calculate_composite_score()` function and `close()` method.

**`app/repositories/document_repository.py`** — Now extends `BaseRepository`. Replaced `self.conn`.

**`app/repositories/company_repository.py`** — Now extends `BaseRepository`. Replaced `self.conn`.

**`app/repositories/scoring_repository.py`** — Now extends `BaseRepository`. Replaced `self.conn`. Added `upsert_culture_mapping()` method.

### Service Layer Changes

**`app/services/scoring_service.py`** — Removed `get_snowflake_connection` import and `self.conn`. `_get_section_text()` and `_get_all_chunks_text()` now delegate to `ChunkRepository` instead of using a raw connection.

**`app/services/s3_storage.py`** — Added `delete_prefix()` and `delete_keys()` methods to serve as the sole S3 deletion gateway.

**`app/services/__init__.py`** — Removed `SnowflakeService` import/re-export. Deleted `_get_signals_storage_classes` dead code.

### Pipeline Changes

**`app/pipelines/job_signals.py`** — `step5_store_to_s3_and_snowflake()` now uses `SignalRepository.create_signal()` instead of `SnowflakeService.insert_external_signal()`. Column mapping updated.

**`app/pipelines/pipeline2_runner.py`** — Removed `SnowflakeService` import, `_init_snowflake()`, `_close_snowflake()`. `step_write_to_snowflake()` now uses `SignalRepository.upsert_summary()`.

### Streamlit Changes

**`streamlit/data_loader.py`** — Deleted `_get_snowflake_conn()` and three functions that ran raw SQL against Snowflake. Replaced with HTTP calls to the FastAPI backend (`/health/table-counts`, `/signals/{ticker}/current-scores`, `/documents/report`).

### New Health Endpoint

**`app/routers/health.py`** — Added `GET /health/table-counts` to expose Snowflake table counts via API (so Streamlit doesn't need a direct connection).

---

### Net Effect

All changes enforce two invariants: Snowflake access only through `BaseRepository.execute_query()`, and S3 access only through `S3StorageService`. Raw SQL, raw boto3, cross-router imports, and direct Snowflake connections from Streamlit were all eliminated. Business logic moved from routers into services and repositories, leaving routers as thin HTTP wrappers.

