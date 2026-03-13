-- Analyst Notes Table — CS4 post-LOI due diligence evidence
-- Snowflake-compatible DDL
-- Notes:
--   - No CHECK constraints (parsed but not enforced in Snowflake)
--   - No CREATE INDEX (only supported on Hybrid Tables)
--   - Clustering on COMPANY_ID for fast per-company queries
--   - Validation enforced at application layer in analyst_notes.py

CREATE TABLE IF NOT EXISTS ANALYST_NOTES (
    NOTE_ID        VARCHAR(36)        NOT NULL,
    COMPANY_ID     VARCHAR(36)        NOT NULL,
    NOTE_TYPE      VARCHAR(50)        NOT NULL,
    -- Valid values (app-enforced): interview_transcript, management_meeting,
    --               site_visit, dd_finding, data_room_summary
    DIMENSION      VARCHAR(50)        NOT NULL,
    -- Valid values (app-enforced): data_infrastructure, ai_governance,
    --               technology_stack, talent, leadership,
    --               use_case_portfolio, culture
    ASSESSOR       VARCHAR(255)       NOT NULL,
    CONFIDENCE     NUMBER(4,3)        DEFAULT 1.0,
    -- Range 0.0-1.0, enforced at application layer
    S3_KEY         VARCHAR(500),
    METADATA       VARIANT,           -- JSON blob (Snowflake native JSON type)
    CREATED_AT     TIMESTAMP_NTZ      DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ANALYST_NOTES PRIMARY KEY (NOTE_ID)
)
CLUSTER BY (COMPANY_ID);
-- Clustering on COMPANY_ID replaces an index for per-company queries
-- Snowflake auto-manages micro-partition pruning on clustered columnsgi