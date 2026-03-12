-- =============================================================================
-- Digital Presence Evidence Tables
-- app/database/digital_presence_schema.sql
--
-- Created for CS4: stores concrete subdomain scanning evidence so that
-- digital_presence scores are backed by real scraped data, not LLM guesses.
--
-- Run once against your Snowflake database:
--   snowsql -f app/database/digital_presence_schema.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Table 1: DIGITAL_PRESENCE_DOMAINS
--
-- One row per domain/subdomain scanned per ticker per pipeline run.
-- Primary domain (google.com) + every subdomain Groq discovered and we scanned.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DIGITAL_PRESENCE_DOMAINS (
    id                  VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),  -- UUID PK
    ticker              VARCHAR(10)     NOT NULL,
    company_id          VARCHAR(36),                                     -- FK → COMPANIES.id
    primary_domain      VARCHAR(255)    NOT NULL,                        -- e.g. google.com
    scanned_domain      VARCHAR(255)    NOT NULL,                        -- e.g. cloud.google.com
    domain_type         VARCHAR(20)     NOT NULL,                        -- 'primary' | 'subdomain'
    discovery_source    VARCHAR(30)     NOT NULL,                        -- 'config' | 'groq_discovery' | 'hardcoded_fallback'
    builtwith_live      INTEGER         DEFAULT 0,                       -- total live technologies from BuiltWith
    builtwith_categories INTEGER        DEFAULT 0,                       -- category count from BuiltWith
    wappalyzer_count    INTEGER         DEFAULT 0,                       -- technologies detected by Wappalyzer
    scraper_score       FLOAT           DEFAULT 0.0,                     -- score from THIS domain only (0-100)
    scan_success        BOOLEAN         DEFAULT FALSE,                   -- TRUE if any scraper returned data
    scan_error          VARCHAR(500),                                    -- error message if failed
    scanned_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    run_date            DATE            NOT NULL DEFAULT CURRENT_DATE(),

    PRIMARY KEY (id),
    CONSTRAINT uq_dp_domain_run UNIQUE (ticker, scanned_domain, run_date)
);

-- Index for fast ticker + date lookups
CREATE INDEX IF NOT EXISTS idx_dpd_ticker_date
    ON DIGITAL_PRESENCE_DOMAINS (ticker, run_date DESC);


-- -----------------------------------------------------------------------------
-- Table 2: DIGITAL_PRESENCE_TECHNOLOGIES
--
-- One row per technology detected per domain per run.
-- This is the actual evidence — what was found, where, by which tool.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DIGITAL_PRESENCE_TECHNOLOGIES (
    id                  VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),
    ticker              VARCHAR(10)     NOT NULL,
    scanned_domain      VARCHAR(255)    NOT NULL,                        -- FK → DIGITAL_PRESENCE_DOMAINS.scanned_domain
    tech_name           VARCHAR(255)    NOT NULL,                        -- e.g. "Kubernetes", "TensorFlow"
    category            VARCHAR(100)    NOT NULL,                        -- e.g. "orchestration", "ml_framework"
    detection_source    VARCHAR(20)     NOT NULL,                        -- 'builtwith' | 'wappalyzer'
    is_ai_related       BOOLEAN         DEFAULT FALSE,
    confidence          FLOAT           DEFAULT 0.0,                     -- 0.0–1.0
    run_date            DATE            NOT NULL DEFAULT CURRENT_DATE(),
    detected_at         TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (id),
    -- Deduplicate: same tech on same domain in same day → upsert
    CONSTRAINT uq_dp_tech_run UNIQUE (ticker, scanned_domain, tech_name, run_date)
);

-- Index for evidence retrieval per ticker
CREATE INDEX IF NOT EXISTS idx_dpt_ticker_date
    ON DIGITAL_PRESENCE_TECHNOLOGIES (ticker, run_date DESC);

CREATE INDEX IF NOT EXISTS idx_dpt_ai_related
    ON DIGITAL_PRESENCE_TECHNOLOGIES (ticker, is_ai_related, run_date DESC);


-- -----------------------------------------------------------------------------
-- Convenience view: latest evidence summary per ticker
-- Use this to see what we actually found for a company's latest run.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW DIGITAL_PRESENCE_SUMMARY AS
SELECT
    d.ticker,
    d.primary_domain,
    COUNT(DISTINCT d.scanned_domain)                        AS domains_scanned,
    COUNT(DISTINCT CASE WHEN d.scan_success THEN d.scanned_domain END) AS domains_successful,
    SUM(d.builtwith_live)                                   AS total_builtwith_techs,
    SUM(d.wappalyzer_count)                                 AS total_wappalyzer_techs,
    MAX(d.scraper_score)                                    AS best_domain_score,
    COUNT(t.id)                                             AS total_technologies,
    COUNT(CASE WHEN t.is_ai_related THEN 1 END)             AS ai_technologies,
    d.run_date,
    MAX(d.scanned_at)                                       AS last_scanned_at
FROM DIGITAL_PRESENCE_DOMAINS d
LEFT JOIN DIGITAL_PRESENCE_TECHNOLOGIES t
    ON d.ticker = t.ticker
    AND d.scanned_domain = t.scanned_domain
    AND d.run_date = t.run_date
GROUP BY d.ticker, d.primary_domain, d.run_date;