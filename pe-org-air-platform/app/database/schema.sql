CREATE WAREHOUSE IF NOT EXISTS PE_ORGAIR_WH
WITH
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS PE_ORGAIR_DB;


CREATE SCHEMA IF NOT EXISTS PE_ORGAIR_DB.PLATFORM;

USE WAREHOUSE PE_ORGAIR_WH;
USE DATABASE PE_ORGAIR_DB;
USE SCHEMA PLATFORM;

USE WAREHOUSE PE_ORGAIR_WH;
USE DATABASE PE_ORGAIR_DB;
USE SCHEMA PLATFORM;

CREATE TABLE IF NOT EXISTS industries (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    sector VARCHAR(100) NOT NULL,
    h_r_base DECIMAL(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS companies (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    ticker VARCHAR(10),
    industry_id VARCHAR(36),
    position_factor DECIMAL(4,3) DEFAULT 0.0,
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (industry_id) REFERENCES industries(id)
);

CREATE TABLE IF NOT EXISTS assessments (
    id VARCHAR(36) PRIMARY KEY,
    company_id VARCHAR(36) NOT NULL,
    assessment_type VARCHAR(20) NOT NULL,
    assessment_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'draft',
    primary_assessor VARCHAR(255),
    secondary_assessor VARCHAR(255),
    v_r_score DECIMAL(5,2),
    confidence_lower DECIMAL(5,2),
    confidence_upper DECIMAL(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (company_id) REFERENCES companies(id)
);


CREATE TABLE IF NOT EXISTS dimension_scores (
    id VARCHAR(36) PRIMARY KEY,
    assessment_id VARCHAR(36) NOT NULL,
    dimension VARCHAR(30) NOT NULL,
    score DECIMAL(5,2) NOT NULL,
    weight DECIMAL(4,3),
    confidence DECIMAL(4,3) DEFAULT 0.8,
    evidence_count INT DEFAULT 0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (assessment_id) REFERENCES assessments(id),
    UNIQUE (assessment_id, dimension)
);

-- =============================================================================
-- PE Org-AI-R Platform Foundation - Stored Procedures (Snowflake)
-- =============================================================================
-- All procedures with validation (replacing CHECK constraints)
-- =============================================================================


-- =============================================================================
-- INSERT INDUSTRY
-- Validates: h_r_base between 0 and 100
-- =============================================================================

CREATE OR REPLACE PROCEDURE insert_industry(
    p_id VARCHAR,
    p_name VARCHAR,
    p_sector VARCHAR,
    p_h_r_base DECIMAL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    IF (:p_h_r_base IS NOT NULL AND (:p_h_r_base < 0 OR :p_h_r_base > 100)) THEN
        RETURN 'ERROR: h_r_base must be between 0 and 100';
    END IF;
    
    INSERT INTO industries (id, name, sector, h_r_base)
    VALUES (:p_id, :p_name, :p_sector, :p_h_r_base);
    
    RETURN 'SUCCESS: Industry inserted';
END;
$$;


-- =============================================================================
-- INSERT COMPANY
-- Validates: position_factor between -1.0 and 1.0
-- =============================================================================

CREATE OR REPLACE PROCEDURE insert_company(
    p_name VARCHAR,
    p_ticker VARCHAR,
    p_industry_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_id VARCHAR;
BEGIN
    v_id := UUID_STRING();

    INSERT INTO companies (
        id,
        name,
        ticker,
        industry_id
    )
    VALUES (
        :v_id,
        :p_name,
        :p_ticker,
        :p_industry_id
    );

    RETURN 'SUCCESS: Company inserted with id = ' || v_id;
END;
$$;

-- =============================================================================
-- INSERT ASSESSMENT
-- Validates: 
--   - assessment_type: screening, due_diligence, quarterly, exit_prep
--   - status: draft, in_progress, submitted, approved, superseded
--   - v_r_score: between 0 and 100
-- =============================================================================

CREATE OR REPLACE PROCEDURE insert_assessment(
    p_id VARCHAR,
    p_company_id VARCHAR,
    p_assessment_type VARCHAR,
    p_assessment_date DATE,
    p_status VARCHAR,
    p_primary_assessor VARCHAR,
    p_secondary_assessor VARCHAR,
    p_v_r_score DECIMAL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    IF (:p_assessment_type NOT IN ('screening', 'due_diligence', 'quarterly', 'exit_prep')) THEN
        RETURN 'ERROR: assessment_type must be one of: screening, due_diligence, quarterly, exit_prep';
    END IF;
    
    IF (:p_status NOT IN ('draft', 'in_progress', 'submitted', 'approved', 'superseded')) THEN
        RETURN 'ERROR: status must be one of: draft, in_progress, submitted, approved, superseded';
    END IF;
    
    IF (:p_v_r_score IS NOT NULL AND (:p_v_r_score < 0 OR :p_v_r_score > 100)) THEN
        RETURN 'ERROR: v_r_score must be between 0 and 100';
    END IF;
    
    INSERT INTO assessments (id, company_id, assessment_type, assessment_date, status, primary_assessor, secondary_assessor, v_r_score)
    VALUES (:p_id, :p_company_id, :p_assessment_type, :p_assessment_date, :p_status, :p_primary_assessor, :p_secondary_assessor, :p_v_r_score);
    
    RETURN 'SUCCESS: Assessment inserted';
END;
$$;


-- =============================================================================
-- INSERT DIMENSION SCORE
-- Validates:
--   - dimension: data_infrastructure, ai_governance, technology_stack,
--                talent_skills, leadership_vision, use_case_portfolio, culture_change
--   - score: between 0 and 100
--   - weight: between 0 and 1
--   - confidence: between 0 and 1
-- =============================================================================

CREATE OR REPLACE PROCEDURE insert_dimension_score(
    p_id VARCHAR,
    p_assessment_id VARCHAR,
    p_dimension VARCHAR,
    p_score DECIMAL,
    p_weight DECIMAL,
    p_confidence DECIMAL,
    p_evidence_count INT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    IF (:p_dimension NOT IN ('data_infrastructure', 'ai_governance', 'technology_stack', 'talent_skills', 'leadership_vision', 'use_case_portfolio', 'culture_change')) THEN
        RETURN 'ERROR: dimension must be one of: data_infrastructure, ai_governance, technology_stack, talent_skills, leadership_vision, use_case_portfolio, culture_change';
    END IF;
    
    IF (:p_score < 0 OR :p_score > 100) THEN
        RETURN 'ERROR: score must be between 0 and 100';
    END IF;
    
    IF (:p_weight IS NOT NULL AND (:p_weight < 0 OR :p_weight > 1)) THEN
        RETURN 'ERROR: weight must be between 0 and 1';
    END IF;
    
    IF (:p_confidence IS NOT NULL AND (:p_confidence < 0 OR :p_confidence > 1)) THEN
        RETURN 'ERROR: confidence must be between 0 and 1';
    END IF;
    
    INSERT INTO dimension_scores (id, assessment_id, dimension, score, weight, confidence, evidence_count)
    VALUES (:p_id, :p_assessment_id, :p_dimension, :p_score, :p_weight, :p_confidence, :p_evidence_count);
    
    RETURN 'SUCCESS: Dimension score inserted';
END;
$$;


-- =============================================================================
-- UPDATE DIMENSION SCORE
-- Validates same as INSERT (for non-null values)
-- =============================================================================

CREATE OR REPLACE PROCEDURE update_dimension_score(
    p_id VARCHAR,
    p_dimension VARCHAR,
    p_score DECIMAL,
    p_weight DECIMAL,
    p_confidence DECIMAL,
    p_evidence_count INT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    IF (:p_dimension IS NOT NULL AND :p_dimension NOT IN ('data_infrastructure', 'ai_governance', 'technology_stack', 'talent_skills', 'leadership_vision', 'use_case_portfolio', 'culture_change')) THEN
        RETURN 'ERROR: dimension must be one of: data_infrastructure, ai_governance, technology_stack, talent_skills, leadership_vision, use_case_portfolio, culture_change';
    END IF;
    
    IF (:p_score IS NOT NULL AND (:p_score < 0 OR :p_score > 100)) THEN
        RETURN 'ERROR: score must be between 0 and 100';
    END IF;
    
    IF (:p_weight IS NOT NULL AND (:p_weight < 0 OR :p_weight > 1)) THEN
        RETURN 'ERROR: weight must be between 0 and 1';
    END IF;
    
    IF (:p_confidence IS NOT NULL AND (:p_confidence < 0 OR :p_confidence > 1)) THEN
        RETURN 'ERROR: confidence must be between 0 and 1';
    END IF;
    
    UPDATE dimension_scores
    SET 
        dimension = COALESCE(:p_dimension, dimension),
        score = COALESCE(:p_score, score),
        weight = COALESCE(:p_weight, weight),
        confidence = COALESCE(:p_confidence, confidence),
        evidence_count = COALESCE(:p_evidence_count, evidence_count)
    WHERE id = :p_id;
    
    RETURN 'SUCCESS: Dimension score updated';
END;
$$;

-- =============================================================================
-- SIGNAL SCORE SCHEMA
-- Stores company signal scores with upsert strategy:
-- - If ticker exists: replace the row
-- - If ticker is new: insert new row
-- =============================================================================

CREATE TABLE IF NOT EXISTS SIGNAL_SCORES (
    -- Primary identifiers
    ticker VARCHAR(20) NOT NULL,               -- Upsert key (PRIMARY KEY)
    company_id VARCHAR(36) NOT NULL,           -- UUID from companies table
    company_name VARCHAR(255) NOT NULL,

    -- Signal Scores (0-100 scale)
    hiring_score FLOAT,                        -- Job market/hiring signal
    innovation_score FLOAT,                    -- Patent/innovation signal
    tech_stack_score FLOAT,                    -- Tech stack signal
    leadership_score FLOAT,                    -- Leadership signal (NULL for now)
    composite_score FLOAT,                     -- Weighted average of available scores

    -- Metrics
    total_jobs INTEGER DEFAULT 0,
    ai_jobs INTEGER DEFAULT 0,
    total_patents INTEGER DEFAULT 0,
    ai_patents INTEGER DEFAULT 0,

    -- Tech stack keywords (stored as JSON array)
    techstack_keywords VARIANT,

    -- S3 references
    s3_jobs_key VARCHAR(500),
    s3_patents_key VARCHAR(500),

    -- Timestamps
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Primary key constraint
    PRIMARY KEY (ticker)
);

COMMENT ON TABLE SIGNAL_SCORES IS 'Company signal scores from job market, patents, and tech stack analysis. Upsert by ticker.';
COMMENT ON COLUMN SIGNAL_SCORES.ticker IS 'Company ticker symbol (upsert key)';
COMMENT ON COLUMN SIGNAL_SCORES.hiring_score IS 'Job market/hiring signal score (0-100)';
COMMENT ON COLUMN SIGNAL_SCORES.innovation_score IS 'Patent/innovation signal score (0-100)';
COMMENT ON COLUMN SIGNAL_SCORES.tech_stack_score IS 'Tech stack signal score (0-100)';
COMMENT ON COLUMN SIGNAL_SCORES.leadership_score IS 'Leadership signal score (0-100) - currently unused';
COMMENT ON COLUMN SIGNAL_SCORES.composite_score IS 'Average of available scores';


-- =============================================================================
-- SCORING TABLE
-- Stores computed CS3 scores (TC, V^R, PF, H^R) per company ticker.
-- Upsert strategy: MERGE on ticker (one row per company, updated each run).
-- =============================================================================

CREATE TABLE IF NOT EXISTS SCORING (
    ticker      VARCHAR(20)   NOT NULL PRIMARY KEY,
    tc          FLOAT,                                       -- Talent Concentration [0, 1]
    vr          FLOAT,                                       -- V^R Score [0, 100]
    pf          FLOAT,                                       -- Position Factor [-1, 1]
    hr          FLOAT,                                       -- H^R Score [0, 100]
    scored_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),   -- First scored
    updated_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()    -- Last updated
);

COMMENT ON TABLE SCORING IS 'CS3 composite scores (TC, VR, PF, HR) per company ticker. One row per ticker, updated on each scoring run.';
COMMENT ON COLUMN SCORING.tc IS 'Talent Concentration score [0, 1] — key-person risk indicator';
COMMENT ON COLUMN SCORING.vr IS 'V^R (Value at Risk) score [0, 100] — AI readiness composite';
COMMENT ON COLUMN SCORING.pf IS 'Position Factor [-1, 1] — competitive position vs sector peers';
COMMENT ON COLUMN SCORING.hr IS 'H^R (Human Readiness) score [0, 100] — workforce AI readiness';


-- =============================================================================
-- TC_SCORING TABLE
-- Stores all sub-components that feed into the Talent Concentration (TC) score.
-- One row per ticker; upserted on each TC+VR scoring run.
-- =============================================================================

CREATE TABLE IF NOT EXISTS TC_SCORING (
    ticker                VARCHAR(20)   NOT NULL PRIMARY KEY,

    -- Final TC score
    talent_concentration  FLOAT,                               -- TC ∈ [0, 1]

    -- TC breakdown components
    leadership_ratio      FLOAT,                               -- Leadership / total AI headcount ratio
    team_size_factor      FLOAT,                               -- Penalizes very small or very large teams
    skill_concentration   FLOAT,                               -- HHI-style skill concentration metric
    individual_factor     FLOAT,                               -- Glassdoor individual-mention signal

    -- Job posting counts
    total_ai_jobs         INTEGER,                             -- Total AI-relevant postings found
    senior_ai_jobs        INTEGER,                             -- Senior / lead / principal AI roles
    mid_ai_jobs           INTEGER,                             -- Mid-level AI roles
    entry_ai_jobs         INTEGER,                             -- Entry-level AI roles
    unique_skills_count   INTEGER,                             -- Distinct AI skills found in postings

    -- Glassdoor signals
    individual_mentions   INTEGER,                             -- Named-leader mentions in reviews
    review_count          INTEGER,                             -- Total Glassdoor reviews analysed
    ai_mentions           INTEGER,                             -- AI / ML mentions in reviews

    -- Validation
    tc_in_range           BOOLEAN,                             -- TRUE if TC within CS3 Table 5 range
    tc_expected           VARCHAR(50),                         -- e.g. '0.05 – 0.20'

    scored_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE TC_SCORING IS 'Talent Concentration (TC) score sub-components per ticker. Populated by POST /api/v1/scoring/tc-vr/{ticker}.';
COMMENT ON COLUMN TC_SCORING.talent_concentration IS 'Final TC score ∈ [0, 1]; higher = greater key-person concentration risk';
COMMENT ON COLUMN TC_SCORING.leadership_ratio IS 'Ratio of senior AI roles to total AI roles';
COMMENT ON COLUMN TC_SCORING.team_size_factor IS 'Size-normalisation factor applied to TC';
COMMENT ON COLUMN TC_SCORING.skill_concentration IS 'Skill-diversity penalty (HHI-style)';
COMMENT ON COLUMN TC_SCORING.individual_factor IS 'Glassdoor individual-leader mention signal';
COMMENT ON COLUMN TC_SCORING.total_ai_jobs IS 'Total AI job postings found in the hiring signal';
COMMENT ON COLUMN TC_SCORING.senior_ai_jobs IS 'Senior / lead / principal AI postings';
COMMENT ON COLUMN TC_SCORING.mid_ai_jobs IS 'Mid-level AI postings';
COMMENT ON COLUMN TC_SCORING.entry_ai_jobs IS 'Entry-level AI postings';
COMMENT ON COLUMN TC_SCORING.unique_skills_count IS 'Number of distinct AI skills across all postings';
COMMENT ON COLUMN TC_SCORING.individual_mentions IS 'Count of named-leader mentions in Glassdoor reviews';
COMMENT ON COLUMN TC_SCORING.review_count IS 'Total Glassdoor reviews analysed';
COMMENT ON COLUMN TC_SCORING.ai_mentions IS 'Count of AI/ML keyword mentions in Glassdoor reviews';


-- =============================================================================
-- VR_SCORING TABLE
-- Stores all sub-components that feed into the V^R (Value at Risk) score.
-- One row per ticker; upserted on each TC+VR scoring run.
-- =============================================================================

CREATE TABLE IF NOT EXISTS VR_SCORING (
    ticker                    VARCHAR(20)   NOT NULL PRIMARY KEY,

    -- Final VR score and intermediate calculations
    vr_score                  FLOAT,                           -- V^R ∈ [0, 100]
    weighted_dim_score        FLOAT,                           -- Weighted sum of dimension scores (pre-adjustment)
    talent_risk_adj           FLOAT,                           -- TC-based talent risk adjustment applied
    tc_used                   FLOAT,                           -- TC value used in the VR calculation

    -- The 7 CS3 dimension scores (0-100 each) fed into V^R
    dim_data_infrastructure   FLOAT,                           -- Data Infrastructure dimension score
    dim_ai_governance         FLOAT,                           -- AI Governance dimension score
    dim_technology_stack      FLOAT,                           -- Technology Stack dimension score
    dim_talent_skills         FLOAT,                           -- Talent & Skills dimension score
    dim_leadership_vision     FLOAT,                           -- Leadership & Vision dimension score
    dim_use_case_portfolio    FLOAT,                           -- Use Case Portfolio dimension score
    dim_culture_change        FLOAT,                           -- Culture & Change Mgmt dimension score

    -- Validation
    vr_in_range               BOOLEAN,                         -- TRUE if VR within CS3 Table 5 range
    vr_expected               VARCHAR(50),                     -- e.g. '80 – 100'

    scored_at                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE VR_SCORING IS 'V^R (Value at Risk) score sub-components per ticker. Populated by POST /api/v1/scoring/tc-vr/{ticker}.';
COMMENT ON COLUMN VR_SCORING.vr_score IS 'Final V^R score ∈ [0, 100]';
COMMENT ON COLUMN VR_SCORING.weighted_dim_score IS 'Weighted sum of the 7 dimension scores before TC risk adjustment';
COMMENT ON COLUMN VR_SCORING.talent_risk_adj IS 'Adjustment factor derived from TC (talent concentration risk)';
COMMENT ON COLUMN VR_SCORING.tc_used IS 'TC value (from TC_SCORING) used in this VR calculation';
COMMENT ON COLUMN VR_SCORING.dim_data_infrastructure IS 'Data Infrastructure dimension score [0-100]';
COMMENT ON COLUMN VR_SCORING.dim_ai_governance IS 'AI Governance dimension score [0-100]';
COMMENT ON COLUMN VR_SCORING.dim_technology_stack IS 'Technology Stack dimension score [0-100]';
COMMENT ON COLUMN VR_SCORING.dim_talent_skills IS 'Talent & Skills dimension score [0-100]';
COMMENT ON COLUMN VR_SCORING.dim_leadership_vision IS 'Leadership & Vision dimension score [0-100]';
COMMENT ON COLUMN VR_SCORING.dim_use_case_portfolio IS 'Use Case Portfolio dimension score [0-100]';
COMMENT ON COLUMN VR_SCORING.dim_culture_change IS 'Culture & Change Management dimension score [0-100]';


-- =============================================================================
-- PF_SCORING TABLE
-- Stores all sub-components that feed into the Position Factor (PF) score.
-- One row per ticker; upserted on each PF scoring run.
-- =============================================================================

CREATE TABLE IF NOT EXISTS PF_SCORING (
    ticker                VARCHAR(20)   NOT NULL PRIMARY KEY,

    -- Final PF score and inputs
    position_factor       FLOAT,                               -- PF ∈ [-1, 1]
    vr_score_used         FLOAT,                               -- VR used as input to PF
    sector                VARCHAR(100),                        -- Sector assignment (e.g. 'technology')

    -- VR relative-to-sector component
    sector_avg_vr         FLOAT,                               -- Average VR across sector peers
    vr_diff               FLOAT,                               -- vr_score_used - sector_avg_vr
    vr_component          FLOAT,                               -- Scaled VR component of PF

    -- Market cap component
    market_cap_percentile FLOAT,                               -- Percentile within sector peers [0, 1]
    mcap_component        FLOAT,                               -- Scaled market-cap component of PF

    -- Validation
    pf_in_range           BOOLEAN,                             -- TRUE if PF within CS3 Table 5 range
    pf_expected           VARCHAR(50),                         -- e.g. '0.7 – 1.0'

    scored_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE PF_SCORING IS 'Position Factor (PF) score sub-components per ticker. Populated by POST /api/v1/scoring/pf/{ticker}.';
COMMENT ON COLUMN PF_SCORING.position_factor IS 'Final Position Factor ∈ [-1, 1]; positive = advantaged vs peers';
COMMENT ON COLUMN PF_SCORING.vr_score_used IS 'V^R score used as the primary input to PF calculation';
COMMENT ON COLUMN PF_SCORING.sector IS 'Sector label used for peer comparison';
COMMENT ON COLUMN PF_SCORING.sector_avg_vr IS 'Mean V^R of sector peers at time of scoring';
COMMENT ON COLUMN PF_SCORING.vr_diff IS 'Difference between company VR and sector average VR';
COMMENT ON COLUMN PF_SCORING.vr_component IS 'VR-relative component contribution to PF';
COMMENT ON COLUMN PF_SCORING.market_cap_percentile IS 'Company market-cap percentile within sector [0, 1]';
COMMENT ON COLUMN PF_SCORING.mcap_component IS 'Market-cap component contribution to PF';


-- =============================================================================
-- HR_SCORING TABLE
-- Stores all sub-components that feed into the H^R (Human Readiness) score.
-- One row per ticker; upserted on each H^R scoring run.
-- =============================================================================

CREATE TABLE IF NOT EXISTS HR_SCORING (
    ticker                VARCHAR(20)   NOT NULL PRIMARY KEY,

    -- Final H^R score and calculation chain
    hr_score              FLOAT,                               -- H^R ∈ [0, 100]
    hr_base               FLOAT,                               -- Sector-derived base H^R before PF adjustment
    position_factor_used  FLOAT,                               -- PF value used in H^R calculation
    position_adjustment   FLOAT,                               -- Additive adjustment from PF (hr_base × 0.15 × PF)
    sector                VARCHAR(100),                        -- Sector assignment
    interpretation        VARCHAR(500),                        -- Human-readable readiness narrative

    -- Validation
    hr_in_range           BOOLEAN,                             -- TRUE if H^R within CS3 Table 5 range
    hr_expected           VARCHAR(50),                         -- e.g. '82.9 – 86.3'

    scored_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE HR_SCORING IS 'H^R (Human Readiness) score sub-components per ticker. Populated by POST /api/v1/scoring/hr/{ticker}.';
COMMENT ON COLUMN HR_SCORING.hr_score IS 'Final H^R score ∈ [0, 100]';
COMMENT ON COLUMN HR_SCORING.hr_base IS 'Sector baseline H^R before Position Factor adjustment';
COMMENT ON COLUMN HR_SCORING.position_factor_used IS 'PF value (from PF_SCORING) applied in H^R calculation';
COMMENT ON COLUMN HR_SCORING.position_adjustment IS 'hr_base × 0.15 × position_factor';
COMMENT ON COLUMN HR_SCORING.sector IS 'Sector label used to derive hr_base';
COMMENT ON COLUMN HR_SCORING.interpretation IS 'Narrative interpretation of the H^R score';

