-- =============================================================================
-- Signal-to-Dimension Mapping Table (CS3 Task 5.0a) — Pivoted Format
-- Directly stores the Table 1 matrix from CS3 doc
-- Each row = one signal source, columns = dimension weights
-- =============================================================================

USE WAREHOUSE PE_ORGAIR_WH;
USE DATABASE PE_ORGAIR_DB;
USE SCHEMA PLATFORM;

CREATE TABLE IF NOT EXISTS signal_dimension_mappings (
    id VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    signal_source VARCHAR(50) NOT NULL UNIQUE,
    data_infrastructure DECIMAL(4,2) DEFAULT NULL,
    ai_governance DECIMAL(4,2) DEFAULT NULL,
    technology_stack DECIMAL(4,2) DEFAULT NULL,
    talent_skills DECIMAL(4,2) DEFAULT NULL,
    leadership_vision DECIMAL(4,2) DEFAULT NULL,
    use_case_portfolio DECIMAL(4,2) DEFAULT NULL,
    culture_change DECIMAL(4,2) DEFAULT NULL,
    primary_dimension VARCHAR(30) NOT NULL,
    reliability DECIMAL(4,3) DEFAULT 0.800,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE signal_dimension_mappings IS 
    'Maps CS2 evidence signals to CS3 VR scoring dimensions. Each row is one signal source with weights across 7 dimensions. NULL = no contribution. Weights per row MUST sum to 1.0.';


-- =============================================================================
-- STORED PROCEDURE: Validates weights sum to 1.0 before insert
-- =============================================================================

CREATE OR REPLACE PROCEDURE insert_signal_dimension_mapping(
    p_signal_source VARCHAR,
    p_data_infrastructure DECIMAL,
    p_ai_governance DECIMAL,
    p_technology_stack DECIMAL,
    p_talent_skills DECIMAL,
    p_leadership_vision DECIMAL,
    p_use_case_portfolio DECIMAL,
    p_culture_change DECIMAL,
    p_primary_dimension VARCHAR,
    p_reliability DECIMAL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total DECIMAL(5,2);
BEGIN
    -- Calculate sum of weights (NULLs treated as 0)
    v_total := COALESCE(:p_data_infrastructure, 0) +
               COALESCE(:p_ai_governance, 0) +
               COALESCE(:p_technology_stack, 0) +
               COALESCE(:p_talent_skills, 0) +
               COALESCE(:p_leadership_vision, 0) +
               COALESCE(:p_use_case_portfolio, 0) +
               COALESCE(:p_culture_change, 0);

    -- Strict validation: weights must sum to exactly 1.00
    IF (:v_total != 1.00) THEN
        RETURN 'ERROR: Dimension weights must sum to 1.00, got ' || :v_total || ' for source ' || :p_signal_source;
    END IF;

    -- Validate primary_dimension is one of the 7 valid dimensions
    IF (:p_primary_dimension NOT IN ('data_infrastructure', 'ai_governance', 'technology_stack', 'talent_skills', 'leadership_vision', 'use_case_portfolio', 'culture_change')) THEN
        RETURN 'ERROR: primary_dimension must be one of the 7 valid dimensions';
    END IF;

    -- Validate the primary_dimension column actually has a non-null weight
    IF (
        (:p_primary_dimension = 'data_infrastructure'  AND :p_data_infrastructure IS NULL) OR
        (:p_primary_dimension = 'ai_governance'        AND :p_ai_governance IS NULL) OR
        (:p_primary_dimension = 'technology_stack'     AND :p_technology_stack IS NULL) OR
        (:p_primary_dimension = 'talent_skills'        AND :p_talent_skills IS NULL) OR
        (:p_primary_dimension = 'leadership_vision'    AND :p_leadership_vision IS NULL) OR
        (:p_primary_dimension = 'use_case_portfolio'   AND :p_use_case_portfolio IS NULL) OR
        (:p_primary_dimension = 'culture_change'       AND :p_culture_change IS NULL)
    ) THEN
        RETURN 'ERROR: primary_dimension column must have a non-null weight';
    END IF;

    -- Validate reliability is between 0 and 1
    IF (:p_reliability < 0 OR :p_reliability > 1) THEN
        RETURN 'ERROR: reliability must be between 0 and 1';
    END IF;

    INSERT INTO signal_dimension_mappings (
        id, signal_source, 
        data_infrastructure, ai_governance, technology_stack, 
        talent_skills, leadership_vision, use_case_portfolio, culture_change,
        primary_dimension, reliability
    )
    SELECT 
        UUID_STRING(), :p_signal_source,
        :p_data_infrastructure, :p_ai_governance, :p_technology_stack,
        :p_talent_skills, :p_leadership_vision, :p_use_case_portfolio, :p_culture_change,
        :p_primary_dimension, :p_reliability;

    RETURN 'SUCCESS: Mapping inserted for ' || :p_signal_source || ' (total weight = ' || :v_total || ')';
END;
$$;


-- =============================================================================
-- SEED DATA: 7 CS2 Sources via stored procedure (validated)
-- =============================================================================

-- technology_hiring: 0.10 + 0.20 + 0.70 + 0.10 = 1.10 
-- Wait — that's 1.10! CS3 doc shows Data=0.10, Tech=0.20, Talent=0.70, Culture=0.10
-- But the doc says "Weights within a source sum to 1.0"
-- Rechecking: the image shows Data=0.10, Tech=0.20, Talent=0.70 = 1.00
-- Culture=0.10 is NOT in the original row for technology_hiring
-- The 0.10 culture belongs to technology_hiring ONLY if we drop Data
-- Let's match the CS3 doc EXACTLY:

-- technology_hiring:   Data=0.10 + Tech=0.20 + Talent=0.70 = 1.00 ✓
CALL insert_signal_dimension_mapping('technology_hiring',   0.10, NULL, 0.20, 0.70, NULL, NULL, NULL, 'talent_skills',       0.850);

-- innovation_activity: Data=0.20 + Tech=0.50 + Use=0.30 = 1.00 ✓
CALL insert_signal_dimension_mapping('innovation_activity', 0.20, NULL, 0.50, NULL, NULL, 0.30, NULL, 'technology_stack',    0.800);

-- digital_presence:    Data=0.60 + Tech=0.40 = 1.00 ✓
CALL insert_signal_dimension_mapping('digital_presence',    0.60, NULL, 0.40, NULL, NULL, NULL, NULL, 'data_infrastructure', 0.750);

-- leadership_signals:  Gov=0.25 + Lead=0.60 + Culture=0.15 = 1.00 ✓
CALL insert_signal_dimension_mapping('leadership_signals',  NULL, 0.25, NULL, NULL, 0.60, NULL, 0.15, 'leadership_vision',  0.700);

-- sec_item_1:          Tech=0.30 + Use=0.70 = 1.00 ✓
CALL insert_signal_dimension_mapping('sec_item_1',          NULL, NULL, 0.30, NULL, NULL, 0.70, NULL, 'use_case_portfolio',  0.800);

-- sec_item_1a:         Data=0.20 + Gov=0.80 = 1.00 ✓
CALL insert_signal_dimension_mapping('sec_item_1a',         0.20, 0.80, NULL, NULL, NULL, NULL, NULL, 'ai_governance',       0.850);

-- sec_item_7:          Data=0.20 + Lead=0.50 + Use=0.30 = 1.00 ✓
CALL insert_signal_dimension_mapping('sec_item_7',          0.20, NULL, NULL, NULL, 0.50, 0.30, NULL, 'leadership_vision',  0.800);

-- CS3 NEW Sources (uncomment when collectors are built)
-- glassdoor_reviews:   Talent=0.10 + Lead=0.10 + Culture=0.80 = 1.00 ✓
-- CALL insert_signal_dimension_mapping('glassdoor_reviews',   NULL, NULL, NULL, 0.10, 0.10, NULL, 0.80, 'culture_change',  0.700);
-- board_composition:   Gov=0.70 + Lead=0.30 = 1.00 ✓
-- CALL insert_signal_dimension_mapping('board_composition',   NULL, 0.70, NULL, NULL, 0.30, NULL, NULL, 'ai_governance',   0.750);


-- =============================================================================
-- VERIFICATION QUERIES (run these after seeding)
-- =============================================================================

-- View the matrix
SELECT signal_source, 
       data_infrastructure AS "Data",
       ai_governance AS "Gov", 
       technology_stack AS "Tech",
       talent_skills AS "Talent",
       leadership_vision AS "Lead",
       use_case_portfolio AS "Use",
       culture_change AS "Culture"
FROM signal_dimension_mappings
ORDER BY created_at;

-- Verify weights sum to 1.0 per row
SELECT signal_source,
       COALESCE(data_infrastructure,0) + COALESCE(ai_governance,0) + 
       COALESCE(technology_stack,0) + COALESCE(talent_skills,0) + 
       COALESCE(leadership_vision,0) + COALESCE(use_case_portfolio,0) + 
       COALESCE(culture_change,0) AS total_weight
FROM signal_dimension_mappings;