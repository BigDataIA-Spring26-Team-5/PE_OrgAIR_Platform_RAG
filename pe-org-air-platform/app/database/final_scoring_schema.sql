CREATE TABLE IF NOT EXISTS SCORING (
    ticker          VARCHAR(10) PRIMARY KEY,
    org_air         DECIMAL(5,2),
    vr_score        DECIMAL(5,2),
    hr_score        DECIMAL(5,2),
    synergy_score   DECIMAL(5,2),
    position_factor DECIMAL(6,4),
    talent_concentration DECIMAL(6,4),
    confidence      DECIMAL(4,3),
    ci_lower        DECIMAL(5,2),
    ci_upper        DECIMAL(5,2),
    scored_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
