-- =============================================================
-- Disaster Response Analytics — Database Schema
-- This file creates all tables across three layers:
--   raw      → exact copies of source data, no changes
--   staging  → cleaned, validated, properly typed
--   curated  → aggregated KPIs ready for dashboards
-- =============================================================


-- SCHEMAS: these are like folders inside your database
-- that group related tables together

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;


-- PIPELINE METADATA: tracks every time your pipeline runs
-- so you can see what succeeded, what failed, and when

CREATE TABLE IF NOT EXISTS public.pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    started_at      TIMESTAMP DEFAULT NOW(),
    finished_at     TIMESTAMP,
    status          VARCHAR(20) DEFAULT 'RUNNING',
    rows_ingested   INT DEFAULT 0,
    error_message   TEXT,
    metadata        JSONB
);


-- =============================================================
-- RAW LAYER
-- Data lands here exactly as it came from the source.
-- Dates are stored as TEXT (not DATE) because we don't
-- clean anything at this stage. This preserves the original
-- data for auditing.
-- =============================================================

CREATE TABLE IF NOT EXISTS raw.fema_disasters (
    id                  SERIAL PRIMARY KEY,
    disaster_number     INT,
    declaration_date    TEXT,
    disaster_type       TEXT,
    incident_type       TEXT,
    title               TEXT,
    state               TEXT,
    fips_state_code     TEXT,
    fips_county_code    TEXT,
    designated_area     TEXT,
    declaration_type    TEXT,
    incident_begin_date TEXT,
    incident_end_date   TEXT,
    close_out_date      TEXT,
    hash                TEXT,
    last_refresh        TEXT,
    fema_id             TEXT,
    source_payload      JSONB,
    ingested_at         TIMESTAMP DEFAULT NOW()
);

-- An index makes lookups by disaster_number fast
CREATE INDEX IF NOT EXISTS idx_raw_fema_disaster_number
    ON raw.fema_disasters(disaster_number);


CREATE TABLE IF NOT EXISTS raw.insurance_claims (
    id                  SERIAL PRIMARY KEY,
    claim_id            VARCHAR(50),
    policy_id           VARCHAR(50),
    disaster_number     INT,
    claimant_name       TEXT,
    claim_type          TEXT,
    claim_amount        NUMERIC(12,2),
    date_filed          TEXT,
    date_resolved       TEXT,
    status              TEXT,
    region              TEXT,
    adjuster_id         VARCHAR(50),
    priority            TEXT,
    notes               TEXT,
    ingested_at         TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_claims_disaster
    ON raw.insurance_claims(disaster_number);
CREATE INDEX IF NOT EXISTS idx_raw_claims_claim_id
    ON raw.insurance_claims(claim_id);


-- =============================================================
-- STAGING LAYER
-- Data is cleaned and validated here:
--   - TEXT dates become real DATE types
--   - Constraints enforce allowed values
--   - Derived columns are calculated (days_open, is_overdue)
-- =============================================================

CREATE TABLE IF NOT EXISTS staging.disasters (
    disaster_id         SERIAL PRIMARY KEY,
    disaster_number     INT UNIQUE NOT NULL,
    declaration_date    DATE,
    disaster_type       VARCHAR(50),
    incident_type       VARCHAR(100),
    title               VARCHAR(255),
    state               VARCHAR(2),
    fips_state_code     VARCHAR(5),
    fips_county_code    VARCHAR(5),
    designated_area     VARCHAR(255),
    incident_begin_date DATE,
    incident_end_date   DATE,
    close_out_date      DATE,
    processed_at        TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stg_disaster_state
    ON staging.disasters(state);
CREATE INDEX IF NOT EXISTS idx_stg_disaster_type
    ON staging.disasters(incident_type);


CREATE TABLE IF NOT EXISTS staging.claims (
    claim_pk            SERIAL PRIMARY KEY,
    claim_id            VARCHAR(50) UNIQUE NOT NULL,
    policy_id           VARCHAR(50) NOT NULL,
    disaster_number     INT REFERENCES staging.disasters(disaster_number),
    claimant_name       VARCHAR(255),

    -- CHECK constraints enforce that only valid values are allowed
    claim_type          VARCHAR(50) CHECK (claim_type IN (
                            'Property', 'Auto', 'Health',
                            'Life', 'Business Interruption'
                        )),
    claim_amount        NUMERIC(12,2) CHECK (claim_amount >= 0),
    date_filed          DATE NOT NULL,
    date_resolved       DATE,
    status              VARCHAR(20) CHECK (status IN (
                            'Open', 'In Review', 'Approved',
                            'Denied', 'Closed'
                        )),
    region              VARCHAR(100),
    adjuster_id         VARCHAR(50),
    priority            VARCHAR(20) CHECK (priority IN (
                            'Low', 'Medium', 'High', 'Critical'
                        )),

    -- These two columns don't exist in the raw data.
    -- They are DERIVED (calculated) during the staging transformation.
    days_open           INT,
    is_overdue          BOOLEAN DEFAULT FALSE,

    processed_at        TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stg_claims_status
    ON staging.claims(status);
CREATE INDEX IF NOT EXISTS idx_stg_claims_disaster
    ON staging.claims(disaster_number);
CREATE INDEX IF NOT EXISTS idx_stg_claims_filed
    ON staging.claims(date_filed);


-- =============================================================
-- CURATED LAYER
-- Pre-aggregated tables designed to feed dashboards directly.
-- Each table answers a specific business question.
-- =============================================================

-- One row per disaster: how many claims, what amounts, SLA compliance
CREATE TABLE IF NOT EXISTS curated.disaster_summary (
    disaster_number     INT PRIMARY KEY,
    title               VARCHAR(255),
    state               VARCHAR(2),
    incident_type       VARCHAR(100),
    declaration_date    DATE,
    incident_begin_date DATE,
    incident_end_date   DATE,
    total_claims        INT DEFAULT 0,
    open_claims         INT DEFAULT 0,
    closed_claims       INT DEFAULT 0,
    approved_claims     INT DEFAULT 0,
    denied_claims       INT DEFAULT 0,
    total_claim_amount  NUMERIC(14,2) DEFAULT 0,
    avg_claim_amount    NUMERIC(12,2) DEFAULT 0,
    avg_days_to_resolve NUMERIC(8,2),
    sla_compliance_pct  NUMERIC(5,2),
    refreshed_at        TIMESTAMP DEFAULT NOW()
);

-- Daily activity broken down by region
CREATE TABLE IF NOT EXISTS curated.claims_daily_metrics (
    metric_date         DATE NOT NULL,
    region              VARCHAR(100) NOT NULL,
    claims_filed        INT DEFAULT 0,
    claims_resolved     INT DEFAULT 0,
    claims_open         INT DEFAULT 0,
    claims_overdue      INT DEFAULT 0,
    total_amount_filed  NUMERIC(14,2) DEFAULT 0,
    avg_days_open       NUMERIC(8,2),
    sla_compliance_pct  NUMERIC(5,2),
    refreshed_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (metric_date, region)
);

-- Regional scorecards
CREATE TABLE IF NOT EXISTS curated.regional_performance (
    region              VARCHAR(100) PRIMARY KEY,
    total_claims        INT DEFAULT 0,
    open_claims         INT DEFAULT 0,
    overdue_claims      INT DEFAULT 0,
    avg_resolution_days NUMERIC(8,2),
    total_claim_amount  NUMERIC(14,2) DEFAULT 0,
    sla_compliance_pct  NUMERIC(5,2),
    top_disaster_type   VARCHAR(100),
    refreshed_at        TIMESTAMP DEFAULT NOW()
);

-- Open claims grouped by how old they are
CREATE TABLE IF NOT EXISTS curated.backlog_aging (
    aging_bucket        VARCHAR(30) NOT NULL,
    region              VARCHAR(100) NOT NULL,
    claim_count         INT DEFAULT 0,
    total_amount        NUMERIC(14,2) DEFAULT 0,
    refreshed_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (aging_bucket, region)
);

-- A VIEW is like a saved query. It calculates SLA metrics
-- on the fly whenever you query it.
CREATE OR REPLACE VIEW curated.v_sla_overview AS
SELECT
    d.state,
    d.incident_type,
    COUNT(c.claim_pk)                                           AS total_claims,
    COUNT(*) FILTER (WHERE c.is_overdue)                        AS overdue_claims,
    ROUND(100.0 * COUNT(*) FILTER (WHERE NOT c.is_overdue)
          / NULLIF(COUNT(*), 0), 2)                             AS sla_compliance_pct,
    ROUND(AVG(c.days_open)::NUMERIC, 1)                         AS avg_days_open
FROM staging.claims c
JOIN staging.disasters d ON d.disaster_number = c.disaster_number
GROUP BY d.state, d.incident_type;