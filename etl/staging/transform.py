from config.settings import Config
from utils.database import get_connection, log_pipeline_run
from utils.logger import get_logger

logger = get_logger(__name__)

PIPELINE_NAME = "staging_transform"


def transform_disasters_sql():
    return """
    INSERT INTO staging.disasters (
        disaster_number, declaration_date, disaster_type,
        incident_type, title, state, fips_state_code,
        fips_county_code, designated_area,
        incident_begin_date, incident_end_date, close_out_date
    )
    SELECT DISTINCT ON (disaster_number)
        disaster_number,
        CASE WHEN declaration_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
             THEN declaration_date::DATE ELSE NULL END,
        NULL,
        NULLIF(TRIM(incident_type), ''),
        NULLIF(TRIM(application_title), ''),
        UPPER(NULLIF(TRIM(state), '')),
        state_number_code,
        county_code,
        NULLIF(TRIM(county), ''),
        CASE WHEN first_obligation_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
             THEN first_obligation_date::DATE ELSE NULL END,
        CASE WHEN last_obligation_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
             THEN last_obligation_date::DATE ELSE NULL END,
        NULL
    FROM raw.fema_disasters
    WHERE disaster_number IS NOT NULL
    ORDER BY disaster_number, ingested_at DESC
    ON CONFLICT (disaster_number)
    DO UPDATE SET
        declaration_date    = EXCLUDED.declaration_date,
        incident_type       = EXCLUDED.incident_type,
        title               = EXCLUDED.title,
        incident_end_date   = EXCLUDED.incident_end_date,
        processed_at        = NOW();
    """


def transform_claims_sql():
    sla_days = Config.SLA_TARGET_DAYS
    return f"""
    INSERT INTO staging.claims (
        claim_id, policy_id, disaster_number, claimant_name,
        claim_type, claim_amount, date_filed, date_resolved,
        status, region, adjuster_id, priority, days_open, is_overdue
    )
    SELECT
        claim_id,
        policy_id,
        disaster_number,
        claimant_name,
        claim_type,
        claim_amount,
        date_filed::DATE,
        CASE WHEN date_resolved ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
             THEN date_resolved::DATE ELSE NULL END,
        status,
        NULLIF(TRIM(region), ''),
        adjuster_id,
        priority,
        CASE
            WHEN date_resolved IS NOT NULL
                 AND date_resolved ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
            THEN (date_resolved::DATE - date_filed::DATE)
            ELSE (CURRENT_DATE - date_filed::DATE)
        END AS days_open,
        CASE
            WHEN status NOT IN ('Approved', 'Denied', 'Closed')
                 AND (CURRENT_DATE - date_filed::DATE) > {sla_days}
            THEN TRUE
            ELSE FALSE
        END AS is_overdue
    FROM raw.insurance_claims
    WHERE claim_id IS NOT NULL
      AND date_filed IS NOT NULL
      AND date_filed ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
      AND claim_amount >= 0
      AND claim_type IN (
          'Property', 'Auto', 'Health',
          'Life', 'Business Interruption'
      )
      AND status IN (
          'Open', 'In Review', 'Approved', 'Denied', 'Closed'
      )
    ON CONFLICT (claim_id)
    DO UPDATE SET
        status        = EXCLUDED.status,
        date_resolved = EXCLUDED.date_resolved,
        days_open     = EXCLUDED.days_open,
        is_overdue    = EXCLUDED.is_overdue,
        processed_at  = NOW();
    """


def run():
    logger.info(f"=== Starting {PIPELINE_NAME} ===")
    total_affected = 0

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                logger.info("Transforming disasters: raw -> staging")
                cur.execute(transform_disasters_sql())
                disaster_rows = cur.rowcount
                logger.info(f"  Disasters upserted: {disaster_rows}")
                total_affected += disaster_rows

                logger.info("Transforming claims: raw -> staging")
                cur.execute(transform_claims_sql())
                claims_rows = cur.rowcount
                logger.info(f"  Claims upserted: {claims_rows}")
                total_affected += claims_rows

        log_pipeline_run(PIPELINE_NAME, "SUCCESS", total_affected)
        logger.info(f"=== {PIPELINE_NAME} complete: {total_affected} rows ===")

    except Exception as e:
        log_pipeline_run(PIPELINE_NAME, "FAILED", total_affected, str(e))
        logger.exception(f"Pipeline {PIPELINE_NAME} failed: {e}")
        raise

    return total_affected


if __name__ == "__main__":
    run()
