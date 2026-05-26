from config.settings import Config
from utils.database import get_connection, log_pipeline_run
from utils.logger import get_logger

logger = get_logger(__name__)

PIPELINE_NAME = "curated_aggregation"


def disaster_summary_sql():
    sla = Config.SLA_TARGET_DAYS
    return f"""
    INSERT INTO curated.disaster_summary (
        disaster_number, title, state, incident_type,
        declaration_date, incident_begin_date, incident_end_date,
        total_claims, open_claims, closed_claims,
        approved_claims, denied_claims,
        total_claim_amount, avg_claim_amount,
        avg_days_to_resolve, sla_compliance_pct
    )
    SELECT
        d.disaster_number,
        d.title,
        d.state,
        d.incident_type,
        d.declaration_date,
        d.incident_begin_date,
        d.incident_end_date,
        COUNT(c.claim_pk),
        COUNT(*) FILTER (WHERE c.status IN ('Open', 'In Review')),
        COUNT(*) FILTER (WHERE c.status IN ('Approved', 'Denied', 'Closed')),
        COUNT(*) FILTER (WHERE c.status = 'Approved'),
        COUNT(*) FILTER (WHERE c.status = 'Denied'),
        COALESCE(SUM(c.claim_amount), 0),
        ROUND(AVG(c.claim_amount), 2),
        ROUND(AVG(c.days_open) FILTER (
            WHERE c.status IN ('Approved', 'Denied', 'Closed')
        ), 2),
        ROUND(100.0 * COUNT(*) FILTER (WHERE c.days_open <= {sla})
              / NULLIF(COUNT(*), 0), 2)
    FROM staging.disasters d
    LEFT JOIN staging.claims c ON c.disaster_number = d.disaster_number
    GROUP BY d.disaster_number, d.title, d.state, d.incident_type,
             d.declaration_date, d.incident_begin_date, d.incident_end_date
    ON CONFLICT (disaster_number)
    DO UPDATE SET
        total_claims        = EXCLUDED.total_claims,
        open_claims         = EXCLUDED.open_claims,
        closed_claims       = EXCLUDED.closed_claims,
        approved_claims     = EXCLUDED.approved_claims,
        denied_claims       = EXCLUDED.denied_claims,
        total_claim_amount  = EXCLUDED.total_claim_amount,
        avg_claim_amount    = EXCLUDED.avg_claim_amount,
        avg_days_to_resolve = EXCLUDED.avg_days_to_resolve,
        sla_compliance_pct  = EXCLUDED.sla_compliance_pct,
        refreshed_at        = NOW();
    """


def daily_metrics_sql():
    sla = Config.SLA_TARGET_DAYS
    return f"""
    DELETE FROM curated.claims_daily_metrics;
    INSERT INTO curated.claims_daily_metrics (
        metric_date, region,
        claims_filed, claims_resolved, claims_open, claims_overdue,
        total_amount_filed, avg_days_open, sla_compliance_pct
    )
    SELECT
        c.date_filed,
        COALESCE(c.region, 'Unknown'),
        COUNT(*),
        COUNT(*) FILTER (WHERE c.date_resolved IS NOT NULL),
        COUNT(*) FILTER (WHERE c.status IN ('Open', 'In Review')),
        COUNT(*) FILTER (WHERE c.is_overdue),
        COALESCE(SUM(c.claim_amount), 0),
        ROUND(AVG(c.days_open), 2),
        ROUND(100.0 * COUNT(*) FILTER (WHERE c.days_open <= {sla})
              / NULLIF(COUNT(*), 0), 2)
    FROM staging.claims c
    GROUP BY c.date_filed, c.region
    ORDER BY c.date_filed, c.region;
    """


def regional_performance_sql():
    sla = Config.SLA_TARGET_DAYS
    return f"""
    DELETE FROM curated.regional_performance;
    INSERT INTO curated.regional_performance (
        region, total_claims, open_claims, overdue_claims,
        avg_resolution_days, total_claim_amount,
        sla_compliance_pct, top_disaster_type
    )
    SELECT
        COALESCE(c.region, 'Unknown'),
        COUNT(*),
        COUNT(*) FILTER (WHERE c.status IN ('Open', 'In Review')),
        COUNT(*) FILTER (WHERE c.is_overdue),
        ROUND(AVG(c.days_open) FILTER (
            WHERE c.status IN ('Approved', 'Denied', 'Closed')
        ), 2),
        COALESCE(SUM(c.claim_amount), 0),
        ROUND(100.0 * COUNT(*) FILTER (WHERE c.days_open <= {sla})
              / NULLIF(COUNT(*), 0), 2),
        MODE() WITHIN GROUP (ORDER BY d.incident_type)
    FROM staging.claims c
    LEFT JOIN staging.disasters d ON d.disaster_number = c.disaster_number
    GROUP BY c.region;
    """


def backlog_aging_sql():
    return """
    DELETE FROM curated.backlog_aging;
    INSERT INTO curated.backlog_aging (
        aging_bucket, region, claim_count, total_amount
    )
    SELECT
        CASE
            WHEN days_open BETWEEN 0  AND 7  THEN '0-7 days'
            WHEN days_open BETWEEN 8  AND 14 THEN '8-14 days'
            WHEN days_open BETWEEN 15 AND 30 THEN '15-30 days'
            WHEN days_open BETWEEN 31 AND 60 THEN '31-60 days'
            WHEN days_open > 60              THEN '60+ days'
        END,
        COALESCE(region, 'Unknown'),
        COUNT(*),
        COALESCE(SUM(claim_amount), 0)
    FROM staging.claims
    WHERE status IN ('Open', 'In Review')
    GROUP BY 1, 2;
    """


def run():
    logger.info(f"=== Starting {PIPELINE_NAME} ===")
    total_affected = 0

    steps = [
        ("disaster_summary", disaster_summary_sql()),
        ("claims_daily_metrics", daily_metrics_sql()),
        ("regional_performance", regional_performance_sql()),
        ("backlog_aging", backlog_aging_sql()),
    ]

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for name, sql in steps:
                    logger.info(f"Building curated.{name}...")
                    cur.execute(sql)
                    rows = cur.rowcount
                    logger.info(f"  {name}: {rows} rows affected")
                    total_affected += max(rows, 0)

        log_pipeline_run(PIPELINE_NAME, "SUCCESS", total_affected)
        logger.info(f"=== {PIPELINE_NAME} complete: {total_affected} rows ===")

    except Exception as e:
        log_pipeline_run(PIPELINE_NAME, "FAILED", total_affected, str(e))
        logger.exception(f"Pipeline {PIPELINE_NAME} failed: {e}")
        raise

    return total_affected


if __name__ == "__main__":
    run()
