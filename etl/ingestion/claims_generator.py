"""
Insurance Claims Data Generation & Ingestion
─────────────────────────────────────────────
Generates realistic synthetic insurance claims tied to FEMA
disasters already in the raw layer and loads them into
raw.insurance_claims.

In a production environment, this module would be replaced by
a connector to the actual claims database, API, or a Change
Data Capture (CDC) stream from the claims management system.

Features:
    - Claims linked to real disaster numbers from raw.fema_disasters
    - Weighted distributions for claim types, statuses, and priorities
    - Realistic dollar amounts calibrated to each claim type
    - Filing dates within 30 days of disaster declaration
    - Resolution dates 3-90 days after filing for closed claims
    - Regions derived from state codes
    - Idempotent — skips claim_ids that already exist
    - Reproducible output via fixed random seeds

Usage:
    python -m etl.ingestion.claims_generator     # run standalone
    python -m etl.run_pipeline claims             # run via orchestrator
"""

import random
from datetime import datetime, timedelta
from faker import Faker

from config.settings import Config
from utils.database import batch_insert, execute_query, log_pipeline_run
from utils.logger import get_logger

logger = get_logger(__name__)
fake = Faker()

# Fixed seeds ensure reproducible output across runs.
# Running this twice with the same raw data produces
# identical claims, which is useful for testing.
Faker.seed(42)
random.seed(42)

PIPELINE_NAME = "claims_generation"

# Claim type distribution: Property claims are most common (40%),
# followed by Auto (20%), then Health, Business Interruption, and Life.
CLAIM_TYPES = ["Property", "Auto", "Health", "Life", "Business Interruption"]
CLAIM_TYPE_WEIGHTS = [0.40, 0.20, 0.15, 0.10, 0.15]

# Status distribution: 30% Approved, 25% Open, 20% Closed,
# 15% In Review, 10% Denied. This reflects a realistic mix
# where most claims are eventually approved.
STATUSES = ["Open", "In Review", "Approved", "Denied", "Closed"]
STATUS_WEIGHTS = [0.25, 0.15, 0.30, 0.10, 0.20]

# Priority distribution: Medium is most common (40%),
# followed by High (25%), Low (20%), and Critical (15%).
PRIORITIES = ["Low", "Medium", "High", "Critical"]
PRIORITY_WEIGHTS = [0.20, 0.40, 0.25, 0.15]

# Maps US state abbreviations to broader regions for aggregation.
# States not listed here default to "Other".
STATE_REGIONS = {
    "TX": "South", "FL": "South", "LA": "South", "AL": "South",
    "GA": "South", "MS": "South", "SC": "South", "NC": "South",
    "CA": "West", "OR": "West", "WA": "West", "AZ": "West",
    "NV": "West", "CO": "West", "UT": "West",
    "NY": "Northeast", "NJ": "Northeast", "PA": "Northeast",
    "MA": "Northeast", "CT": "Northeast", "ME": "Northeast",
    "IL": "Midwest", "OH": "Midwest", "MI": "Midwest",
    "MN": "Midwest", "IA": "Midwest", "WI": "Midwest",
}

# Column names matching the raw.insurance_claims table schema.
RAW_COLUMNS = [
    "claim_id", "policy_id", "disaster_number", "claimant_name",
    "claim_type", "claim_amount", "date_filed", "date_resolved",
    "status", "region", "adjuster_id", "priority", "notes",
]


def generate_claim_amount(claim_type):
    """Generate a realistic claim dollar amount based on claim type.

    Uses a triangular distribution where most values cluster near
    the lower end of the range, reflecting real-world patterns where
    small claims vastly outnumber large ones.

    Amount ranges by type:
        - Property:              $5,000 — $500,000
        - Auto:                  $1,000 — $75,000
        - Health:                $500   — $150,000
        - Life:                  $10,000 — $1,000,000
        - Business Interruption: $10,000 — $2,000,000

    Args:
        claim_type (str): One of the five valid claim types.

    Returns:
        float: Dollar amount rounded to 2 decimal places.
    """
    ranges = {
        "Property": (5000, 500000),
        "Auto": (1000, 75000),
        "Health": (500, 150000),
        "Life": (10000, 1000000),
        "Business Interruption": (10000, 2000000),
    }
    lo, hi = ranges.get(claim_type, (1000, 100000))
    return round(random.triangular(lo, hi, lo * 2), 2)


def generate_one_claim(disaster_number, state, declaration_date, seq):
    """Generate a single synthetic insurance claim record.

    Creates a tuple representing one row in raw.insurance_claims,
    with realistic values for all fields. The claim is linked to
    a specific disaster and filed within 30 days of the declaration.

    Args:
        disaster_number (int): FEMA disaster number to link this claim to.
        state (str): Two-letter state abbreviation where the disaster occurred.
            Used to derive the region.
        declaration_date (str): Disaster declaration date as a string
            (e.g., '2023-09-15T00:00:00.000Z'). Only the first 10 characters
            are parsed.
        seq (int): Sequence number used to generate a unique claim_id
            (e.g., CLM-1239-00001).

    Returns:
        tuple: A single row of claim data matching RAW_COLUMNS order:
            (claim_id, policy_id, disaster_number, claimant_name,
             claim_type, claim_amount, date_filed, date_resolved,
             status, region, adjuster_id, priority, notes)
    """
    claim_type = random.choices(CLAIM_TYPES, weights=CLAIM_TYPE_WEIGHTS, k=1)[0]
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
    priority = random.choices(PRIORITIES, weights=PRIORITY_WEIGHTS, k=1)[0]

    # Parse the declaration date; fall back to Jan 1 2023 if unparseable
    try:
        begin = datetime.strptime(declaration_date[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        begin = datetime(2023, 1, 1)

    # Claims are filed 0-30 days after the disaster is declared
    date_filed = begin + timedelta(days=random.randint(0, 30))

    # Only resolved claims (Approved, Denied, Closed) have a resolution date
    date_resolved = None
    if status in ("Approved", "Denied", "Closed"):
        date_resolved = date_filed + timedelta(days=random.randint(3, 90))

    region = STATE_REGIONS.get(state, "Other")

    return (
        f"CLM-{disaster_number}-{seq:05d}",
        f"POL-{fake.bothify('??####')}",
        disaster_number,
        fake.name(),
        claim_type,
        generate_claim_amount(claim_type),
        date_filed.strftime("%Y-%m-%d"),
        date_resolved.strftime("%Y-%m-%d") if date_resolved else None,
        status,
        region,
        f"ADJ-{random.randint(100, 999)}",
        priority,
        fake.sentence(nb_words=8) if random.random() < 0.3 else None,
    )


def run():
    """Execute the full claims generation pipeline.

    Workflow:
        1. Query raw.fema_disasters for unique disaster numbers
           (up to 200, most recent record per disaster)
        2. Check raw.insurance_claims for existing claim_ids to
           avoid duplicates
        3. Generate 20-200 synthetic claims per disaster using
           weighted random distributions
        4. Batch insert new claims into raw.insurance_claims
        5. Log the pipeline execution to public.pipeline_runs

    The number of claims per disaster is controlled by
    Config.CLAIMS_PER_DISASTER_MIN and Config.CLAIMS_PER_DISASTER_MAX.

    Returns:
        int: Total number of new claim records inserted.

    Raises:
        Exception: Re-raises any database error after logging
            the failure to public.pipeline_runs.
    """
    logger.info(f"=== Starting {PIPELINE_NAME} ===")
    total_inserted = 0

    try:
        # Step 1: Get disasters to generate claims against
        disasters = execute_query(
            """
            SELECT DISTINCT ON (disaster_number)
                disaster_number, state, declaration_date
            FROM raw.fema_disasters
            WHERE disaster_number IS NOT NULL
            ORDER BY disaster_number, ingested_at DESC
            LIMIT 200
            """,
            fetch=True,
        )

        if not disasters:
            logger.warning("No disasters found in raw layer. Run FEMA ingestion first.")
            return 0

        # Step 2: Check for existing claims to avoid duplicates
        existing = execute_query(
            "SELECT DISTINCT claim_id FROM raw.insurance_claims",
            fetch=True,
        )
        existing_ids = {r["claim_id"] for r in existing} if existing else set()

        logger.info(f"Found {len(disasters)} disasters, {len(existing_ids)} existing claims.")

        # Step 3: Generate claims for each disaster
        all_rows = []
        for disaster in disasters:
            num_claims = random.randint(
                Config.CLAIMS_PER_DISASTER_MIN,
                Config.CLAIMS_PER_DISASTER_MAX,
            )
            for seq in range(1, num_claims + 1):
                claim = generate_one_claim(
                    disaster["disaster_number"],
                    disaster["state"],
                    disaster["declaration_date"],
                    seq,
                )
                # Only add if this claim_id doesn't already exist
                if claim[0] not in existing_ids:
                    all_rows.append(claim)

        logger.info(f"Generated {len(all_rows)} new claim records.")

        # Step 4: Batch insert into raw layer
        if all_rows:
            total_inserted = batch_insert(
                "raw.insurance_claims",
                RAW_COLUMNS,
                all_rows,
                page_size=Config.BATCH_SIZE,
            )

        # Step 5: Log successful execution
        log_pipeline_run(PIPELINE_NAME, "SUCCESS", total_inserted)
        logger.info(f"=== {PIPELINE_NAME} complete: {total_inserted} new rows ===")

    except Exception as e:
        log_pipeline_run(PIPELINE_NAME, "FAILED", total_inserted, str(e))
        logger.exception(f"Pipeline {PIPELINE_NAME} failed: {e}")
        raise

    return total_inserted


if __name__ == "__main__":
    run()-