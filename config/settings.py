import os
from dotenv import load_dotenv

# This reads your .env file and makes its values available
# through os.getenv()
load_dotenv()


class Config:
    """Central configuration for the Disaster Response Analytics pipeline.

    All settings are in one place. If you need to change a threshold,
    API URL, or database connection, you change it here and every
    module picks it up automatically.
    """

    # ── Database ──────────────────────────────────────────────
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise EnvironmentError(
            "DATABASE_URL is not set. "
            "Create a .env file with your database connection string."
        )

    # ── FEMA API ──────────────────────────────────────────────
    FEMA_API_BASE_URL = os.getenv(
        "FEMA_API_BASE_URL", "https://www.fema.gov/api/open/v2"
    )
    FEMA_DISASTERS_ENDPOINT = f"{FEMA_API_BASE_URL}/PublicAssistanceFundedProjectsDetails"
    FEMA_PAGE_SIZE = 1000   # how many records to fetch per API call
    FEMA_MAX_PAGES = 50     # safety limit to prevent infinite loops

    # ── Pipeline Settings ─────────────────────────────────────
    BATCH_SIZE = 500        # how many rows to insert at once
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_DIR = os.getenv("LOG_DIR", "logs")

    # ── Claims Generator ──────────────────────────────────────
    CLAIMS_PER_DISASTER_MIN = 20
    CLAIMS_PER_DISASTER_MAX = 200

    # ── SLA Thresholds (days) ─────────────────────────────────
    SLA_TARGET_DAYS = 30    # claims should be resolved within 30 days
    SLA_WARNING_DAYS = 21   # warn when a claim is 21+ days old