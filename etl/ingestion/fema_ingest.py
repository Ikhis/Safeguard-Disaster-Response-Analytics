import json
import time
import requests

from config.settings import Config
from utils.database import batch_insert, execute_query, log_pipeline_run
from utils.logger import get_logger

logger = get_logger(__name__)

PIPELINE_NAME = "fema_funded_projects_ingestion"

RAW_COLUMNS = [
    "disaster_number", "declaration_date", "incident_type",
    "pw_number", "application_title", "applicant_id",
    "damage_category_code", "damage_category_descrip",
    "project_status", "project_process_step", "project_size",
    "county", "county_code", "state", "state_number_code",
    "project_amount", "federal_share_obligated", "total_obligated",
    "last_obligation_date", "first_obligation_date",
    "mitigation_amount", "gm_project_id", "gm_applicant_id",
    "last_refresh", "hash", "source_payload",
]


def fetch_page(skip, top, retries=3):
    url = Config.FEMA_DISASTERS_ENDPOINT
    params = {"$skip": skip, "$top": top, "$orderby": "disasterNumber"}
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            wait = 2 ** attempt
            logger.warning(f"FEMA API attempt {attempt}/{retries} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)
    logger.error(f"FEMA API failed after {retries} retries at skip={skip}")
    return None


def extract_projects(max_pages=None):
    max_pages = max_pages or Config.FEMA_MAX_PAGES
    page_size = Config.FEMA_PAGE_SIZE
    for page_num in range(max_pages):
        skip = page_num * page_size
        logger.info(f"Fetching FEMA page {page_num + 1} (skip={skip}, top={page_size})")
        data = fetch_page(skip, page_size)
        if data is None:
            logger.error("Aborting extraction due to API failure.")
            break
        records = data.get("PublicAssistanceFundedProjectsDetails", [])
        if not records:
            logger.info(f"No more records at page {page_num + 1}. Extraction complete.")
            break
        yield records
        logger.info(f"Page {page_num + 1}: {len(records)} records extracted")


def map_record(record):
    return (
        record.get("disasterNumber"),
        record.get("declarationDate"),
        record.get("incidentType"),
        record.get("pwNumber"),
        record.get("applicationTitle"),
        record.get("applicantId"),
        record.get("damageCategoryCode"),
        record.get("damageCategoryDescrip"),
        record.get("projectStatus"),
        record.get("projectProcessStep"),
        record.get("projectSize"),
        record.get("county"),
        record.get("countyCode"),
        record.get("stateAbbreviation"),
        record.get("stateNumberCode"),
        record.get("projectAmount"),
        record.get("federalShareObligated"),
        record.get("totalObligated"),
        record.get("lastObligationDate"),
        record.get("firstObligationDate"),
        record.get("mitigationAmount"),
        str(record.get("gmProjectId", "")),
        str(record.get("gmApplicantId", "")),
        record.get("lastRefresh"),
        record.get("hash"),
        json.dumps(record),
    )


def get_existing_hashes():
    rows = execute_query("SELECT DISTINCT hash FROM raw.fema_disasters", fetch=True)
    return {r["hash"] for r in rows} if rows else set()


def load_projects(records, existing_hashes):
    new_rows = [map_record(r) for r in records if r.get("hash") not in existing_hashes]
    if not new_rows:
        return 0
    inserted = batch_insert("raw.fema_disasters", RAW_COLUMNS, new_rows)
    existing_hashes.update(r.get("hash") for r in records)
    return inserted


def run(max_pages=None):
    logger.info(f"=== Starting {PIPELINE_NAME} ===")
    total_inserted = 0
    try:
        existing_hashes = get_existing_hashes()
        logger.info(f"Found {len(existing_hashes)} existing records (will skip).")
        for page_records in extract_projects(max_pages):
            inserted = load_projects(page_records, existing_hashes)
            total_inserted += inserted
        log_pipeline_run(PIPELINE_NAME, "SUCCESS", total_inserted)
        logger.info(f"=== {PIPELINE_NAME} complete: {total_inserted} new rows ===")
    except Exception as e:
        log_pipeline_run(PIPELINE_NAME, "FAILED", total_inserted, str(e))
        logger.exception(f"Pipeline {PIPELINE_NAME} failed: {e}")
        raise
    return total_inserted


if __name__ == "__main__":
    run(max_pages=5)
