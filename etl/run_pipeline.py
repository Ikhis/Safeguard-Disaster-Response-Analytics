import sys
import time
from utils.logger import get_logger

logger = get_logger("pipeline")


def timed(label, func, *args, **kwargs):
    logger.info(f">> Starting: {label}")
    start = time.time()
    try:
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logger.info(f">> Finished: {label} in {elapsed:.1f}s (result: {result})")
        return result
    except Exception as e:
        elapsed = time.time() - start
        logger.error(f">> FAILED: {label} after {elapsed:.1f}s: {e}")
        raise


def run_full_pipeline(fema_max_pages=5):
    logger.info("=" * 50)
    logger.info("  DISASTER RESPONSE ANALYTICS - FULL RUN")
    logger.info("=" * 50)
    pipeline_start = time.time()

    from etl.ingestion.fema_ingest import run as fema_run
    timed("Step 1: FEMA Ingestion", fema_run, max_pages=fema_max_pages)

    from etl.ingestion.claims_generator import run as claims_run
    timed("Step 2: Claims Generation", claims_run)

    from etl.staging.transform import run as staging_run
    timed("Step 3: Staging Transformations", staging_run)

    from etl.curated.aggregate import run as curated_run
    timed("Step 4: Curated Aggregations", curated_run)

    total_time = time.time() - pipeline_start
    logger.info(f"=== Full pipeline completed in {total_time:.1f}s ===")


def run_step(step):
    steps = {
        "fema": lambda: __import__("etl.ingestion.fema_ingest", fromlist=["run"]).run(max_pages=5),
        "claims": lambda: __import__("etl.ingestion.claims_generator", fromlist=["run"]).run(),
        "staging": lambda: __import__("etl.staging.transform", fromlist=["run"]).run(),
        "curated": lambda: __import__("etl.curated.aggregate", fromlist=["run"]).run(),
    }
    if step not in steps:
        print(f"Unknown step. Available: {', '.join(steps.keys())}")
        sys.exit(1)
    timed(f"Step: {step}", steps[step])


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_step(sys.argv[1])
    else:
        run_full_pipeline()
