import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine
from contextlib import contextmanager
from config.settings import Config
from utils.logger import get_logger

logger = get_logger(__name__)
_engine = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(Config.DATABASE_URL, pool_pre_ping=True)
        logger.info("SQLAlchemy engine created.")
    return _engine


@contextmanager
def get_connection():
    conn = psycopg2.connect(Config.DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_query(query, params=None, fetch=False):
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()


def batch_insert(table, columns, rows, page_size=500):
    if not rows:
        return 0
    cols = ", ".join(columns)
    template = "(" + ", ".join(["%s"] * len(columns)) + ")"
    total_inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for i in range(0, len(rows), page_size):
                batch = rows[i:i + page_size]
                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO {table} ({cols}) VALUES %s ON CONFLICT DO NOTHING",
                    batch,
                    template=template,
                    page_size=page_size,
                )
                total_inserted += len(batch)
    logger.info(f"Total inserted into {table}: {total_inserted} rows")
    return total_inserted


def log_pipeline_run(pipeline_name, status, rows=0, error=None):
    execute_query(
        """INSERT INTO public.pipeline_runs
            (pipeline_name, status, rows_ingested, error_message, finished_at)
        VALUES (%s, %s, %s, %s, NOW())""",
        (pipeline_name, status, rows, error),
    )
