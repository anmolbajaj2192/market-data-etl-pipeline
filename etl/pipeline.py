"""
AlphaPlus Capital - ETL Pipeline
---------------------------------
Extracts market data from the Mock API, validates, transforms (VWAP +
outlier detection), and loads into PostgreSQL - with idempotency guarantees.
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from pydantic import BaseModel, ValidationError, field_validator

# ---------------------------------------------------------------------------
# Structured logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("etl.pipeline")

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------
API_URL        = os.getenv("API_URL",        "http://api:8000/v1/market-data")
DB_HOST        = os.getenv("DB_HOST",        "db")
DB_PORT        = int(os.getenv("DB_PORT",    "5432"))
DB_NAME        = os.getenv("DB_NAME",        "alphaplus")
DB_USER        = os.getenv("DB_USER",        "etl_user")
DB_PASSWORD    = os.getenv("DB_PASSWORD",    "changeme")
POLL_INTERVAL  = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))
MAX_RETRIES    = int(os.getenv("MAX_RETRIES",            "3"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "5"))
OUTLIER_THRESHOLD = float(os.getenv("OUTLIER_THRESHOLD", "0.15"))  # 15 %

# ---------------------------------------------------------------------------
# Pydantic schema
# ---------------------------------------------------------------------------

class MarketRecord(BaseModel):
    instrument_id: str
    price: float
    volume: float
    timestamp: str

    @field_validator("instrument_id")
    @classmethod
    def instrument_not_empty(cls, v: Any) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("instrument_id must be a non-empty string")
        return v.strip().upper()

    @field_validator("price", "volume", mode="before")
    @classmethod
    def must_be_positive_number(cls, v: Any) -> float:
        try:
            val = float(v)
        except (TypeError, ValueError):
            raise ValueError(f"Expected numeric value, got {v!r}")
        if val <= 0:
            raise ValueError(f"Value must be > 0, got {val}")
        return val

    @field_validator("timestamp")
    @classmethod
    def valid_iso_timestamp(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise ValueError("timestamp must be a string")
        # Basic ISO-8601 check – real production would use dateutil.parser
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"Invalid ISO 8601 timestamp: {v!r}")
        return v


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )


def ensure_schema(conn: psycopg2.extensions.connection) -> None:
    """Create tables if they don't exist (idempotent DDL)."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                id              BIGSERIAL PRIMARY KEY,
                instrument_id   VARCHAR(20)    NOT NULL,
                price           NUMERIC(18,6)  NOT NULL,
                volume          NUMERIC(18,6)  NOT NULL,
                timestamp       TIMESTAMPTZ    NOT NULL,
                vwap            NUMERIC(18,6),
                is_outlier      BOOLEAN        DEFAULT FALSE,
                ingested_at     TIMESTAMPTZ    DEFAULT NOW(),
                CONSTRAINT uq_instrument_ts UNIQUE (instrument_id, timestamp)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS etl_run_log (
                id              BIGSERIAL PRIMARY KEY,
                run_at          TIMESTAMPTZ DEFAULT NOW(),
                records_fetched INT,
                records_valid   INT,
                records_dropped INT,
                records_written INT,
                records_dupes   INT,
                execution_ms    INT,
                status          VARCHAR(20)
            );
        """)
        conn.commit()
    logger.info("DB schema verified / created")


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def fetch_batch() -> list[dict[str, Any]] | None:
    """
    Poll the API with retry/back-off.
    Returns the raw JSON list, or None on unrecoverable error.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(API_URL, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 500:
                logger.warning("API returned 500 (attempt %d/%d) - retrying", attempt, MAX_RETRIES)
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                logger.error("Unexpected response shape: %s", type(data))
                return None
            return data
        except requests.exceptions.Timeout:
            logger.warning("Request timed out (attempt %d/%d)", attempt, MAX_RETRIES)
            time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as exc:
            logger.error("Network error: %s (attempt %d/%d)", exc, attempt, MAX_RETRIES)
            time.sleep(2 ** attempt)

    logger.error("All %d retries exhausted – skipping batch", MAX_RETRIES)
    return None


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------

def validate_records(
    raw: list[dict[str, Any]],
) -> tuple[list[MarketRecord], int]:
    """
    Run Pydantic validation.  Returns (valid_records, dropped_count).
    """
    valid, dropped = [], 0
    for item in raw:
        try:
            valid.append(MarketRecord(**item))
        except (ValidationError, TypeError) as exc:
            dropped += 1
            logger.warning("Validation failure – record dropped: %s | raw=%s", exc, item)
    return valid, dropped


def compute_vwap(records: list[MarketRecord]) -> dict[str, float]:
    """VWAP = Σ(price × volume) / Σ(volume) per instrument."""
    pv_sum: dict[str, float] = {}
    v_sum:  dict[str, float] = {}
    for r in records:
        pv_sum[r.instrument_id] = pv_sum.get(r.instrument_id, 0.0) + r.price * r.volume
        v_sum[r.instrument_id]  = v_sum.get(r.instrument_id, 0.0)  + r.volume

    return {
        inst: pv_sum[inst] / v_sum[inst]
        for inst in pv_sum
        if v_sum[inst] > 0
    }


def detect_outliers(
    records: list[MarketRecord],
    threshold: float = OUTLIER_THRESHOLD,
) -> dict[str, bool]:
    """
    Returns a mapping of (instrument_id, timestamp) → is_outlier.
    Flags records whose price deviates > threshold from the batch average.
    """
    # Compute simple mean per instrument for the current batch
    price_sum: dict[str, float] = {}
    price_cnt: dict[str, int]   = {}
    for r in records:
        price_sum[r.instrument_id] = price_sum.get(r.instrument_id, 0.0) + r.price
        price_cnt[r.instrument_id] = price_cnt.get(r.instrument_id, 0) + 1

    mean_price = {
        inst: price_sum[inst] / price_cnt[inst]
        for inst in price_sum
    }

    outliers: dict[tuple[str, str], bool] = {}
    for r in records:
        avg = mean_price[r.instrument_id]
        deviation = abs(r.price - avg) / avg if avg else 0.0
        key = (r.instrument_id, r.timestamp)
        outliers[key] = deviation > threshold
        if outliers[key]:
            logger.warning(
                "Outlier detected: %s @ %s | price=%.4f avg=%.4f dev=%.2f%%",
                r.instrument_id, r.timestamp, r.price, avg, deviation * 100,
            )
    return outliers


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def upsert_records(
    conn: psycopg2.extensions.connection,
    records: list[MarketRecord],
    vwap_map: dict[str, float],
    outlier_map: dict[tuple[str, str], bool],
) -> tuple[int, int]:
    """
    Insert records; skip duplicates via ON CONFLICT DO NOTHING.
    Returns (written, duplicates_skipped).
    """
    rows = [
        (
            r.instrument_id,
            r.price,
            r.volume,
            r.timestamp,
            vwap_map.get(r.instrument_id),
            outlier_map.get((r.instrument_id, r.timestamp), False),
        )
        for r in records
    ]

    sql = """
        INSERT INTO market_data
            (instrument_id, price, volume, timestamp, vwap, is_outlier)
        VALUES %s
        ON CONFLICT (instrument_id, timestamp) DO NOTHING
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
        written = cur.rowcount  # rows actually inserted
        conn.commit()

    duplicates = len(rows) - written
    return written, duplicates


def log_run(
    conn: psycopg2.extensions.connection,
    fetched: int,
    valid: int,
    dropped: int,
    written: int,
    dupes: int,
    elapsed_ms: int,
    status: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_run_log
                (records_fetched, records_valid, records_dropped,
                 records_written, records_dupes, execution_ms, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (fetched, valid, dropped, written, dupes, elapsed_ms, status),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------

def run_once(conn: psycopg2.extensions.connection) -> None:
    t0 = time.monotonic()
    logger.info("=== ETL run started ===")

    # --- Extract -------------------------------------------------------
    raw = fetch_batch()
    if raw is None:
        log_run(conn, 0, 0, 0, 0, 0, 0, "FETCH_ERROR")
        return

    fetched = len(raw)
    logger.info("Fetched %d raw records from API", fetched)

    # --- Validate ------------------------------------------------------
    valid_records, dropped = validate_records(raw)
    logger.info(
        "Validation complete - valid=%d dropped=%d",
        len(valid_records), dropped,
    )

    if not valid_records:
        logger.warning("No valid records to process; skipping load")
        elapsed = int((time.monotonic() - t0) * 1000)
        log_run(conn, fetched, 0, dropped, 0, 0, elapsed, "NO_VALID_RECORDS")
        return

    # --- Transform -----------------------------------------------------
    vwap_map    = compute_vwap(valid_records)
    outlier_map = detect_outliers(valid_records)

    outlier_count = sum(1 for v in outlier_map.values() if v)
    logger.info(
        "Transformation done - VWAP computed for %d instruments, %d outliers flagged",
        len(vwap_map), outlier_count,
    )

    # --- Load ----------------------------------------------------------
    written, dupes = upsert_records(conn, valid_records, vwap_map, outlier_map)
    logger.info("Load complete - written=%d duplicates_skipped=%d", written, dupes)

    elapsed = int((time.monotonic() - t0) * 1000)

    # --- Structured summary log ----------------------------------------
    logger.info(
        "ETL run summary | fetched=%d valid=%d dropped=%d written=%d dupes=%d elapsed_ms=%d",
        fetched, len(valid_records), dropped, written, dupes, elapsed,
    )

    log_run(conn, fetched, len(valid_records), dropped, written, dupes, elapsed, "SUCCESS")


def wait_for_db(retries: int = 15, delay: int = 3) -> psycopg2.extensions.connection:
    """Retry DB connection on startup (container ordering)."""
    for attempt in range(1, retries + 1):
        try:
            conn = get_db_connection()
            logger.info("Database connection established")
            return conn
        except psycopg2.OperationalError as exc:
            logger.warning("DB not ready yet (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(delay)
    logger.error("Could not connect to database after %d attempts - exiting", retries)
    sys.exit(1)


def main() -> None:
    logger.info("AlphaPlus ETL pipeline starting up")
    conn = wait_for_db()
    ensure_schema(conn)

    logger.info(
        "Polling %s every %ds | outlier_threshold=%.0f%%",
        API_URL, POLL_INTERVAL, OUTLIER_THRESHOLD * 100,
    )

    while True:
        try:
            run_once(conn)
        except Exception as exc:
            logger.exception("Unexpected error in run_once: %s", exc)
            # Reconnect in case of a stale connection
            try:
                conn.close()
            except Exception:
                pass
            conn = wait_for_db()

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
