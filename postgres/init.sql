-- Run once when the Postgres container is first created.
-- The ETL pipeline also runs CREATE TABLE IF NOT EXISTS, so this is
-- just a convenience for local dev / first-boot exploration.

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

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_md_instrument ON market_data (instrument_id);
CREATE INDEX IF NOT EXISTS idx_md_ts         ON market_data (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_md_outlier    ON market_data (is_outlier) WHERE is_outlier = TRUE;