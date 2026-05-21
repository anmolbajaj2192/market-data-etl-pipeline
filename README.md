# AlphaPlus Capital – End-to-End Data Engineering Challenge

A fully containerised, production-ready data ecosystem consisting of a **Mock Market Data API**, a **PostgreSQL sink**, and a **Python ETL pipeline** — all orchestrated with Docker Compose.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Quick Start](#quick-start)
3. [Project Structure](#project-structure)
4. [Component Details](#component-details)
   - [Task 1 – Mock API](#task-1--mock-api-fastapi)
   - [Task 2 – ETL Pipeline](#task-2--etl-pipeline)
   - [Task 3 – Infrastructure](#task-3--infrastructure-docker)
5. [Configuration Reference](#configuration-reference)
6. [System Design Q&A](#system-design-qa)
   - [Scaling to 1 Billion Events / Day](#1-scaling-to-1-billion-events--day)
   - [Production Health Checks](#2-production-health-checks--monitoring)
   - [Idempotency & Recovery](#3-idempotency--pipeline-recovery)
7. [Verifying the Pipeline](#verifying-the-pipeline)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│                   Docker Network (bridge)            │
│                                                      │
│  ┌───────────────┐   HTTP Poll    ┌────────────────┐ │
│  │  FastAPI      │ ◄──────────── │  ETL Pipeline  │ │
│  │  :8000        │               │  (Python)      │ │
│  │  /v1/market-  │               │                │ │
│  │  data         │               │  • Validate    │ │
│  │               │               │  • VWAP        │ │
│  │  5% Chaos     │               │  • Outliers    │ │
│  │  Injection    │               │  • Upsert      │ │
│  └───────────────┘               └───────┬────────┘ │
│                                          │ psycopg2 │
│                                  ┌───────▼────────┐  │
│                                  │  PostgreSQL 16 │  │
│                                  │  market_data   │  │
│                                  │  etl_run_log   │  │
│                                  └────────────────┘  │
└──────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites

- Docker ≥ 24
- Docker Compose v2 (bundled with Docker Desktop or `docker compose` plugin)

### 1. Clone the repository

```bash
git clone <repo-url>
cd alphaplus-data-engineering
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env if you want custom DB credentials (defaults work out of the box)
```

### 3. Launch everything with one command

```bash
docker compose up --build
```

This starts three services in dependency order:
1. **db** (PostgreSQL) — waits until healthy
2. **api** (FastAPI) — waits until healthy
3. **etl** (Python pipeline) — polls the API every 10 seconds

### 4. Stop and clean up

```bash
docker compose down -v   # -v also removes the named volume
```

---

## Project Structure

```
.
├── api/
│   ├── Dockerfile
│   ├── main.py            # FastAPI server with chaos injection
│   └── requirements.txt
├── etl/
│   ├── Dockerfile
│   ├── pipeline.py        # Full ETL with validation, VWAP, outliers
│   └── requirements.txt
├── postgres/
│   └── init.sql           # Schema + indexes (idempotent DDL)
├── docker-compose.yml
├── .env                   # Real secrets (git-ignored)
├── .env.example           # Template committed to repo
├── .gitignore
└── README.md
```

---

## Component Details

### Task 1 – Mock API (FastAPI)

**Endpoint:** `GET /v1/market-data`

Returns a JSON array of market records for 10 instruments
(`AAPL`, `MSFT`, `GOOGL`, `AMZN`, `TSLA`, `BTC-USD`, `ETH-USD`, `NFLX`, `NVDA`, `META`).

```json
[
  {
    "instrument_id": "AAPL",
    "price": 186.3241,
    "volume": 4823.17,
    "timestamp": "2024-05-18T14:32:01.123456+00:00"
  },
  ...
]
```

**Chaos Engineering (5% fault rate):**

| Fault type | Probability | Behaviour |
|---|---|---|
| HTTP 500 | 2.5 % | Returns `{"error": "Internal Server Error"}` with status 500 |
| Malformed record | 2.5 % | One record in the batch gets a corrupted field (bad price string, null volume, missing timestamp, or null instrument_id) |

**Health check:** `GET /health` → `{"status": "ok", "ts": "..."}`

---

### Task 2 – ETL Pipeline

#### Extraction

- Polls `GET /v1/market-data` on a configurable interval (default: 10 s).
- Retries up to 3 times with **exponential back-off** (2 s → 4 s → 8 s) on HTTP 500 or network timeout.
- Abandons a batch (logs `FETCH_ERROR`) after all retries fail; does not crash.

#### Validation (Pydantic v2)

Each record is validated against `MarketRecord`:

| Field | Rule |
|---|---|
| `instrument_id` | Non-empty string |
| `price` | Numeric, > 0 |
| `volume` | Numeric, > 0 |
| `timestamp` | Valid ISO-8601 string |

Invalid records are **dropped and counted**; they never reach the database.

#### Transformation

**VWAP** per instrument across the current batch:

```
VWAP(i) = Σ(price × volume) / Σ(volume)   for all records of instrument i
```

**Outlier Detection:** A record is flagged `is_outlier = TRUE` if:

```
|price - mean_price(instrument)| / mean_price(instrument) > 0.15
```

#### Loading (Idempotency)

```sql
INSERT INTO market_data (...)
ON CONFLICT (instrument_id, timestamp) DO NOTHING
```

The `UNIQUE` constraint on `(instrument_id, timestamp)` ensures no duplicates are written regardless of how many times the same batch is replayed.

#### Structured Logging

Every run emits a summary log line:

```json
{"time":"...","level":"INFO","message":"ETL run summary | fetched=10 valid=9 dropped=1 written=9 dupes=0 elapsed_ms=43"}
```

A persistent audit trail is also written to the `etl_run_log` table.

---

### Task 3 – Infrastructure (Docker)

- **Separate Dockerfiles** for `api` and `etl` services (each with a minimal `python:3.12-slim` base).
- **`docker-compose.yml`** orchestrates all three services on a private bridge network (`alphaplus_net`).
- **Secret management** via `.env` file; credentials are injected via `env_file` — nothing is hard-coded.
- **`depends_on` with `condition: service_healthy`** ensures the ETL only starts after the DB and API pass their health checks.
- **Named volume** (`pg_data`) persists PostgreSQL data across container restarts.

---

## Configuration Reference

All tunable parameters are exposed via environment variables (set in `.env`):

| Variable | Default | Description |
|---|---|---|
| `DB_NAME` | `alphaplus` | PostgreSQL database name |
| `DB_USER` | `etl_user` | Database user |
| `DB_PASSWORD` | — | **Required** – database password |
| `DB_PORT` | `5432` | PostgreSQL port |
| `POLL_INTERVAL_SECONDS` | `10` | Seconds between API polls |
| `MAX_RETRIES` | `3` | Retry attempts on API failure |
| `REQUEST_TIMEOUT_SECONDS` | `5` | HTTP request timeout |
| `OUTLIER_THRESHOLD` | `0.15` | Outlier deviation threshold (15 %) |

---

## System Design Q&A

### 1. Scaling to 1 Billion Events / Day

The current polling-based ETL pipeline is suitable for small to medium workloads, but it would not scale efficiently for billions of events per day. To handle large-scale streaming data, the architecture could be redesigned into separate ingestion, processing, and storage layers.

#### Ingestion Layer — Apache Kafka

Instead of directly polling the API, incoming market events could be published to Kafka topics partitioned by `instrument_id`. Kafka provides high-throughput event streaming, decouples producers from consumers, and allows replaying events if needed.

#### Processing Layer — Spark Structured Streaming / Flink

Distributed stream-processing frameworks such as Spark Structured Streaming or Flink could consume data from Kafka and process events in parallel. VWAP calculation and outlier detection could run continuously on streaming data across multiple worker nodes.

#### Storage Layer

Validated records could be stored in cloud object storage such as S3 or GCS in Parquet format for long-term analytics. Analytical systems like Snowflake or BigQuery could then be used for querying and reporting at scale. PostgreSQL would still be useful for smaller operational workloads or recent data access.

#### Cloud-Native Alternative

Managed cloud services could also replace self-hosted infrastructure:

| Component | AWS | GCP |
|---|---|---|
| Message Streaming | Kinesis Data Streams | Pub/Sub |
| Stream Processing | Kinesis Data Analytics | Dataflow |
| Storage | S3 + Redshift | GCS + BigQuery |
| Orchestration | MWAA (Airflow) | Cloud Composer |

---

### 2. Production Health Checks & Monitoring

For a production deployment, the system should include health checks, metrics collection, and alerting to ensure reliability and quick issue detection.

#### Infrastructure Health Checks

- The API service can expose a `/health` endpoint to confirm that the application is running.
- PostgreSQL readiness can be checked using `pg_isready` (already configured in `docker-compose.yml`).
- ETL health can be verified by checking whether recent successful runs exist in the `etl_run_log` table.

#### Metrics Collection (Prometheus + Grafana)

The ETL service could expose a `/metrics` endpoint using `prometheus_client`. Prometheus would collect metrics, and Grafana would visualize them through dashboards.

Example metrics:

| Metric | Purpose |
|---|---|
| `etl_records_processed_total` | Total processed records |
| `etl_records_dropped_total` | Invalid/dropped records |
| `etl_run_duration_seconds` | ETL execution time |
| `etl_api_errors_total` | API request failures |
| `etl_outliers_flagged_total` | Number of outliers detected |

Alerts can be configured if:
- processing stops,
- API error rates increase,
- ETL duration becomes unusually high,
- or dropped records spike significantly.

#### Alerting

Grafana alerts or cloud monitoring services can send notifications to Slack, email when failures or abnormal behavior are detected.

---

### 3. Idempotency & Pipeline Recovery

#### Idempotent Writes

The pipeline prevents duplicate records using the database constraint:

```sql
UNIQUE (instrument_id, timestamp)
```

combined with:

```sql
ON CONFLICT DO NOTHING
```

This ensures that even if the same batch is processed multiple times after a failure or retry, duplicate records are not inserted into the database.

#### Recovery for Large Batches

For large datasets, the pipeline could process records in smaller chunks instead of loading everything at once.

A checkpoint table could store:
- the last processed chunk,
- processing status,
- and timestamps.

If the pipeline crashes, it can restart from the last successful checkpoint instead of reprocessing the entire batch.

#### Additional Reliability Improvements

- Database transactions can ensure that partial writes are rolled back if a failure occurs during insertion.
- Invalid records can be stored in a separate quarantine table for later inspection instead of being silently discarded.
- A schema version column can help track future data structure changes safely.

---

## Verifying the Pipeline

After `docker compose up --build`, you can inspect results directly:

```bash
# Connect to Postgres
docker exec -it alphaplus_db psql -U etl_user -d alphaplus

-- Recent ingested records
SELECT instrument_id, price, volume, vwap, is_outlier, ingested_at
FROM market_data
ORDER BY ingested_at DESC
LIMIT 20;

-- ETL run audit log
SELECT * FROM etl_run_log ORDER BY run_at DESC LIMIT 10;

-- Outlier summary
SELECT instrument_id, COUNT(*) AS outlier_count
FROM market_data
WHERE is_outlier = TRUE
GROUP BY instrument_id
ORDER BY outlier_count DESC;
```

```bash
# Watch live ETL logs
docker logs -f alphaplus_etl

# Hit the API directly
curl http://localhost:8000/v1/market-data | python3 -m json.tool
```
