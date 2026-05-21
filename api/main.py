"""
Mock Market Data API
Simulates a real-time financial data feed with chaos engineering built in.
"""

import random
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Response
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="AlphaPlus Market Data API", version="1.0.0")

# ---------------------------------------------------------------------------
# Instrument universe with realistic base prices
# ---------------------------------------------------------------------------
INSTRUMENTS: dict[str, float] = {
    "AAPL":   185.0,
    "MSFT":   415.0,
    "GOOGL":  175.0,
    "AMZN":   190.0,
    "TSLA":   175.0,
    "BTC-USD": 68000.0,
    "ETH-USD":  3500.0,
    "NFLX":   640.0,
    "NVDA":   875.0,
    "META":   500.0,
}

FAULT_RATE = 0.05          # 5 % of requests are faulty


class MarketRecord(BaseModel):
    instrument_id: str
    price: float
    volume: float
    timestamp: str


def _make_clean_records() -> list[dict[str, Any]]:
    """Generate one valid record per instrument."""
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for instrument, base_price in INSTRUMENTS.items():
        price  = round(base_price * random.uniform(0.98, 1.02), 4)
        volume = round(random.uniform(100, 10_000), 2)
        records.append({
            "instrument_id": instrument,
            "price":         price,
            "volume":        volume,
            "timestamp":     now,
        })
    return records


def _inject_fault(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Randomly corrupt one field in a random record."""
    bad = random.choice(records).copy()
    fault_type = random.choice(["bad_price", "bad_volume", "missing_field", "null_id"])

    if fault_type == "bad_price":
        bad["price"] = "NOT_A_NUMBER"
        logger.warning("Fault injected: bad_price on %s", bad["instrument_id"])
    elif fault_type == "bad_volume":
        bad["volume"] = None
        logger.warning("Fault injected: null_volume on %s", bad["instrument_id"])
    elif fault_type == "missing_field":
        bad.pop("timestamp", None)
        logger.warning("Fault injected: missing timestamp on %s", bad["instrument_id"])
    else:
        bad["instrument_id"] = None
        logger.warning("Fault injected: null instrument_id")

    # Replace the record in-place
    idx = random.randrange(len(records))
    records[idx] = bad
    return records


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health_check():
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/v1/market-data")
def market_data(response: Response):
    """
    Return a batch of synthetic market records.
    5 % chance: return HTTP 500  OR  return a batch containing one malformed record.
    """
    roll = random.random()

    # Hard 500 error (half of the 5 %)
    if roll < FAULT_RATE / 2:
        logger.error("Chaos: returning HTTP 500")
        response.status_code = 500
        return {"error": "Internal Server Error - chaos fault injected"}

    records = _make_clean_records()# this fnx is interal/private and store list of dictionaries

    # Malformed data (other half of the 5 %)
    if roll < FAULT_RATE:
        records = _inject_fault(records)
        logger.warning("Chaos: serving batch with one malformed record")

    logger.info("Serving %d records", len(records))
    return records