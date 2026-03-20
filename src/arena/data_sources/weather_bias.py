from __future__ import annotations

import logging
from datetime import datetime, timezone

from arena.db import ArenaDB

logger = logging.getLogger(__name__)

FORECAST_HISTORY_SCHEMA = """
CREATE TABLE IF NOT EXISTS forecast_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location TEXT NOT NULL,
    source TEXT NOT NULL,
    forecast_date TEXT NOT NULL,
    target_date TEXT NOT NULL,
    predicted_high_c REAL,
    predicted_low_c REAL,
    actual_high_c REAL,
    actual_low_c REAL,
    error_high_c REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_fh_location_source
ON forecast_history(location, source, target_date);
"""


def ensure_forecast_history_table(db: ArenaDB) -> None:
    with db.connect() as conn:
        conn.executescript(FORECAST_HISTORY_SCHEMA)


async def record_forecast(
    db: ArenaDB,
    location: str,
    source: str,
    target_date: str,
    predicted_high_c: float | None,
    predicted_low_c: float | None,
) -> None:
    forecast_date = datetime.now(timezone.utc).isoformat()
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO forecast_history "
            "(location, source, forecast_date, target_date, predicted_high_c, predicted_low_c) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (location.lower(), source, forecast_date, target_date, predicted_high_c, predicted_low_c),
        )


async def get_bias_correction(db: ArenaDB, location: str, source: str | None = None) -> dict:
    query = (
        "SELECT predicted_high_c, actual_high_c "
        "FROM forecast_history "
        "WHERE location = ? AND actual_high_c IS NOT NULL"
    )
    params: list = [location.lower()]
    if source:
        query += " AND source = ?"
        params.append(source)
    query += " ORDER BY created_at DESC LIMIT 30"

    with db.connect() as conn:
        rows = list(conn.execute(query, params))

    n = len(rows)
    if n < 5:
        return {"bias_c": 0.0, "sample_size": n, "reliable": False}

    errors = [float(row["predicted_high_c"]) - float(row["actual_high_c"]) for row in rows]
    mean_error = sum(errors) / len(errors)
    return {"bias_c": round(mean_error, 3), "sample_size": n, "reliable": True}


async def backfill_actuals(
    db: ArenaDB,
    location: str,
    target_date: str,
    actual_high_c: float,
    actual_low_c: float,
) -> int:
    with db.connect() as conn:
        cursor = conn.execute(
            "UPDATE forecast_history SET "
            "actual_high_c = ?, actual_low_c = ?, error_high_c = predicted_high_c - ? "
            "WHERE location = ? AND target_date = ?",
            (actual_high_c, actual_low_c, actual_high_c, location.lower(), target_date),
        )
        return cursor.rowcount
