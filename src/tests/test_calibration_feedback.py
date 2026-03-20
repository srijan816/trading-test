from __future__ import annotations

import sqlite3
from pathlib import Path

from arena.calibration.resolution_hook import compute_sigma_adjustment
from arena.data_sources.weather_ensemble import load_latest_sigma


def test_compute_sigma_adjustment_widens_fast_when_ratio_is_extreme():
    adjusted = compute_sigma_adjustment(1.2, 12.5, 5)
    assert adjusted > 3.0


def test_compute_sigma_adjustment_can_narrow_when_ratio_is_low():
    adjusted = compute_sigma_adjustment(2.0, 0.7, 8)
    assert adjusted < 2.0


def test_load_latest_sigma_prefers_metric_specific_adjustment(tmp_path: Path):
    db_path = tmp_path / "sigma.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE parameter_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT,
                city TEXT,
                parameter_name TEXT,
                current_value REAL,
                recommended_value REAL,
                reason TEXT,
                auto_applied INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO parameter_adjustments
            (strategy_id, city, parameter_name, current_value, recommended_value, reason)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("weather_ensemble", "Dallas", "ensemble_sigma", 1.2, 1.6, "generic"),
        )
        conn.execute(
            """
            INSERT INTO parameter_adjustments
            (strategy_id, city, parameter_name, current_value, recommended_value, reason)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("weather_ensemble", "Dallas", "ensemble_sigma_low", 1.1, 2.4, "low metric"),
        )
        conn.commit()
    finally:
        conn.close()

    assert load_latest_sigma(db_path, "Dallas", metric="low") == 2.4
    assert load_latest_sigma(db_path, "Dallas", metric="high") == 1.6
