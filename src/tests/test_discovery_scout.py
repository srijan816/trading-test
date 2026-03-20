from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from arena.db import ArenaDB
from arena.intelligence.discovery import SignalType
from arena.intelligence.discovery_scout import DiscoveryUniverseScanner, extract_yes_no_asks, score_adjacent_market


def _market_row(**overrides):
    base = {
        "market_id": "m1",
        "venue": "polymarket",
        "slug": "sample-market",
        "question": "Will BTC be between $100k-$105k on April 1?",
        "category": "crypto",
        "event_group": None,
        "market_type": "binary",
        "outcomes_json": json.dumps(
            [
                {"label": "Yes", "best_ask": 0.44},
                {"label": "No", "best_ask": 0.58},
            ]
        ),
        "resolution_source": "Polymarket",
        "end_time": (datetime.now(UTC) + timedelta(hours=24)).isoformat(),
        "volume_usd": 125000.0,
        "liquidity_usd": 85000.0,
        "status": "active",
        "resolved_outcome_id": None,
        "fetched_at": datetime.now(UTC).isoformat(),
    }
    base.update(overrides)
    return base


def test_score_adjacent_market_prefers_liquid_structured_markets():
    strong = score_adjacent_market(_market_row())
    weak = score_adjacent_market(
        _market_row(
            question="Will ETH close green?",
            volume_usd=500.0,
            liquidity_usd=300.0,
            end_time=(datetime.now(UTC) + timedelta(hours=500)).isoformat(),
        )
    )
    assert strong > weak


def test_extract_yes_no_asks_reads_binary_outcomes():
    yes_ask, no_ask = extract_yes_no_asks(_market_row())
    assert yes_ask == 0.44
    assert no_ask == 0.58


def test_weather_city_expansion_flags_missing_city_support(tmp_path: Path):
    db = ArenaDB(tmp_path / "arena.db")
    db.initialize()
    row = _market_row(
        market_id="weather-1",
        question="Will the highest temperature in Tokyo be 30°C or higher on April 1?",
        category="weather",
    )
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO markets (
                market_id, venue, slug, question, category, event_group, market_type,
                outcomes_json, resolution_source, end_time, volume_usd, liquidity_usd,
                status, resolved_outcome_id, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["market_id"],
                row["venue"],
                row["slug"],
                row["question"],
                row["category"],
                row["event_group"],
                row["market_type"],
                row["outcomes_json"],
                row["resolution_source"],
                row["end_time"],
                row["volume_usd"],
                row["liquidity_usd"],
                row["status"],
                row["resolved_outcome_id"],
                row["fetched_at"],
            ),
        )

    scanner = DiscoveryUniverseScanner(db)
    with db.connect() as conn:
        active_rows = list(conn.execute("SELECT * FROM markets WHERE status = 'active'"))
    signals = scanner._build_weather_city_signals(active_rows)
    assert len(signals) == 1
    _, signal = signals[0]
    assert signal.signal_type == SignalType.MARKET_EXPANSION
    assert "Tokyo" in signal.headline
