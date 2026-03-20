from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from arena.db import ArenaDB
from arena.models import ExecutionResult, new_id
from arena.risk.risk_manager import RiskManager
from arena.risk.trading_guardrails import compute_daily_pnl, get_active_trading_pause, maybe_trigger_trading_pause


def _insert_market(conn, *, market_id: str, question: str, category: str = "weather") -> None:
    conn.execute(
        """
        INSERT INTO markets (
            market_id, venue, slug, question, category, event_group, market_type,
            outcomes_json, resolution_source, end_time, volume_usd, liquidity_usd,
            status, resolved_outcome_id, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            market_id,
            "polymarket",
            market_id,
            question,
            category,
            None,
            "binary",
            json.dumps([{"label": "Yes", "best_ask": 0.45}, {"label": "No", "best_ask": 0.57}]),
            "Polymarket",
            (datetime.now(UTC) + timedelta(hours=24)).isoformat(),
            10000.0,
            5000.0,
            "active",
            None,
            datetime.now(UTC).isoformat(),
        ),
    )


def test_maybe_trigger_trading_pause_after_five_failures(tmp_path: Path):
    db = ArenaDB(tmp_path / "arena.db")
    db.initialize()
    for _ in range(5):
        db.save_execution(
            ExecutionResult(
                execution_id=new_id("exec"),
                decision_id=new_id("dec"),
                strategy_id="algo_forecast",
                timestamp=datetime.now(UTC),
                action_type="BUY_YES",
                market_id="m1",
                venue="polymarket",
                outcome_id="yes",
                status="rejected",
                requested_amount_usd=10.0,
                filled_quantity=0.0,
                avg_fill_price=0.0,
                slippage_applied=0.0,
                fees_applied=0.0,
                total_cost=0.0,
                rejection_reason="test",
                orderbook_snapshot_id=new_id("book"),
            )
        )

    pause = maybe_trigger_trading_pause(db, "algo_forecast", threshold=5, minutes=5)
    assert pause is not None
    assert get_active_trading_pause(db, "algo_forecast") is not None


def test_compute_daily_pnl_uses_closed_and_open_positions(tmp_path: Path):
    db = ArenaDB(tmp_path / "arena.db")
    db.initialize()
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO positions (
                position_id, strategy_id, market_id, venue, outcome_id, outcome_label, side,
                quantity, avg_entry_price, current_price, unrealized_pnl, realized_pnl,
                entry_time, entry_decision_id, status, last_updated_at, closed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "closed1",
                "algo_forecast",
                "m1",
                "polymarket",
                "yes",
                "Yes",
                "BUY_YES",
                10.0,
                0.4,
                0.6,
                0.0,
                -12.5,
                datetime.now(UTC).isoformat(),
                "dec1",
                "closed",
                datetime.now(UTC).isoformat(),
                datetime.now(UTC).isoformat(),
            ),
        )
        conn.execute(
            """
            INSERT INTO positions (
                position_id, strategy_id, market_id, venue, outcome_id, outcome_label, side,
                quantity, avg_entry_price, current_price, unrealized_pnl, realized_pnl,
                entry_time, entry_decision_id, status, last_updated_at, closed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "open1",
                "algo_forecast",
                "m2",
                "polymarket",
                "yes",
                "Yes",
                "BUY_YES",
                10.0,
                0.5,
                0.45,
                -3.0,
                0.0,
                datetime.now(UTC).isoformat(),
                "dec2",
                "open",
                datetime.now(UTC).isoformat(),
                None,
            ),
        )

    pnl = compute_daily_pnl(db, "algo_forecast", (datetime.now(UTC) - timedelta(hours=1)).isoformat())
    assert pnl == -15.5


def test_risk_manager_blocks_weather_city_date_concentration(tmp_path: Path):
    db = ArenaDB(tmp_path / "arena.db")
    db.initialize()
    db.ensure_portfolio("algo_forecast", 100.0)
    with db.connect() as conn:
        _insert_market(
            conn,
            market_id="weather-a",
            question="Will the highest temperature in Dallas be 92°F or higher on April 1?",
        )
        conn.execute(
            """
            INSERT INTO positions (
                position_id, strategy_id, market_id, venue, outcome_id, outcome_label, side,
                quantity, avg_entry_price, current_price, unrealized_pnl, realized_pnl,
                entry_time, entry_decision_id, status, last_updated_at, closed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "pos1",
                "algo_forecast",
                "weather-a",
                "polymarket",
                "yes",
                "Yes",
                "BUY_YES",
                75.0,
                0.4,
                0.4,
                0.0,
                0.0,
                datetime.now(UTC).isoformat(),
                "dec-weather",
                "open",
                datetime.now(UTC).isoformat(),
                None,
            ),
        )

    risk_manager = RiskManager(
        db,
        {
            "max_daily_trades": 20,
            "max_daily_loss_usd": 1000.0,
            "max_open_positions": 10,
            "max_exposure_per_market_usd": 1000.0,
            "max_total_exposure_usd": 1000.0,
            "cooldown_after_loss_streak": 10,
            "cooldown_minutes": 60,
            "max_city_date_concentration_pct": 0.35,
        },
    )
    result = asyncio.run(
        risk_manager.check_trade(
            strategy_id="algo_forecast",
            market_id="weather-a",
            amount_usd=10.0,
            side="BUY_YES",
            venue="polymarket",
        )
    )
    assert result["approved"] is False
    assert "concentration" in result["reason"].lower()
