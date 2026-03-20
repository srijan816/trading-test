from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from arena.db import ArenaDB
from arena.engine.limit_order_manager import LimitOrderManager
from arena.engine.order_types import LimitOrder, OrderSide
from arena.engine.paper_limit_executor import PaperLimitExecutor
from arena.engine.settlement import SettlementEngine
from arena.models import Market, OrderBookSnapshot, Outcome


class _ReplayMarketData:
    def __init__(self, snapshots: list[OrderBookSnapshot]) -> None:
        self.snapshots = list(snapshots)
        self.index = 0

    async def get_orderbook(self, market_id: str, outcome_id: str | None = None) -> OrderBookSnapshot:
        snapshot = self.snapshots[min(self.index, len(self.snapshots) - 1)]
        if self.index < len(self.snapshots) - 1:
            self.index += 1
        return snapshot


def test_paper_limit_order_lifecycle_to_settlement(tmp_path: Path):
    db = ArenaDB(tmp_path / "arena.db")
    db.initialize()
    db.ensure_portfolio("algo_forecast", 1000.0)
    market = Market(
        market_id="weather-limit-1",
        venue="polymarket",
        slug="dallas-92f",
        question="Will the highest temperature in Dallas be 92°F or higher on April 1?",
        category="weather",
        market_type="binary",
        outcomes=[Outcome("yes", "Yes", 0.42, 0.46, 0.44), Outcome("no", "No", 0.54, 0.58, 0.56)],
        resolution_source="NWS",
        end_time=datetime.now(UTC) + timedelta(hours=6),
        volume_usd=15000.0,
        liquidity_usd=8000.0,
        status="active",
    )
    db.upsert_market(market)

    market_data = _ReplayMarketData(
        [
            OrderBookSnapshot(
                "weather-limit-1",
                "yes",
                "polymarket",
                datetime.now(UTC),
                [(0.42, 100.0)],
                [(0.46, 100.0)],
                0.44,
                0.04,
            ),
            OrderBookSnapshot(
                "weather-limit-1",
                "yes",
                "polymarket",
                datetime.now(UTC) + timedelta(seconds=5),
                [(0.44, 100.0)],
                [(0.43, 100.0)],
                0.435,
                0.01,
            ),
        ]
    )
    paper_limit_executor = PaperLimitExecutor(
        config={"random_fill_min_seconds": 0, "random_fill_max_seconds": 0, "allow_partial_fills": True},
        market_data_adapter=market_data,
    )
    manager = LimitOrderManager(
        db_path=str(db.path),
        venue_adapter=paper_limit_executor,
        config={"db_path": str(db.path), "default_starting_balance": 1000.0},
    )
    order = LimitOrder(
        market_id="weather-limit-1",
        side=OrderSide.BUY_YES,
        price=0.44,
        size_dollars=22.0,
        quantity=50.0,
        strategy_id="algo_forecast",
        model_probability=0.57,
        edge_bps=1300,
        metadata={"outcome_id": "yes", "outcome_label": "Yes", "decision_id": "decision_limit_1", "venue": "polymarket"},
    )

    placed = asyncio.run(manager.place_limit_order(order))
    assert placed.status.value == "open"
    updates = asyncio.run(manager.monitor_orders())
    assert any(update.new_status.value in {"partial", "filled"} for update in updates)

    with db.connect() as conn:
        positions = conn.execute("SELECT * FROM positions WHERE strategy_id = 'algo_forecast'").fetchall()
        executions = conn.execute("SELECT * FROM executions WHERE strategy_id = 'algo_forecast'").fetchall()
    assert positions
    assert executions

    settlement = SettlementEngine(db)
    resolution = settlement.settle_market("weather-limit-1", "polymarket", "yes", "Yes", "NWS")
    assert resolution.positions_settled

    final_portfolio = db.get_portfolio("algo_forecast")
    assert final_portfolio.realized_pnl >= 0.0
