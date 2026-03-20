from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from arena.backtest.orderbook_replay import OrderbookReplayHarness
from arena.engine.order_types import LimitOrder, OrderSide
from arena.models import OrderBookSnapshot


def test_orderbook_replay_harness_fills_and_scores_pnl():
    harness = OrderbookReplayHarness()
    order = LimitOrder(
        market_id="m1",
        side=OrderSide.BUY_YES,
        price=0.44,
        size_dollars=22.0,
        quantity=50.0,
        strategy_id="algo_forecast",
        model_probability=0.58,
        edge_bps=1400,
        metadata={"outcome_id": "yes"},
    )
    snapshots = [
        OrderBookSnapshot("m1", "yes", "polymarket", datetime.now(UTC), [(0.42, 100.0)], [(0.46, 100.0)], 0.44, 0.04),
        OrderBookSnapshot("m1", "yes", "polymarket", datetime.now(UTC), [(0.44, 100.0)], [(0.43, 100.0)], 0.435, 0.01),
    ]
    result = asyncio.run(harness.replay_limit_order(order, snapshots, settlement_yes_price=1.0))
    assert result.filled is True
    assert result.fill_price is not None
    assert result.realized_pnl is not None
    assert result.realized_pnl > 0.0
