from arena.db import ArenaDB
from arena.engine.paper_executor import PaperExecutor, simulate_fill
from arena.models import OrderBookSnapshot, Portfolio, ProposedAction
from datetime import datetime, timezone


def test_simulate_fill_full_and_partial():
    full = simulate_fill([(0.5, 100)], 25.0)
    assert round(full.filled_quantity, 4) == 50.0
    assert full.remaining_amount == 0.0
    partial = simulate_fill([(0.5, 10)], 25.0)
    assert round(partial.filled_quantity, 4) == 10.0
    assert round(partial.remaining_amount, 4) == 20.0


def test_executor_sorts_asks_for_buy(tmp_path):
    db = ArenaDB(tmp_path / "arena.db")
    db.initialize()
    executor = PaperExecutor(db, extra_slippage_bps=0)
    portfolio = Portfolio("s1", 100.0, [], 100.0, 0.0, 0.0, 0, 0, 0, 0.0, 100.0)
    action = ProposedAction("BUY", "m1", "polymarket", "yes", "Yes", 10.0, 1.0, "test")
    orderbook = OrderBookSnapshot(
        "m1",
        "yes",
        "polymarket",
        datetime.now(timezone.utc),
        [],
        [(0.9, 10.0), (0.2, 100.0), (0.5, 10.0)],
        0.2,
        0.7,
    )
    execution, _ = executor.execute(
        "d1",
        "s1",
        action,
        orderbook,
        portfolio,
        {"max_position_pct": 1.0, "max_positions": 3, "max_daily_loss_pct": 1.0},
        fee_bps=0,
    )
    assert execution.status == "filled"
    assert round(execution.avg_fill_price, 4) == 0.2


def test_executor_rejects_risk_limit(tmp_path):
    db = ArenaDB(tmp_path / "arena.db")
    db.initialize()
    executor = PaperExecutor(db)
    portfolio = Portfolio("s1", 100.0, [], 100.0, 0.0, 0.0, 0, 0, 0, 0.0, 100.0)
    action = ProposedAction("BUY", "m1", "polymarket", "yes", "Yes", 20.0, 0.55, "test")
    orderbook = OrderBookSnapshot("m1", "yes", "polymarket", datetime.now(timezone.utc), [(0.49, 10)], [(0.51, 10)], 0.50, 0.02)
    execution, position = executor.execute("d1", "s1", action, orderbook, portfolio, {"max_position_pct": 0.10, "max_positions": 3, "max_daily_loss_pct": 0.10}, fee_bps=0)
    assert execution.status == "rejected"
    assert execution.rejection_reason == "position_limit_exceeded"
    assert position is None
