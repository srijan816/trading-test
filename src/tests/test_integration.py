from datetime import datetime, timedelta, timezone
import asyncio

from arena.db import ArenaDB
from arena.engine.paper_executor import PaperExecutor
from arena.engine.portfolio import apply_execution_to_portfolio
from arena.engine.settlement import SettlementEngine
from arena.models import Market, OrderBookSnapshot, Outcome
from arena.strategies.algo_forecast import ForecastConsensusStrategy


def test_vertical_slice_algo_trade_and_settlement(tmp_path):
    db = ArenaDB(tmp_path / "arena.db")
    db.initialize()
    db.ensure_portfolio("algo_forecast", 1000.0)
    market = Market(
        market_id="m1",
        venue="polymarket",
        slug="hk-temp",
        question="Will Hong Kong hit 28C?",
        category="weather",
        market_type="binary",
        outcomes=[Outcome("yes", "Yes", 0.45, 0.47, 0.46), Outcome("no", "No", 0.53, 0.55, 0.54)],
        resolution_source="HKO",
        end_time=datetime.now(timezone.utc) + timedelta(hours=1),
        volume_usd=12000.0,
        liquidity_usd=7000.0,
        status="active",
    )
    db.upsert_market(market)
    strategy = ForecastConsensusStrategy(
        db,
        {
            "id": "algo_forecast",
            "risk": {"min_edge_bps": 800, "max_position_pct": 0.08, "max_positions": 5, "max_daily_loss_pct": 0.10},
            "starting_balance": 1000.0,
        },
    )
    decision = asyncio.run(strategy.generate_decision())
    db.save_decision(decision)
    action = decision.actions[0]
    orderbook = OrderBookSnapshot("m1", "yes", "polymarket", datetime.now(timezone.utc), [(0.44, 100)], [(0.46, 100)], 0.45, 0.02)
    db.save_orderbook_snapshot(orderbook)
    portfolio = db.get_portfolio("algo_forecast")
    executor = PaperExecutor(db)
    execution, position = executor.execute(decision.decision_id, "algo_forecast", action, orderbook, portfolio, {"max_position_pct": 0.08, "max_positions": 5, "max_daily_loss_pct": 0.10}, fee_bps=0)
    assert execution.status in {"filled", "partial"}
    assert position is not None
    db.save_execution(execution)
    db.upsert_position(position)
    updated = apply_execution_to_portfolio(portfolio, position, execution)
    db.save_portfolio(updated)
    settlement = SettlementEngine(db)
    resolution = settlement.settle_market("m1", "polymarket", "yes", "Yes", "HKO")
    assert resolution.positions_settled
    final_portfolio = db.get_portfolio("algo_forecast")
    assert final_portfolio.realized_pnl >= 0
