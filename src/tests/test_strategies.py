from datetime import datetime, timedelta, timezone

from arena.db import ArenaDB
from arena.models import Market, Outcome
from arena.strategies.algo_forecast import ForecastConsensusStrategy


def test_algo_forecast_generates_decision(tmp_path):
    db = ArenaDB(tmp_path / "arena.db")
    db.initialize()
    market = Market(
        market_id="m1",
        venue="polymarket",
        slug="hk-temp",
        question="Will Hong Kong hit 28C?",
        category="weather",
        market_type="binary",
        outcomes=[Outcome("yes", "Yes", 0.45, 0.47, 0.46), Outcome("no", "No", 0.53, 0.55, 0.54)],
        resolution_source="HKO",
        end_time=datetime.now(timezone.utc) + timedelta(hours=12),
        volume_usd=10000.0,
        liquidity_usd=5000.0,
        status="active",
    )
    db.upsert_market(market)
    strategy = ForecastConsensusStrategy(
        db,
        {
            "id": "algo_forecast",
            "risk": {"min_edge_bps": 800, "max_position_pct": 0.08},
            "starting_balance": 1000.0,
        },
    )
    decision = __import__("asyncio").run(strategy.generate_decision())
    assert decision.strategy_id == "algo_forecast"
    assert decision.actions
