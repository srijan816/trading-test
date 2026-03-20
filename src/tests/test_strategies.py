from datetime import datetime, timedelta, timezone

import pytest

from arena.db import ArenaDB
from arena.models import Market, Outcome
from arena.strategies.algo_forecast import ForecastConsensusStrategy, parse_weather_question


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


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        (
            "Will the highest temperature in Dallas be 92°F or higher on March 19?",
            {"metric": "high", "direction": "above", "threshold": 92.0, "unit": "f", "canonical_city": "Dallas"},
        ),
        (
            "Will the highest temperature in Dallas be between 90-91°F on March 19?",
            {"metric": "high", "direction": "between", "lower_bound": 90.0, "upper_bound": 91.0, "unit": "f", "canonical_city": "Dallas"},
        ),
        (
            "Will it rain in London on March 20?",
            {"metric": "rain", "direction": "rain", "unit": "probability", "canonical_city": "London"},
        ),
        (
            "Will the lowest temperature in New York be 32°F or above on March 22?",
            {"metric": "low", "direction": "above", "threshold": 32.0, "unit": "f", "canonical_city": "New York"},
        ),
        (
            "Will the highest temperature in Ankara be 12°C on March 22?",
            {"metric": "high", "direction": "exact", "threshold": 12.0, "unit": "c", "canonical_city": "Ankara"},
        ),
        (
            "Will the highest temperature in Atlanta be 59°F or below on March 19?",
            {"metric": "high", "direction": "below", "threshold": 59.0, "unit": "f", "canonical_city": "Atlanta"},
        ),
    ],
)
def test_parse_weather_question_patterns(question, expected):
    params = parse_weather_question(question)
    assert params is not None
    for key, value in expected.items():
        assert getattr(params, key) == value
