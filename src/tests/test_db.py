from datetime import datetime, timezone

from arena.db import ArenaDB
from arena.models import Market, Outcome


def test_db_initializes_and_persists_market(tmp_path):
    db = ArenaDB(tmp_path / "arena.db")
    db.initialize()
    market = Market(
        market_id="m1",
        venue="polymarket",
        slug="hk-temp",
        question="Will Hong Kong reach 28C?",
        category="weather",
        market_type="binary",
        outcomes=[Outcome("yes", "Yes", 0.55, 0.57, 0.56)],
        resolution_source="HKO",
        end_time=datetime.now(timezone.utc),
        volume_usd=1000.0,
        liquidity_usd=500.0,
        status="active",
    )
    db.upsert_market(market)
    rows = db.list_markets(category="weather", status="active")
    assert len(rows) == 1
    assert rows[0]["market_id"] == "m1"
