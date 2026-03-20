from datetime import datetime, timedelta, timezone

from arena.main import _position_exit_signal
from arena.models import Position


def _position(avg_entry_price: float = 0.50) -> Position:
    return Position(
        position_id="p1",
        strategy_id="algo_forecast",
        market_id="m1",
        venue="polymarket",
        outcome_id="yes",
        outcome_label="Yes",
        side="long",
        quantity=10.0,
        avg_entry_price=avg_entry_price,
        current_price=avg_entry_price,
        unrealized_pnl=0.0,
        entry_time=datetime.now(timezone.utc) - timedelta(hours=2),
        entry_decision_id="d1",
    )


def _candidate(best_bid: float, predicted_yes: float, hours_remaining: float) -> dict:
    end_time = datetime.now(timezone.utc) + timedelta(hours=hours_remaining)
    return {
        "predicted_yes": predicted_yes,
        "yes_outcome": {"best_bid": best_bid, "mid_price": best_bid + 0.01},
        "no_outcome": {"best_bid": 1.0 - best_bid, "mid_price": 1.0 - best_bid + 0.01},
        "market": {"end_time": end_time.isoformat()},
    }


def test_position_exit_signal_edge_reversal():
    signal = _position_exit_signal(_position(0.50), _candidate(best_bid=0.62, predicted_yes=0.55, hours_remaining=24), datetime.now(timezone.utc))
    assert signal is not None
    assert signal["reason"] == "edge_reversal"


def test_position_exit_signal_stop_loss():
    signal = _position_exit_signal(_position(0.50), _candidate(best_bid=0.40, predicted_yes=0.55, hours_remaining=24), datetime.now(timezone.utc))
    assert signal is not None
    assert signal["reason"] == "stop_loss"


def test_position_exit_signal_time_exit():
    signal = _position_exit_signal(_position(0.50), _candidate(best_bid=0.49, predicted_yes=0.50, hours_remaining=6), datetime.now(timezone.utc))
    assert signal is not None
    assert signal["reason"] == "time_exit"
