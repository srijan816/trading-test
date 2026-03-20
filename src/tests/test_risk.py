from arena.engine.risk import validate_action
from arena.models import Portfolio, ProposedAction


def test_risk_checks_insufficient_cash():
    portfolio = Portfolio("s1", 10.0, [], 10.0, 0.0, 0.0, 0, 0, 0, 0.0, 10.0)
    action = ProposedAction("BUY", "m1", "polymarket", "yes", "Yes", 20.0, 0.6, "test")
    result = validate_action(portfolio, action, max_position_pct=0.5, max_positions=5, max_daily_loss_pct=0.1)
    assert not result.ok
    assert result.reason == "insufficient_cash"
