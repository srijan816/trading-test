from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from arena.models import Portfolio, ProposedAction


@dataclass(slots=True)
class RiskCheckResult:
    ok: bool
    reason: str | None = None


def validate_action(
    portfolio: Portfolio,
    action: ProposedAction,
    max_position_pct: float,
    max_positions: int,
    max_daily_loss_pct: float,
) -> RiskCheckResult:
    if portfolio.peak_value > 0:
        daily_drawdown = max((portfolio.peak_value - portfolio.total_value) / portfolio.peak_value, 0.0)
        if daily_drawdown >= max_daily_loss_pct:
            return RiskCheckResult(False, "daily_loss_cap_hit")
    if action.amount_usd > portfolio.cash:
        return RiskCheckResult(False, "insufficient_cash")
    if portfolio.total_value > 0 and action.amount_usd / portfolio.total_value > max_position_pct:
        return RiskCheckResult(False, "position_limit_exceeded")
    if len(portfolio.positions) >= max_positions:
        return RiskCheckResult(False, "max_positions_reached")
    return RiskCheckResult(True)
