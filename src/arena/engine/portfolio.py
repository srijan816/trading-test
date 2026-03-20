from __future__ import annotations

from dataclasses import replace

from arena.models import ExecutionResult, Portfolio, Position, utc_now


def compute_position_unrealized(position: Position, current_price: float) -> float:
    return (current_price - position.avg_entry_price) * position.quantity


def apply_execution_to_portfolio(portfolio: Portfolio, position: Position | None, execution: ExecutionResult) -> Portfolio:
    updated_positions = list(portfolio.positions)
    cash = portfolio.cash
    total_trades = portfolio.total_trades
    if execution.status in {"filled", "partial"} and execution.filled_quantity > 0:
        cash -= execution.total_cost
        total_trades += 1
        if position:
            updated_positions.append(position)
    unrealized = sum(item.unrealized_pnl for item in updated_positions if item.status == "open")
    total_value = cash + sum(item.quantity * item.current_price for item in updated_positions if item.status == "open")
    peak_value = max(portfolio.peak_value, total_value)
    drawdown = max((peak_value - total_value) / peak_value, portfolio.max_drawdown) if peak_value else portfolio.max_drawdown
    return Portfolio(
        strategy_id=portfolio.strategy_id,
        cash=cash,
        positions=updated_positions,
        total_value=total_value,
        realized_pnl=portfolio.realized_pnl,
        unrealized_pnl=unrealized,
        total_trades=total_trades,
        winning_trades=portfolio.winning_trades,
        losing_trades=portfolio.losing_trades,
        max_drawdown=drawdown,
        peak_value=peak_value,
        updated_at=utc_now(),
    )


def close_position(portfolio: Portfolio, position_id: str, payout: float) -> Portfolio:
    updated_positions: list[Position] = []
    realized_pnl = portfolio.realized_pnl
    cash = portfolio.cash
    winning_trades = portfolio.winning_trades
    losing_trades = portfolio.losing_trades
    for position in portfolio.positions:
        if position.position_id != position_id:
            updated_positions.append(position)
            continue
        cost_basis = position.quantity * position.avg_entry_price
        pnl = payout - cost_basis
        realized_pnl += pnl
        cash += payout
        if pnl >= 0:
            winning_trades += 1
        else:
            losing_trades += 1
        updated_positions.append(replace(position, status="closed", current_price=1.0 if payout > 0 else 0.0, unrealized_pnl=0.0, last_updated_at=utc_now()))
    open_positions = [item for item in updated_positions if item.status == "open"]
    unrealized = sum(item.unrealized_pnl for item in open_positions)
    total_value = cash + sum(item.quantity * item.current_price for item in open_positions)
    peak_value = max(portfolio.peak_value, total_value)
    drawdown = max((peak_value - total_value) / peak_value, portfolio.max_drawdown) if peak_value else portfolio.max_drawdown
    return Portfolio(
        strategy_id=portfolio.strategy_id,
        cash=cash,
        positions=updated_positions,
        total_value=total_value,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized,
        total_trades=portfolio.total_trades,
        winning_trades=winning_trades,
        losing_trades=losing_trades,
        max_drawdown=drawdown,
        peak_value=peak_value,
        updated_at=utc_now(),
    )
