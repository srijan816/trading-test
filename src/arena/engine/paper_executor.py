from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from arena.db import ArenaDB
from arena.engine.portfolio import apply_execution_to_portfolio, compute_position_unrealized
from arena.engine.risk import validate_action
from arena.models import ExecutionResult, OrderBookSnapshot, Position, ProposedAction, new_id, utc_now


@dataclass(slots=True)
class FillSimulation:
    filled_quantity: float
    avg_fill_price: float
    remaining_amount: float


def simulate_fill(levels: list[tuple[float, float]], amount_usd: float) -> FillSimulation:
    remaining = amount_usd
    total_cost = 0.0
    total_qty = 0.0
    for price, quantity in levels:
        if price <= 0:
            continue
        max_notional = price * quantity
        take = min(remaining, max_notional)
        qty = take / price
        total_qty += qty
        total_cost += take
        remaining -= take
        if remaining <= 1e-9:
            break
    avg = total_cost / total_qty if total_qty else 0.0
    return FillSimulation(total_qty, avg, max(remaining, 0.0))


class PaperExecutor:
    def __init__(self, db: ArenaDB, extra_slippage_bps: int = 50) -> None:
        self.db = db
        self.extra_slippage_bps = extra_slippage_bps

    def execute(
        self,
        decision_id: str,
        strategy_id: str,
        action: ProposedAction,
        orderbook: OrderBookSnapshot,
        portfolio,
        risk_limits: dict,
        fee_bps: float,
    ) -> tuple[ExecutionResult, Position | None]:
        risk_result = validate_action(
            portfolio,
            action,
            max_position_pct=float(risk_limits["max_position_pct"]),
            max_positions=int(risk_limits["max_positions"]),
            max_daily_loss_pct=float(risk_limits["max_daily_loss_pct"]),
        )
        if not risk_result.ok:
            execution = ExecutionResult(
                execution_id=new_id("exec"),
                decision_id=decision_id,
                strategy_id=strategy_id,
                timestamp=utc_now(),
                action_type=action.action_type,
                market_id=action.market_id,
                venue=action.venue,
                outcome_id=action.outcome_id,
                status="rejected",
                requested_amount_usd=action.amount_usd,
                filled_quantity=0.0,
                avg_fill_price=0.0,
                slippage_applied=0.0,
                fees_applied=0.0,
                total_cost=0.0,
                rejection_reason=risk_result.reason,
                orderbook_snapshot_id=orderbook.snapshot_id,
            )
            return execution, None
        levels = (
            sorted(orderbook.asks, key=lambda level: level[0])
            if action.action_type == "BUY"
            else sorted(orderbook.bids, key=lambda level: level[0], reverse=True)
        )
        fill = simulate_fill(levels, action.amount_usd)
        if fill.filled_quantity <= 0:
            execution = ExecutionResult(
                execution_id=new_id("exec"),
                decision_id=decision_id,
                strategy_id=strategy_id,
                timestamp=utc_now(),
                action_type=action.action_type,
                market_id=action.market_id,
                venue=action.venue,
                outcome_id=action.outcome_id,
                status="rejected",
                requested_amount_usd=action.amount_usd,
                filled_quantity=0.0,
                avg_fill_price=0.0,
                slippage_applied=0.0,
                fees_applied=0.0,
                total_cost=0.0,
                rejection_reason="insufficient_liquidity",
                orderbook_snapshot_id=orderbook.snapshot_id,
            )
            return execution, None
        slippage = fill.avg_fill_price * (self.extra_slippage_bps / 10000)
        avg_fill = min(fill.avg_fill_price + slippage, 1.0) if action.action_type == "BUY" else max(fill.avg_fill_price - slippage, 0.0)
        fees = action.amount_usd * (fee_bps / 10000)
        total_cost = fill.filled_quantity * avg_fill + fees
        status = "partial" if fill.remaining_amount > 0 else "filled"
        execution = ExecutionResult(
            execution_id=new_id("exec"),
            decision_id=decision_id,
            strategy_id=strategy_id,
            timestamp=utc_now(),
            action_type=action.action_type,
            market_id=action.market_id,
            venue=action.venue,
            outcome_id=action.outcome_id,
            status=status,
            requested_amount_usd=action.amount_usd,
            filled_quantity=fill.filled_quantity,
            avg_fill_price=avg_fill,
            slippage_applied=slippage,
            fees_applied=fees,
            total_cost=total_cost,
            rejection_reason=None,
            orderbook_snapshot_id=orderbook.snapshot_id,
        )
        side = "long" if action.action_type == "BUY" else "short"
        position = Position(
            position_id=new_id("pos"),
            strategy_id=strategy_id,
            market_id=action.market_id,
            venue=action.venue,
            outcome_id=action.outcome_id,
            outcome_label=action.outcome_label,
            side=side,
            quantity=fill.filled_quantity,
            avg_entry_price=avg_fill,
            current_price=orderbook.mid,
            unrealized_pnl=compute_position_unrealized(
                Position(
                    position_id="preview",
                    strategy_id=strategy_id,
                    market_id=action.market_id,
                    venue=action.venue,
                    outcome_id=action.outcome_id,
                    outcome_label=action.outcome_label,
                    side=side,
                    quantity=fill.filled_quantity,
                    avg_entry_price=avg_fill,
                    current_price=orderbook.mid,
                    unrealized_pnl=0.0,
                    entry_time=utc_now(),
                    entry_decision_id=decision_id,
                ),
                orderbook.mid,
            ),
            entry_time=utc_now(),
            entry_decision_id=decision_id,
        )
        return execution, position
