from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from arena.engine.order_types import LimitOrder, OrderSide, OrderStatus, PlacedOrder
from arena.engine.paper_limit_executor import PaperLimitExecutor
from arena.models import OrderBookSnapshot


@dataclass(slots=True)
class ReplayResult:
    filled: bool
    fill_price: float | None
    fill_quantity: float
    snapshots_seen: int
    realized_pnl: float | None


class OrderbookReplayHarness:
    """Replay a resting paper limit order against historical orderbook snapshots."""

    def __init__(self, config: dict | None = None) -> None:
        merged = {
            "random_fill_min_seconds": 0,
            "random_fill_max_seconds": 0,
            "allow_partial_fills": True,
            **(config or {}),
        }
        self.executor = PaperLimitExecutor(config=merged)

    async def replay_limit_order(
        self,
        order: LimitOrder,
        snapshots: Iterable[OrderBookSnapshot],
        *,
        settlement_yes_price: float | None = None,
    ) -> ReplayResult:
        placed = await self.executor.place_order(order)
        replayed = 0
        latest_state: PlacedOrder | None = placed

        for snapshot in snapshots:
            replayed += 1
            updates = await self.executor.check_fills(
                [latest_state],
                {(order.market_id, order.metadata.get("outcome_id")): snapshot},
            )
            for update in updates:
                if update.order_id != latest_state.order_id:
                    continue
                latest_state = PlacedOrder(
                    order_id=latest_state.order_id,
                    venue_order_id=latest_state.venue_order_id,
                    order=latest_state.order,
                    status=update.new_status,
                    placed_at=latest_state.placed_at,
                    filled_at=update.timestamp if update.new_status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED} else latest_state.filled_at,
                    fill_price=update.fill_price or latest_state.fill_price,
                    fill_quantity=update.fill_quantity or latest_state.fill_quantity,
                    cancel_reason=latest_state.cancel_reason,
                )
            if latest_state.status == OrderStatus.FILLED:
                break

        realized_pnl = None
        if latest_state.fill_price is not None and settlement_yes_price is not None:
            payout_price = settlement_yes_price if order.side == OrderSide.BUY_YES else (1.0 - settlement_yes_price)
            realized_pnl = (payout_price - latest_state.fill_price) * float(latest_state.fill_quantity or 0.0)

        return ReplayResult(
            filled=latest_state.status == OrderStatus.FILLED,
            fill_price=latest_state.fill_price,
            fill_quantity=float(latest_state.fill_quantity or 0.0),
            snapshots_seen=replayed,
            realized_pnl=realized_pnl,
        )
