from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import os
import random
import uuid

from arena.engine.order_types import LimitOrder, OrderStatus, OrderUpdate, PlacedOrder


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PaperLimitExecutor:
    """
    Simulates maker-style limit order behavior for paper trading.

    Unlike the existing `PaperExecutor`, this executor does not cross the book.
    Orders rest on the bid or ask and only fill after the market moves through
    the resting price and a randomized delay has elapsed.
    """

    def __init__(
        self,
        config: dict | None = None,
        seed: int | None = None,
        market_data_adapter=None,
    ) -> None:
        self.config = config or {}
        self.random = random.Random(seed if seed is not None else int(os.getenv("LIMIT_ORDER_RANDOM_SEED", "7")))
        self._orders: dict[str, dict] = {}
        self.market_data_adapter = market_data_adapter

    async def place_order(self, order: LimitOrder) -> PlacedOrder:
        """
        Record the order without filling it immediately.

        Buy orders fill on later `check_fills()` passes once the market ask is
        at or below our resting bid. Sell orders use the mirror condition on
        the best bid.
        """
        placed_at = _utc_now()
        venue_order_id = f"paper_{uuid.uuid4().hex[:12]}"
        fill_delay_seconds = self.random.randint(
            int(self.config.get("random_fill_min_seconds", 5)),
            int(self.config.get("random_fill_max_seconds", 60)),
        )
        self._orders[venue_order_id] = {
            "internal_order_id": str(order.metadata.get("order_id", "")),
            "status": OrderStatus.OPEN,
            "placed_at": placed_at,
            "fill_after": placed_at + timedelta(seconds=fill_delay_seconds),
            "filled_quantity": 0.0,
        }
        return PlacedOrder(
            order_id=str(order.metadata.get("order_id", "")),
            venue_order_id=venue_order_id,
            order=order,
            status=OrderStatus.OPEN,
            placed_at=placed_at,
        )

    async def check_fills(self, open_orders: list[PlacedOrder], current_orderbooks: dict) -> list[OrderUpdate]:
        """
        Check open paper orders against the latest market prices.

        Fill logic:
        - Buy orders are eligible once best ask <= our limit price.
        - Sell orders are eligible once best bid >= our limit price.
        - Orders expire once their TTL elapses.
        - Orders older than 2 minutes become `STALE` if the market moves 2c+
          away from the resting price, making them candidates for repricing.
        - Partial fills are possible when the displayed top-of-book size is
          smaller than our requested quantity or by random simulation.
        """
        updates: list[OrderUpdate] = []
        now = _utc_now()
        stale_after = timedelta(seconds=int(self.config.get("stale_after_seconds", 120)))
        stale_delta = float(self.config.get("stale_price_delta", 0.02))
        allow_partial = bool(self.config.get("allow_partial_fills", True))

        for placed_order in open_orders:
            state = self._orders.get(placed_order.venue_order_id)
            if state is None:
                continue

            current_status = OrderStatus(state["status"])
            if current_status in {
                OrderStatus.CANCELLED,
                OrderStatus.EXPIRED,
                OrderStatus.FILLED,
                OrderStatus.REJECTED,
            }:
                continue

            age = now - state["placed_at"]
            if age >= timedelta(seconds=placed_order.order.ttl_seconds):
                state["status"] = OrderStatus.EXPIRED
                updates.append(
                    OrderUpdate(
                        order_id=placed_order.order_id,
                        old_status=current_status,
                        new_status=OrderStatus.EXPIRED,
                        timestamp=now,
                    )
                )
                continue

            orderbook = self._resolve_orderbook(placed_order, current_orderbooks)
            if orderbook is None:
                if current_status == OrderStatus.PENDING:
                    state["status"] = OrderStatus.OPEN
                    updates.append(
                        OrderUpdate(
                            order_id=placed_order.order_id,
                            old_status=current_status,
                            new_status=OrderStatus.OPEN,
                            timestamp=now,
                        )
                    )
                continue

            best_bid, best_bid_size, best_ask, best_ask_size = self._best_levels(orderbook)
            if current_status in {OrderStatus.OPEN, OrderStatus.PENDING}:
                moved_away = (
                    best_ask is not None and (best_ask - placed_order.order.price) > stale_delta
                    if placed_order.order.side.is_buy
                    else best_bid is not None and (placed_order.order.price - best_bid) > stale_delta
                )
                if age >= stale_after and moved_away:
                    state["status"] = OrderStatus.STALE
                    updates.append(
                        OrderUpdate(
                            order_id=placed_order.order_id,
                            old_status=current_status,
                            new_status=OrderStatus.STALE,
                            timestamp=now,
                        )
                    )
                    continue
                if current_status == OrderStatus.PENDING:
                    state["status"] = OrderStatus.OPEN
                    current_status = OrderStatus.OPEN
                    updates.append(
                        OrderUpdate(
                            order_id=placed_order.order_id,
                            old_status=OrderStatus.PENDING,
                            new_status=OrderStatus.OPEN,
                            timestamp=now,
                        )
                    )

            remaining_quantity = max(placed_order.order.quantity - float(state["filled_quantity"]), 0.0)
            if remaining_quantity <= 1e-9:
                continue

            fill_ready = now >= state["fill_after"]
            crosses = (
                best_ask is not None and best_ask <= placed_order.order.price
                if placed_order.order.side.is_buy
                else best_bid is not None and best_bid >= placed_order.order.price
            )
            if not fill_ready or not crosses:
                continue

            displayed_quantity = best_ask_size if placed_order.order.side.is_buy else best_bid_size
            fill_quantity = min(remaining_quantity, displayed_quantity or remaining_quantity)
            if allow_partial and fill_quantity > 0 and remaining_quantity > fill_quantity:
                fill_ratio = self.random.uniform(0.4, 0.9)
                fill_quantity = max(fill_quantity * fill_ratio, min(remaining_quantity, 1.0))
            elif allow_partial and remaining_quantity > 1.0 and self.random.random() < 0.2:
                fill_quantity = max(remaining_quantity * self.random.uniform(0.35, 0.8), 1.0)
                fill_quantity = min(fill_quantity, remaining_quantity)

            fill_quantity = min(fill_quantity, remaining_quantity)
            if fill_quantity <= 1e-9:
                continue

            state["filled_quantity"] = float(state["filled_quantity"]) + fill_quantity
            terminal = abs(state["filled_quantity"] - placed_order.order.quantity) <= 1e-6
            new_status = OrderStatus.FILLED if terminal else OrderStatus.PARTIALLY_FILLED
            old_status = current_status if current_status != OrderStatus.STALE else OrderStatus.STALE
            state["status"] = new_status
            updates.append(
                OrderUpdate(
                    order_id=placed_order.order_id,
                    old_status=old_status,
                    new_status=new_status,
                    fill_price=placed_order.order.price,
                    fill_quantity=float(state["filled_quantity"]),
                    timestamp=now,
                )
            )

        return updates

    async def cancel_order(self, order_id: str) -> bool:
        for venue_order_id, state in self._orders.items():
            internal_id = state.get("internal_order_id")
            if order_id not in {venue_order_id, internal_id}:
                continue
            state["status"] = OrderStatus.CANCELLED
            return True
        return False

    async def get_orderbook(self, market_id: str, outcome_id: str):
        if self.market_data_adapter is None:
            return None
        return await self.market_data_adapter.get_orderbook(market_id, outcome_id)

    def _resolve_orderbook(self, placed_order: PlacedOrder, current_orderbooks: dict):
        metadata = placed_order.order.metadata
        outcome_id = metadata.get("outcome_id") or metadata.get("token_id")
        tuple_key = (placed_order.order.market_id, str(outcome_id)) if outcome_id is not None else None
        if tuple_key and tuple_key in current_orderbooks:
            return current_orderbooks[tuple_key]
        if placed_order.order.market_id in current_orderbooks:
            return current_orderbooks[placed_order.order.market_id]
        return None

    def _best_levels(self, orderbook) -> tuple[float | None, float | None, float | None, float | None]:
        bids = getattr(orderbook, "bids", None)
        asks = getattr(orderbook, "asks", None)
        if bids is None and isinstance(orderbook, dict):
            bids = orderbook.get("bids", [])
        if asks is None and isinstance(orderbook, dict):
            asks = orderbook.get("asks", [])
        normalized_bids = sorted((self._level_value(level) for level in bids or [] if self._level_value(level)), key=lambda item: item[0], reverse=True)
        normalized_asks = sorted((self._level_value(level) for level in asks or [] if self._level_value(level)), key=lambda item: item[0])
        best_bid = normalized_bids[0][0] if normalized_bids else None
        best_bid_size = normalized_bids[0][1] if normalized_bids else None
        best_ask = normalized_asks[0][0] if normalized_asks else None
        best_ask_size = normalized_asks[0][1] if normalized_asks else None
        return best_bid, best_bid_size, best_ask, best_ask_size

    def _level_value(self, level) -> tuple[float, float] | None:
        if isinstance(level, dict):
            if level.get("price") is None:
                return None
            return float(level["price"]), float(level.get("size", 0.0) or 0.0)
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            return float(level[0]), float(level[1])
        return None
