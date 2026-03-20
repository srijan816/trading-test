from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json
import os

from arena.db import ArenaDB
from arena.engine.order_types import LimitOrder, OrderSide, OrderStatus, OrderUpdate, PlacedOrder
from arena.engine.portfolio import apply_execution_to_portfolio
from arena.models import ExecutionResult, OrderBookSnapshot, Position, new_id, utc_now


class LimitOrderManager:
    """
    Manages limit orders across their full lifecycle.

    Maker execution is different from the taker-style `PaperExecutor` already
    used in `main.py`: instead of lifting the ask immediately, we post a resting
    order inside the spread and wait for the market to trade through us. That
    avoids paying spread plus taker fees on every fill and lets the system
    capture some of the edge it currently gives away.
    """

    ACTIVE_STATUSES = (
        OrderStatus.PENDING.value,
        OrderStatus.OPEN.value,
        OrderStatus.PARTIALLY_FILLED.value,
        OrderStatus.STALE.value,
    )

    def __init__(self, db_path: str | None = None, venue_adapter=None, config: dict | None = None):
        self.config = self._merged_config(config or {})
        self.db_path = str(db_path or self.config.get("db_path", ""))
        self.db = ArenaDB(Path(self.db_path)) if self.db_path else None
        if self.db is not None:
            self.db.initialize()
        self.venue_adapter = venue_adapter

    @classmethod
    def new(cls, *_args, **_kwargs) -> LimitOrderManager:
        return cls()

    async def place_limit_order(self, order: LimitOrder) -> PlacedOrder:
        """
        Place a maker order, validating that the quote rests inside the spread.

        If the requested price would cross the spread, we shift it back inside
        the book using `compute_limit_price()` so the order remains maker-only.
        """
        if self.db is None:
            raise RuntimeError("LimitOrderManager requires a db_path for persistence.")
        orderbook = await self._fetch_orderbook_for_order(order)
        target_price = order.price
        if orderbook is not None:
            best_bid, best_ask = self._best_bid_ask(orderbook)
            if not self._is_inside_spread(target_price, best_bid, best_ask):
                adjusted = self.compute_limit_price(
                    order.side,
                    orderbook,
                    self.config,
                    model_probability=order.model_probability,
                )
                if adjusted is None:
                    raise ValueError("No profitable maker price exists inside the current spread.")
                target_price = adjusted
        target_price = round(max(min(float(target_price), 0.99), 0.01), 4)

        internal_order_id = new_id("lmt")
        submitted_order = replace(
            order,
            price=target_price,
            metadata={**order.metadata, "order_id": internal_order_id},
        )
        placed_at = utc_now()

        with self.db.connect() as conn:
            self._insert_limit_order(conn, internal_order_id, submitted_order, status=OrderStatus.PENDING, placed_at=placed_at)
            self._insert_event(
                conn,
                internal_order_id,
                "submitted",
                old_status=None,
                new_status=OrderStatus.PENDING.value,
                details={"limit_price": target_price},
            )

        venue_response = await self._place_with_venue(submitted_order)
        placed_order = self._coerce_placed_order(internal_order_id, submitted_order, venue_response, placed_at)

        with self.db.connect() as conn:
            self._update_order_row(
                conn,
                placed_order.order_id,
                venue_order_id=placed_order.venue_order_id,
                status=placed_order.status,
                fill_price=placed_order.fill_price,
                fill_quantity=placed_order.fill_quantity,
                filled_at=placed_order.filled_at,
                cancel_reason=placed_order.cancel_reason,
                metadata=placed_order.order.metadata,
            )
            if placed_order.status != OrderStatus.PENDING:
                self._insert_event(
                    conn,
                    placed_order.order_id,
                    "accepted",
                    old_status=OrderStatus.PENDING.value,
                    new_status=placed_order.status.value,
                    details={"venue_order_id": placed_order.venue_order_id},
                )
        return placed_order

    async def monitor_orders(self) -> list[OrderUpdate]:
        """
        Poll open maker orders, persist status changes, and book fills.

        Filled quantities are processed incrementally so a partial fill can be
        booked once and later topped up without double-counting.
        """
        if self.db is None:
            return []
        open_orders = self._load_open_orders()
        if not open_orders:
            return []

        current_orderbooks = await self._fetch_current_orderbooks(open_orders)
        updates = await self._poll_updates(open_orders, current_orderbooks)
        if not updates:
            return []

        snapshot_ids = {
            key: getattr(orderbook, "snapshot_id", None)
            for key, orderbook in current_orderbooks.items()
            if orderbook is not None
        }

        for update in updates:
            placed_order = next((item for item in open_orders if item.order_id == update.order_id), None)
            if placed_order is None:
                continue
            fill_quantity = update.fill_quantity
            fill_price = update.fill_price

            with self.db.connect() as conn:
                existing_row = conn.execute(
                    "SELECT fill_quantity, metadata_json FROM limit_orders WHERE order_id = ?",
                    (update.order_id,),
                ).fetchone()
                existing_fill_qty = float(existing_row["fill_quantity"] or 0.0) if existing_row else 0.0
                existing_metadata = json.loads(existing_row["metadata_json"] or "{}") if existing_row and existing_row["metadata_json"] else {}
                processed_fill_qty = float(existing_metadata.get("processed_fill_quantity", 0.0))
                effective_fill_qty = float(fill_quantity if fill_quantity is not None else existing_fill_qty)
                metadata = dict(placed_order.order.metadata)
                metadata.update(existing_metadata)

                self._update_order_row(
                    conn,
                    update.order_id,
                    status=update.new_status,
                    fill_price=fill_price if fill_price is not None else placed_order.fill_price,
                    fill_quantity=effective_fill_qty if fill_quantity is not None else existing_fill_qty,
                    filled_at=update.timestamp if update.new_status == OrderStatus.FILLED else None,
                    metadata=metadata,
                )
                self._insert_event(
                    conn,
                    update.order_id,
                    "status_change",
                    old_status=update.old_status.value,
                    new_status=update.new_status.value,
                    details={
                        "fill_price": fill_price,
                        "fill_quantity": fill_quantity,
                        "timestamp": update.timestamp.isoformat(),
                    },
                )

            newly_processed = max(effective_fill_qty - processed_fill_qty, 0.0)
            if newly_processed > 1e-9 and fill_price is not None:
                snapshot_id = self._snapshot_id_for_order(placed_order, snapshot_ids)
                await self._book_fill(placed_order, fill_price, newly_processed, update.new_status, snapshot_id=snapshot_id)
                with self.db.connect() as conn:
                    metadata = {**placed_order.order.metadata, "processed_fill_quantity": effective_fill_qty}
                    self._update_order_row(conn, update.order_id, metadata=metadata)

            if update.new_status == OrderStatus.EXPIRED and self.config.get("auto_replace_expired"):
                await self._replace_expired_order(placed_order, current_orderbooks)

        return updates

    async def reprice_order(self, order_id: str, new_price: float) -> PlacedOrder:
        """
        Cancel and replace an order to keep it near the top of the maker book.
        """
        if self.db is None:
            raise RuntimeError("LimitOrderManager requires a db_path for persistence.")
        existing = self._get_placed_order(order_id)
        if existing is None:
            raise KeyError(f"Unknown limit order: {order_id}")

        await self.cancel_order(order_id, reason="repriced")
        replacement = replace(
            existing.order,
            price=float(new_price),
            metadata={**existing.order.metadata, "replaces": existing.order_id},
        )
        placed = await self.place_limit_order(replacement)
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE limit_orders SET replaced_by = ? WHERE order_id = ?",
                (placed.order_id, order_id),
            )
            conn.execute(
                "UPDATE limit_orders SET replaces = ? WHERE order_id = ?",
                (order_id, placed.order_id),
            )
        return placed

    async def cancel_order(self, order_id: str, reason: str) -> bool:
        if self.db is None:
            return False
        placed_order = self._get_placed_order(order_id)
        if placed_order is None:
            return False
        cancelled = await self._cancel_with_venue(placed_order)
        if not cancelled:
            return False
        with self.db.connect() as conn:
            self._update_order_row(
                conn,
                order_id,
                status=OrderStatus.CANCELLED,
                cancel_reason=reason,
            )
            self._insert_event(
                conn,
                order_id,
                "cancelled",
                old_status=placed_order.status.value,
                new_status=OrderStatus.CANCELLED.value,
                details={"reason": reason},
            )
        return True

    async def cancel_all(self, market_id: str | None = None) -> int:
        if self.db is None:
            return 0
        query = (
            "SELECT order_id FROM limit_orders WHERE status IN (?, ?, ?, ?)"
            if market_id is None
            else "SELECT order_id FROM limit_orders WHERE market_id = ? AND status IN (?, ?, ?, ?)"
        )
        params: tuple[Any, ...]
        if market_id is None:
            params = tuple(self.ACTIVE_STATUSES)
        else:
            params = (market_id, *self.ACTIVE_STATUSES)
        with self.db.connect() as conn:
            rows = list(conn.execute(query, params))
        cancelled = 0
        for row in rows:
            cancelled += int(await self.cancel_order(str(row["order_id"]), reason="manual"))
        return cancelled

    def compute_limit_price(
        self,
        side: str | OrderSide,
        orderbook: dict | OrderBookSnapshot,
        config: dict,
        model_probability: float | None = None,
    ) -> float | None:
        """
        Compute the best maker quote that stays inside the spread.

        Maker orders must rest strictly between the best bid and best ask.
        Crossing or matching the far side of the spread turns the order into a
        taker trade, which reintroduces the exact spread and fee costs this
        engine is meant to avoid.

        For buy orders we bias toward the bid side:
        - start from spread midpoint minus a small offset
        - never quote worse than `best_bid + tick_size`
        - never cross above `best_ask - tick_size`
        - require a positive post-fee edge versus our model probability

        `model_probability` must be the fair value of the selected contract.
        For `buy_no`, pass the fair value of the NO share, not the YES share.
        """
        normalized_side = OrderSide.from_value(side)
        tick_size = float(config.get("tick_size", 0.01))
        spread_offset = float(config.get("spread_offset", 0.005))
        min_edge = float(config.get("min_edge_after_fees", 0.01))

        best_bid, best_ask = self._best_bid_ask(orderbook)
        if best_bid is None or best_ask is None or best_ask <= best_bid:
            return None
        if (best_ask - best_bid) <= tick_size:
            return None

        midpoint = (best_bid + best_ask) / 2.0
        inside_bid = best_bid + tick_size
        inside_ask = best_ask - tick_size
        if inside_bid > inside_ask:
            return None

        if normalized_side.is_buy:
            raw_price = midpoint - spread_offset
            our_price = max(raw_price, inside_bid)
            our_price = min(our_price, inside_ask)
        else:
            raw_price = midpoint + spread_offset
            our_price = min(raw_price, inside_ask)
            our_price = max(our_price, inside_bid)

        rounded = round(round(our_price / tick_size) * tick_size, 4)
        rounded = max(inside_bid, min(rounded, inside_ask))
        rounded = round(max(min(rounded, 0.99), 0.01), 4)
        if rounded <= best_bid or rounded >= best_ask:
            return None

        if model_probability is not None:
            if normalized_side.is_buy:
                expected_edge = float(model_probability) - rounded
            else:
                expected_edge = rounded - float(model_probability)
            if expected_edge < min_edge:
                return None

        return rounded

    def _merged_config(self, config: dict[str, Any]) -> dict[str, Any]:
        return {
            "tick_size": float(config.get("tick_size", os.getenv("LIMIT_ORDER_TICK_SIZE", "0.01"))),
            "spread_offset": float(config.get("spread_offset", os.getenv("LIMIT_ORDER_SPREAD_OFFSET", "0.005"))),
            "min_edge_after_fees": float(config.get("min_edge_after_fees", os.getenv("LIMIT_ORDER_MIN_EDGE_AFTER_FEES", "0.01"))),
            "order_ttl_seconds": int(config.get("order_ttl_seconds", os.getenv("LIMIT_ORDER_TTL_SECONDS", "300"))),
            "reprice_interval_seconds": int(config.get("reprice_interval_seconds", os.getenv("LIMIT_ORDER_REPRICE_INTERVAL_SECONDS", "45"))),
            "stale_after_seconds": int(config.get("stale_after_seconds", os.getenv("LIMIT_ORDER_STALE_AFTER_SECONDS", "120"))),
            "stale_price_delta": float(config.get("stale_price_delta", os.getenv("LIMIT_ORDER_STALE_PRICE_DELTA", "0.02"))),
            "auto_replace_expired": bool(config.get("auto_replace_expired", False)),
            "default_starting_balance": float(config.get("default_starting_balance", 1000.0)),
            "allow_partial_fills": bool(config.get("allow_partial_fills", True)),
            "random_fill_min_seconds": int(config.get("random_fill_min_seconds", 5)),
            "random_fill_max_seconds": int(config.get("random_fill_max_seconds", 60)),
            **config,
        }

    async def _fetch_orderbook_for_order(self, order: LimitOrder):
        if self.venue_adapter is None or not hasattr(self.venue_adapter, "get_orderbook"):
            return None
        outcome_id = order.metadata.get("outcome_id") or order.metadata.get("token_id")
        if outcome_id is None:
            return None
        orderbook = await self.venue_adapter.get_orderbook(order.market_id, outcome_id)
        if self.db is not None and isinstance(orderbook, OrderBookSnapshot):
            self.db.save_orderbook_snapshot(orderbook)
        return orderbook

    async def _place_with_venue(self, order: LimitOrder):
        if self.venue_adapter is None or not hasattr(self.venue_adapter, "place_order"):
            return {
                "venue_order_id": f"paper_{new_id('venue')}",
                "status": OrderStatus.OPEN.value,
            }
        return await self.venue_adapter.place_order(order)

    async def _cancel_with_venue(self, placed_order: PlacedOrder) -> bool:
        if self.venue_adapter is None or not hasattr(self.venue_adapter, "cancel_order"):
            return True
        try:
            return bool(await self.venue_adapter.cancel_order(placed_order.venue_order_id or placed_order.order_id))
        except TypeError:
            return bool(await self.venue_adapter.cancel_order(placed_order.order_id))

    def _coerce_placed_order(self, order_id: str, order: LimitOrder, venue_response, placed_at: datetime) -> PlacedOrder:
        if isinstance(venue_response, PlacedOrder):
            venue_response.order_id = order_id
            return venue_response
        if isinstance(venue_response, dict):
            return PlacedOrder(
                order_id=order_id,
                venue_order_id=str(venue_response.get("venue_order_id", order_id)),
                order=order,
                status=OrderStatus(str(venue_response.get("status", OrderStatus.OPEN.value))),
                placed_at=placed_at,
                fill_price=venue_response.get("fill_price"),
                fill_quantity=venue_response.get("fill_quantity"),
            )
        raise TypeError(f"Unsupported venue response type: {type(venue_response)!r}")

    def _insert_limit_order(self, conn, order_id: str, order: LimitOrder, *, status: OrderStatus, placed_at: datetime) -> None:
        conn.execute(
            """
            INSERT INTO limit_orders (
                order_id, venue_order_id, market_id, strategy_id, side, limit_price,
                size_dollars, quantity, model_probability, edge_bps, status,
                ttl_seconds, placed_at, updated_at, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                None,
                order.market_id,
                order.strategy_id,
                order.side.value,
                order.price,
                order.size_dollars,
                order.quantity,
                order.model_probability,
                order.edge_bps,
                status.value,
                order.ttl_seconds,
                placed_at.isoformat(),
                placed_at.isoformat(),
                json.dumps(order.metadata, sort_keys=True),
            ),
        )

    def _update_order_row(
        self,
        conn,
        order_id: str,
        *,
        venue_order_id: str | None = None,
        status: OrderStatus | None = None,
        fill_price: float | None = None,
        fill_quantity: float | None = None,
        filled_at: datetime | None = None,
        cancel_reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        assignments = ["updated_at = ?"]
        params: list[Any] = [utc_now().isoformat()]
        if venue_order_id is not None:
            assignments.append("venue_order_id = ?")
            params.append(venue_order_id)
        if status is not None:
            assignments.append("status = ?")
            params.append(status.value if isinstance(status, OrderStatus) else str(status))
        if fill_price is not None:
            assignments.append("fill_price = ?")
            params.append(float(fill_price))
        if fill_quantity is not None:
            assignments.append("fill_quantity = ?")
            params.append(float(fill_quantity))
        if filled_at is not None:
            assignments.append("filled_at = ?")
            params.append(filled_at.isoformat())
        if cancel_reason is not None:
            assignments.append("cancel_reason = ?")
            params.append(cancel_reason)
        if metadata is not None:
            assignments.append("metadata_json = ?")
            params.append(json.dumps(metadata, sort_keys=True))
        params.append(order_id)
        conn.execute(f"UPDATE limit_orders SET {', '.join(assignments)} WHERE order_id = ?", params)

    def _insert_event(
        self,
        conn,
        order_id: str,
        event_type: str,
        *,
        old_status: str | None,
        new_status: str | None,
        details: dict[str, Any] | None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO order_events (order_id, event_type, old_status, new_status, details_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                order_id,
                event_type,
                old_status,
                new_status,
                json.dumps(details, sort_keys=True) if details is not None else None,
            ),
        )

    def _load_open_orders(self) -> list[PlacedOrder]:
        if self.db is None:
            return []
        placeholders = ",".join("?" for _ in self.ACTIVE_STATUSES)
        with self.db.connect() as conn:
            rows = list(
                conn.execute(
                    f"SELECT * FROM limit_orders WHERE status IN ({placeholders}) ORDER BY placed_at ASC",
                    self.ACTIVE_STATUSES,
                )
            )
        return [self._row_to_placed_order(row) for row in rows]

    def _get_placed_order(self, order_id: str) -> PlacedOrder | None:
        if self.db is None:
            return None
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM limit_orders WHERE order_id = ?", (order_id,)).fetchone()
        return self._row_to_placed_order(row) if row is not None else None

    def _row_to_placed_order(self, row) -> PlacedOrder:
        metadata = json.loads(row["metadata_json"] or "{}") if row["metadata_json"] else {}
        limit_order = LimitOrder(
            market_id=str(row["market_id"]),
            side=OrderSide(str(row["side"])),
            price=float(row["limit_price"]),
            size_dollars=float(row["size_dollars"]),
            quantity=float(row["quantity"]),
            strategy_id=str(row["strategy_id"]),
            model_probability=float(row["model_probability"] or 0.0),
            edge_bps=int(row["edge_bps"] or 0),
            ttl_seconds=int(row["ttl_seconds"] or self.config.get("order_ttl_seconds", 300)),
            metadata=metadata,
        )
        return PlacedOrder(
            order_id=str(row["order_id"]),
            venue_order_id=str(row["venue_order_id"] or row["order_id"]),
            order=limit_order,
            status=OrderStatus(str(row["status"])),
            placed_at=self._parse_dt(row["placed_at"]),
            filled_at=self._parse_dt(row["filled_at"]) if row["filled_at"] else None,
            fill_price=float(row["fill_price"]) if row["fill_price"] is not None else None,
            fill_quantity=float(row["fill_quantity"]) if row["fill_quantity"] is not None else None,
            cancel_reason=str(row["cancel_reason"]) if row["cancel_reason"] else None,
        )

    async def _fetch_current_orderbooks(self, orders: list[PlacedOrder]) -> dict[Any, Any]:
        if self.venue_adapter is None or not hasattr(self.venue_adapter, "get_orderbook"):
            return {}
        current_orderbooks: dict[Any, Any] = {}
        for placed_order in orders:
            outcome_id = placed_order.order.metadata.get("outcome_id") or placed_order.order.metadata.get("token_id")
            if outcome_id is None:
                continue
            key = (placed_order.order.market_id, str(outcome_id))
            if key in current_orderbooks:
                continue
            orderbook = await self.venue_adapter.get_orderbook(placed_order.order.market_id, outcome_id)
            current_orderbooks[key] = orderbook
            current_orderbooks.setdefault(placed_order.order.market_id, orderbook)
            if self.db is not None and isinstance(orderbook, OrderBookSnapshot):
                self.db.save_orderbook_snapshot(orderbook)
        return current_orderbooks

    async def _poll_updates(self, open_orders: list[PlacedOrder], current_orderbooks: dict[Any, Any]) -> list[OrderUpdate]:
        if self.venue_adapter is None:
            return []
        if hasattr(self.venue_adapter, "check_fills"):
            return await self.venue_adapter.check_fills(open_orders, current_orderbooks)
        if hasattr(self.venue_adapter, "get_order_status"):
            updates: list[OrderUpdate] = []
            for placed_order in open_orders:
                status_payload = await self.venue_adapter.get_order_status(placed_order.venue_order_id)
                status_value = str(status_payload.get("status", placed_order.status.value)).lower()
                new_status = OrderStatus(status_value)
                if new_status != placed_order.status:
                    updates.append(
                        OrderUpdate(
                            order_id=placed_order.order_id,
                            old_status=placed_order.status,
                            new_status=new_status,
                            fill_price=status_payload.get("fill_price"),
                            fill_quantity=status_payload.get("fill_quantity"),
                        )
                    )
            return updates
        return []

    async def _book_fill(
        self,
        placed_order: PlacedOrder,
        fill_price: float,
        fill_quantity: float,
        status: OrderStatus,
        *,
        snapshot_id: str | None,
    ) -> None:
        if self.db is None or fill_quantity <= 0:
            return
        if not placed_order.order.side.is_buy:
            with self.db.connect() as conn:
                self._insert_event(
                    conn,
                    placed_order.order_id,
                    "fill_not_booked",
                    old_status=status.value,
                    new_status=status.value,
                    details={"reason": "sell-side portfolio accounting is not implemented in the long-only engine"},
                )
            return

        portfolio = self.db.ensure_portfolio(
            placed_order.order.strategy_id,
            float(self.config.get("default_starting_balance", 1000.0)),
        )
        venue = str(placed_order.order.metadata.get("venue", "polymarket"))
        outcome_id = str(
            placed_order.order.metadata.get("outcome_id")
            or placed_order.order.metadata.get("token_id")
            or placed_order.order.market_id
        )
        outcome_label = str(placed_order.order.metadata.get("outcome_label") or placed_order.order.side.outcome_label)
        decision_id = str(placed_order.order.metadata.get("decision_id") or f"limit_{placed_order.order_id}")
        execution = ExecutionResult(
            execution_id=new_id("exec"),
            decision_id=decision_id,
            strategy_id=placed_order.order.strategy_id,
            timestamp=utc_now(),
            action_type="BUY",
            market_id=placed_order.order.market_id,
            venue=venue,
            outcome_id=outcome_id,
            status="filled" if status == OrderStatus.FILLED else "partial",
            requested_amount_usd=round(fill_quantity * fill_price, 6),
            filled_quantity=fill_quantity,
            avg_fill_price=fill_price,
            slippage_applied=0.0,
            fees_applied=0.0,
            total_cost=round(fill_quantity * fill_price, 6),
            rejection_reason=None,
            orderbook_snapshot_id=snapshot_id or new_id("book"),
        )
        position = Position(
            position_id=new_id("pos"),
            strategy_id=placed_order.order.strategy_id,
            market_id=placed_order.order.market_id,
            venue=venue,
            outcome_id=outcome_id,
            outcome_label=outcome_label,
            side="long",
            quantity=fill_quantity,
            avg_entry_price=fill_price,
            current_price=fill_price,
            unrealized_pnl=0.0,
            entry_time=utc_now(),
            entry_decision_id=decision_id,
        )
        self.db.save_execution(execution)
        self.db.upsert_position(position)
        portfolio = apply_execution_to_portfolio(portfolio, position, execution)
        self.db.save_portfolio(portfolio)

    async def _replace_expired_order(self, placed_order: PlacedOrder, current_orderbooks: dict[Any, Any]) -> None:
        orderbook = self._resolve_orderbook(placed_order, current_orderbooks)
        if orderbook is None:
            return
        new_price = self.compute_limit_price(
            placed_order.order.side,
            orderbook,
            self.config,
            model_probability=placed_order.order.model_probability,
        )
        if new_price is None:
            return
        await self.reprice_order(placed_order.order_id, new_price)

    def _resolve_orderbook(self, placed_order: PlacedOrder, current_orderbooks: dict[Any, Any]):
        outcome_id = placed_order.order.metadata.get("outcome_id") or placed_order.order.metadata.get("token_id")
        if outcome_id is not None:
            tuple_key = (placed_order.order.market_id, str(outcome_id))
            if tuple_key in current_orderbooks:
                return current_orderbooks[tuple_key]
        return current_orderbooks.get(placed_order.order.market_id)

    def _best_bid_ask(self, orderbook: dict | OrderBookSnapshot) -> tuple[float | None, float | None]:
        bids = getattr(orderbook, "bids", None)
        asks = getattr(orderbook, "asks", None)
        if bids is None and isinstance(orderbook, dict):
            bids = orderbook.get("bids", [])
        if asks is None and isinstance(orderbook, dict):
            asks = orderbook.get("asks", [])

        normalized_bids = [self._normalize_level(level) for level in bids or []]
        normalized_asks = [self._normalize_level(level) for level in asks or []]
        normalized_bids = [level for level in normalized_bids if level is not None]
        normalized_asks = [level for level in normalized_asks if level is not None]

        best_bid = max((price for price, _ in normalized_bids), default=None)
        best_ask = min((price for price, _ in normalized_asks), default=None)
        return best_bid, best_ask

    def _normalize_level(self, level) -> tuple[float, float] | None:
        if isinstance(level, dict):
            if level.get("price") is None:
                return None
            return float(level["price"]), float(level.get("size", 0.0) or 0.0)
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            return float(level[0]), float(level[1])
        return None

    def _is_inside_spread(self, price: float, best_bid: float | None, best_ask: float | None) -> bool:
        return best_bid is not None and best_ask is not None and best_bid < price < best_ask

    def _snapshot_id_for_order(self, placed_order: PlacedOrder, snapshot_ids: dict[Any, str | None]) -> str | None:
        outcome_id = placed_order.order.metadata.get("outcome_id") or placed_order.order.metadata.get("token_id")
        if outcome_id is not None:
            tuple_key = (placed_order.order.market_id, str(outcome_id))
            if tuple_key in snapshot_ids:
                return snapshot_ids[tuple_key]
        return snapshot_ids.get(placed_order.order.market_id)

    def _parse_dt(self, value: str) -> datetime:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
