from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class OrderStatus(str, Enum):
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    REJECTED = "rejected"
    STALE = "stale"


class OrderSide(str, Enum):
    BUY_YES = "buy_yes"
    BUY_NO = "buy_no"
    SELL_YES = "sell_yes"
    SELL_NO = "sell_no"

    @classmethod
    def from_value(cls, value: str | OrderSide) -> OrderSide:
        if isinstance(value, cls):
            return value
        return cls(str(value).lower())

    @property
    def is_buy(self) -> bool:
        return self in {self.BUY_YES, self.BUY_NO}

    @property
    def outcome_label(self) -> str:
        return "Yes" if self in {self.BUY_YES, self.SELL_YES} else "No"


@dataclass(slots=True)
class LimitOrder:
    market_id: str
    side: OrderSide
    price: float
    size_dollars: float
    quantity: float
    strategy_id: str
    model_probability: float
    edge_bps: int
    ttl_seconds: int = 300
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.side = OrderSide.from_value(self.side)
        self.price = float(self.price)
        self.size_dollars = float(self.size_dollars)
        self.quantity = float(self.quantity)
        self.model_probability = float(self.model_probability)
        self.edge_bps = int(self.edge_bps)
        self.ttl_seconds = int(self.ttl_seconds)

    @property
    def action_type(self) -> str:
        return "BUY" if self.side.is_buy else "SELL"

    def expires_at(self, placed_at: datetime) -> datetime:
        return placed_at + timedelta(seconds=self.ttl_seconds)


@dataclass(slots=True)
class PlacedOrder:
    order_id: str
    venue_order_id: str
    order: LimitOrder
    status: OrderStatus
    placed_at: datetime
    filled_at: datetime | None = None
    fill_price: float | None = None
    fill_quantity: float | None = None
    cancel_reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, OrderStatus):
            self.status = OrderStatus(str(self.status))


@dataclass(slots=True)
class OrderUpdate:
    order_id: str
    old_status: OrderStatus
    new_status: OrderStatus
    fill_price: float | None = None
    fill_quantity: float | None = None
    timestamp: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if not isinstance(self.old_status, OrderStatus):
            self.old_status = OrderStatus(str(self.old_status))
        if not isinstance(self.new_status, OrderStatus):
            self.new_status = OrderStatus(str(self.new_status))
