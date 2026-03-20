from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
import uuid


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class VenueOrderRecord:
    venue_order_id: str
    payload: dict[str, Any]
    status: str = "open"
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime = field(default_factory=_utc_now)


class PolymarketLimitClient:
    """
    Minimal CLOB-facing interface for maker orders.

    Paper mode keeps a local in-memory ledger so the maker engine can run
    end-to-end without a live Polymarket integration.

    Live mode is intentionally unimplemented. The read-only orderbook path in
    this repo already uses `GET /book?token_id=...`; live order submission will
    require signed CLOB requests for order creation, status polling, and order
    cancellation against the same `https://clob.polymarket.com` host. Confirm
    the exact authenticated endpoints and signing flow before enabling it.
    """

    def __init__(self, mode: str = "paper") -> None:
        self.mode = str(mode).lower()
        self._paper_orders: dict[str, VenueOrderRecord] = {}

    async def place_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.mode == "paper":
            venue_order_id = f"paper_{uuid.uuid4().hex[:12]}"
            record = VenueOrderRecord(venue_order_id=venue_order_id, payload=dict(payload))
            self._paper_orders[venue_order_id] = record
            return {"venue_order_id": venue_order_id, "status": record.status}
        raise NotImplementedError("Live Polymarket limit order placement is not implemented yet.")

    async def get_order_status(self, venue_order_id: str) -> dict[str, Any]:
        if self.mode == "paper":
            record = self._paper_orders.get(venue_order_id)
            if record is None:
                return {"venue_order_id": venue_order_id, "status": "unknown"}
            return {"venue_order_id": venue_order_id, "status": record.status, "updated_at": record.updated_at.isoformat()}
        raise NotImplementedError("Live Polymarket order status polling is not implemented yet.")

    async def cancel_order(self, venue_order_id: str) -> bool:
        if self.mode == "paper":
            record = self._paper_orders.get(venue_order_id)
            if record is None:
                return False
            record.status = "cancelled"
            record.updated_at = _utc_now()
            return True
        raise NotImplementedError("Live Polymarket order cancellation is not implemented yet.")
