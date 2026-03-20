from __future__ import annotations

from datetime import datetime, timezone
import logging

import httpx

from arena.models import OrderBookSnapshot

logger = logging.getLogger(__name__)

CLOB_BASE = "https://clob.polymarket.com"


class PolymarketPublicReader:
    """Read-only Polymarket market data client for paper-maker simulation."""

    def __init__(self, base_url: str = CLOB_BASE, timeout: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def get_raw_orderbook(self, token_id: str) -> dict:
        response = await self._http.get("/book", params={"token_id": token_id})
        response.raise_for_status()
        payload = response.json()
        logger.debug("Fetched Polymarket orderbook for token %s", token_id)
        return payload

    async def get_orderbook(self, market_id: str, token_id: str | None = None) -> OrderBookSnapshot:
        actual_token_id = str(token_id or market_id)
        payload = await self.get_raw_orderbook(actual_token_id)
        bids = sorted(
            [(float(level["price"]), float(level["size"])) for level in payload.get("bids", [])],
            key=lambda level: level[0],
            reverse=True,
        )
        asks = sorted(
            [(float(level["price"]), float(level["size"])) for level in payload.get("asks", [])],
            key=lambda level: level[0],
        )
        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 1.0
        mid = (best_bid + best_ask) / 2 if bids and asks else (best_bid or best_ask)
        spread = max(best_ask - best_bid, 0.0)
        return OrderBookSnapshot(
            market_id=market_id,
            outcome_id=actual_token_id,
            venue="polymarket",
            timestamp=datetime.now(timezone.utc),
            bids=bids,
            asks=asks,
            mid=mid,
            spread=spread,
        )

    async def get_midpoint(self, token_id: str) -> float | None:
        response = await self._http.get("/midpoint", params={"token_id": token_id})
        response.raise_for_status()
        data = response.json()
        mid = data.get("mid")
        return float(mid) if mid is not None else None

    async def get_price(self, token_id: str, side: str = "BUY") -> float | None:
        response = await self._http.get("/price", params={"token_id": token_id, "side": side})
        response.raise_for_status()
        data = response.json()
        price = data.get("price")
        return float(price) if price is not None else None

    async def get_tick_size(self, token_id: str) -> str:
        response = await self._http.get("/tick-size", params={"token_id": token_id})
        response.raise_for_status()
        data = response.json()
        return str(data.get("minimum_tick_size") or data.get("tick_size") or "0.01")

    async def close(self) -> None:
        await self._http.aclose()


# Backward-compatible alias for the older scaffold name.
PolymarketLimitClient = PolymarketPublicReader
