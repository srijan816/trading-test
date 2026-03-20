from __future__ import annotations

from datetime import datetime, timezone
import logging
import time

import httpx

from arena.models import OrderBookSnapshot

logger = logging.getLogger(__name__)

CLOB_BASE = "https://clob.polymarket.com"


class PolymarketPublicReader:
    """Read-only Polymarket market data client for paper-maker simulation."""

    def __init__(self, base_url: str = CLOB_BASE, timeout: float = 10.0, cache_ttl_seconds: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)
        self.cache_ttl_seconds = float(cache_ttl_seconds)
        self._orderbook_cache: dict[str, tuple[float, dict]] = {}
        self._midpoint_cache: dict[str, tuple[float, float | None]] = {}
        self._price_cache: dict[tuple[str, str], tuple[float, float | None]] = {}
        self._tick_size_cache: dict[str, tuple[float, str]] = {}

    def _get_cached_value(self, cache: dict, key, ttl_seconds: float):
        entry = cache.get(key)
        if entry is None:
            return None
        cached_at, value = entry
        if (time.monotonic() - cached_at) > ttl_seconds:
            cache.pop(key, None)
            return None
        return value

    def _set_cached_value(self, cache: dict, key, value) -> None:
        cache[key] = (time.monotonic(), value)

    async def get_raw_orderbook(self, token_id: str) -> dict:
        cached = self._get_cached_value(self._orderbook_cache, token_id, self.cache_ttl_seconds)
        if cached is not None:
            logger.debug("Using cached Polymarket orderbook for token %s", token_id)
            return cached
        response = await self._http.get("/book", params={"token_id": token_id})
        response.raise_for_status()
        payload = response.json()
        self._set_cached_value(self._orderbook_cache, token_id, payload)
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
        cached = self._get_cached_value(self._midpoint_cache, token_id, self.cache_ttl_seconds)
        if cached is not None:
            return cached
        response = await self._http.get("/midpoint", params={"token_id": token_id})
        response.raise_for_status()
        data = response.json()
        mid = data.get("mid")
        parsed = float(mid) if mid is not None else None
        self._set_cached_value(self._midpoint_cache, token_id, parsed)
        return parsed

    async def get_price(self, token_id: str, side: str = "BUY") -> float | None:
        cache_key = (token_id, side)
        cached = self._get_cached_value(self._price_cache, cache_key, self.cache_ttl_seconds)
        if cached is not None:
            return cached
        response = await self._http.get("/price", params={"token_id": token_id, "side": side})
        response.raise_for_status()
        data = response.json()
        price = data.get("price")
        parsed = float(price) if price is not None else None
        self._set_cached_value(self._price_cache, cache_key, parsed)
        return parsed

    async def get_tick_size(self, token_id: str) -> str:
        cached = self._get_cached_value(self._tick_size_cache, token_id, 300.0)
        if cached is not None:
            return cached
        response = await self._http.get("/tick-size", params={"token_id": token_id})
        response.raise_for_status()
        data = response.json()
        tick_size = str(data.get("minimum_tick_size") or data.get("tick_size") or "0.01")
        self._set_cached_value(self._tick_size_cache, token_id, tick_size)
        return tick_size

    async def close(self) -> None:
        await self._http.aclose()


# Backward-compatible alias for the older scaffold name.
PolymarketLimitClient = PolymarketPublicReader
