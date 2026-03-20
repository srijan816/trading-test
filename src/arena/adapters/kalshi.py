from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from arena.adapters.base import MarketDataAdapter
from arena.categorization import categorize_market
from arena.models import Market, OrderBookSnapshot, Outcome


class KalshiAdapter(MarketDataAdapter):
    venue = "kalshi"

    def __init__(self, base_url: str, timeout: float = 20.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    async def list_active_markets(self, categories: list[str] | None = None) -> list[Market]:
        raw_markets: list[dict[str, Any]] = []
        cursor: str | None = None
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for _ in range(10):
                params = {"status": "open", "limit": 100}
                if cursor:
                    params["cursor"] = cursor
                response = await client.get(f"{self.base_url}/markets", params=params)
                response.raise_for_status()
                payload = response.json()
                page = payload.get("markets", payload)
                raw_markets.extend(page)
                cursor = payload.get("cursor")
                if not cursor or not page:
                    break
        markets = [self._normalize_market(item) for item in raw_markets]
        if categories:
            markets = [market for market in markets if market.category in set(categories)]
        return markets

    async def get_orderbook(self, market_id: str, outcome_id: str) -> OrderBookSnapshot:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/markets/{market_id}/orderbook")
            response.raise_for_status()
            payload = response.json()
        yes_book = payload.get("orderbook", payload)
        bids = sorted(
            [(float(level["price"]) / 100.0, float(level["quantity"])) for level in yes_book.get("yes", {}).get("bids", [])],
            key=lambda level: level[0],
            reverse=True,
        )
        asks = sorted(
            [(float(level["price"]) / 100.0, float(level["quantity"])) for level in yes_book.get("yes", {}).get("asks", [])],
            key=lambda level: level[0],
        )
        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 1.0
        return OrderBookSnapshot(
            market_id=market_id,
            outcome_id=outcome_id,
            venue=self.venue,
            timestamp=datetime.now(timezone.utc),
            bids=bids,
            asks=asks,
            mid=(best_bid + best_ask) / 2 if bids and asks else best_bid or best_ask,
            spread=max(best_ask - best_bid, 0.0),
        )

    async def get_resolution_status(self, market_id: str) -> Market:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/markets/{market_id}")
            response.raise_for_status()
        payload = response.json()
        return self._normalize_market(payload.get("market", payload))

    async def search_markets(self, query: str) -> list[Market]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/markets", params={"search": query})
            response.raise_for_status()
            payload = response.json()
        raw_markets = payload.get("markets", payload)
        return [self._normalize_market(item) for item in raw_markets]

    def _normalize_market(self, raw: dict[str, Any]) -> Market:
        question = raw.get("title") or raw.get("subtitle") or raw.get("ticker", "")
        yes_bid = float(raw.get("yes_bid", 49)) / 100.0
        yes_ask = float(raw.get("yes_ask", 51)) / 100.0
        no_bid = 1.0 - yes_ask
        no_ask = 1.0 - yes_bid
        outcomes = [
            Outcome("yes", "Yes", yes_bid, yes_ask, (yes_bid + yes_ask) / 2, [(yes_bid, 100.0)], [(yes_ask, 100.0)], (yes_bid + yes_ask) / 2, float(raw.get("volume", 0.0) or 0.0)),
            Outcome("no", "No", no_bid, no_ask, (no_bid + no_ask) / 2, [(no_bid, 100.0)], [(no_ask, 100.0)], (no_bid + no_ask) / 2, float(raw.get("volume", 0.0) or 0.0)),
        ]
        end_time = raw.get("close_time") or raw.get("expiration_time")
        end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00")) if end_time else datetime.now(timezone.utc)
        status = "resolved" if raw.get("status") in {"settled", "resolved"} else "active"
        winner = raw.get("result")
        resolved_outcome_id = None
        if winner in {"yes", "no"}:
            resolved_outcome_id = winner
        return Market(
            market_id=raw.get("ticker", raw.get("id", question)),
            venue=self.venue,
            slug=raw.get("ticker", question.lower().replace(" ", "-")),
            question=question,
            category=self._infer_category(question),
            market_type="binary",
            outcomes=outcomes,
            resolution_source="Kalshi",
            end_time=end_dt,
            volume_usd=float(raw.get("volume", 0.0) or 0.0),
            liquidity_usd=float(raw.get("open_interest", raw.get("volume", 0.0)) or 0.0),
            status=status,
            resolved_outcome_id=resolved_outcome_id,
        )

    def _infer_category(self, text: str) -> str:
        return categorize_market(text)
