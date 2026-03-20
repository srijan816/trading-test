from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import json
import httpx

from arena.adapters.base import MarketDataAdapter
from arena.categorization import categorize_market
from arena.models import Market, OrderBookSnapshot, Outcome


class PolymarketAdapter(MarketDataAdapter):
    venue = "polymarket"

    def __init__(self, gamma_base_url: str, clob_base_url: str, timeout: float = 20.0) -> None:
        self.gamma_base_url = gamma_base_url.rstrip("/")
        self.clob_base_url = clob_base_url.rstrip("/")
        self.timeout = timeout

    async def list_active_markets(self, categories: list[str] | None = None) -> list[Market]:
        params: dict[str, Any] = {"active": "true", "closed": "false", "limit": 100}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.gamma_base_url}/markets", params=params)
            response.raise_for_status()
            payload = response.json()
            markets = [self._normalize_market(item) for item in payload]
        if categories:
            allowed = set(categories)
            markets = [market for market in markets if market.category in allowed]
        return markets

    async def get_orderbook(self, market_id: str, outcome_id: str) -> OrderBookSnapshot:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.clob_base_url}/book", params={"token_id": outcome_id})
            response.raise_for_status()
            payload = response.json()
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
        mid = (best_bid + best_ask) / 2 if bids and asks else best_bid or best_ask
        spread = max(best_ask - best_bid, 0.0)
        return OrderBookSnapshot(
            market_id=market_id,
            outcome_id=outcome_id,
            venue=self.venue,
            timestamp=datetime.now(timezone.utc),
            bids=bids,
            asks=asks,
            mid=mid,
            spread=spread,
        )

    async def get_resolution_status(self, market_id: str) -> Market:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.gamma_base_url}/markets/{market_id}")
            response.raise_for_status()
        return self._normalize_market(response.json())

    async def search_markets(self, query: str) -> list[Market]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.gamma_base_url}/markets", params={"query": query})
            response.raise_for_status()
        return [self._normalize_market(item) for item in response.json()]

    def _normalize_market(self, raw: dict[str, Any]) -> Market:
        question = raw.get("question") or raw.get("title") or raw.get("description", "")
        slug = raw.get("slug") or raw.get("ticker", raw.get("id", ""))
        category = self._infer_category(question, raw)
        outcomes = self._normalize_outcomes(raw)
        end_time = raw.get("endDate") or raw.get("end_time") or raw.get("closeTime")
        if end_time and end_time.endswith("Z"):
            end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
        else:
            end_dt = datetime.now(timezone.utc)
        return Market(
            market_id=str(raw.get("id", slug)),
            venue=self.venue,
            slug=slug,
            question=question,
            category=category,
            market_type="binary" if len(outcomes) <= 2 else "multi_bucket",
            outcomes=outcomes,
            resolution_source=raw.get("resolutionSource") or "Polymarket",
            end_time=end_dt,
            volume_usd=float(raw.get("volume", raw.get("volumeNum", 0.0)) or 0.0),
            liquidity_usd=float(raw.get("liquidity", raw.get("liquidityNum", 0.0)) or 0.0),
            status="resolved" if raw.get("closed") else "active",
            resolved_outcome_id=str(raw.get("winner")) if raw.get("winner") else None,
        )

    def _normalize_outcomes(self, raw: dict[str, Any]) -> list[Outcome]:
        raw_outcomes = raw.get("outcomes")
        raw_prices = raw.get("outcomePrices")
        raw_token_ids = raw.get("clobTokenIds")
        if isinstance(raw_outcomes, str):
            raw_outcomes = self._load_json_list(raw_outcomes)
        if isinstance(raw_prices, str):
            raw_prices = self._load_json_list(raw_prices)
        if isinstance(raw_token_ids, str):
            raw_token_ids = self._load_json_list(raw_token_ids)
        if not raw_outcomes:
            tokens = raw.get("tokens", [])
            raw_outcomes = [
                {
                    "id": token.get("token_id") or token.get("tokenId"),
                    "label": token.get("outcome") or token.get("title"),
                    "price": token.get("price", 0.0),
                    "best_bid": token.get("best_bid"),
                    "best_ask": token.get("best_ask"),
                    "bid_depth": token.get("bid_depth", []),
                    "ask_depth": token.get("ask_depth", []),
                    "last_trade_price": token.get("last_trade_price", token.get("price", 0.0)),
                    "volume": token.get("volume", 0.0),
                }
                for token in tokens
            ]
        outcomes: list[Outcome] = []
        if raw_outcomes and isinstance(raw_outcomes[0], str):
            zipped = zip(raw_outcomes, raw_prices or [], raw_token_ids or [])
            raw_outcomes = [
                {"label": label, "price": price, "id": token_id}
                for label, price, token_id in zipped
            ]
        for item in raw_outcomes or []:
            price = float(item.get("price", item.get("mid", 0.5)) or 0.5)
            best_bid = float(item.get("best_bid", max(price - 0.01, 0.0)) or 0.0)
            best_ask = float(item.get("best_ask", min(price + 0.01, 1.0)) or 1.0)
            bid_depth = [tuple(level) for level in item.get("bid_depth", [])] or [(best_bid, 100.0)]
            ask_depth = [tuple(level) for level in item.get("ask_depth", [])] or [(best_ask, 100.0)]
            outcomes.append(
                Outcome(
                    outcome_id=str(item.get("id") or item.get("token_id") or item.get("tokenId") or item.get("label")),
                    label=str(item.get("label") or item.get("title") or item.get("name")),
                    best_bid=best_bid,
                    best_ask=best_ask,
                    mid_price=price,
                    bid_depth=bid_depth,
                    ask_depth=ask_depth,
                    last_trade_price=float(item.get("last_trade_price", price) or price),
                    volume_usd=float(item.get("volume", 0.0) or 0.0),
                )
            )
        if not outcomes:
            outcomes = [
                Outcome("yes", "Yes", 0.49, 0.51, 0.50, [(0.49, 100.0)], [(0.51, 100.0)], 0.50, 0.0),
                Outcome("no", "No", 0.49, 0.51, 0.50, [(0.49, 100.0)], [(0.51, 100.0)], 0.50, 0.0),
            ]
        return outcomes

    def _load_json_list(self, value: str) -> list[Any]:
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []

    def _infer_category(self, question: str, raw: dict[str, Any]) -> str:
        tags = raw.get("tags") or []
        tag_tokens = [str(tag.get("slug", "")).lower() for tag in tags if isinstance(tag, dict)]
        return categorize_market(
            question,
            extra=f"{raw.get('category', '')} {raw.get('tag', '')}",
            tags=tag_tokens,
            current_category=str(raw.get("category", "")).lower() or None,
        )
