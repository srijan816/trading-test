from __future__ import annotations

from asyncio import Semaphore, gather
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Any
import json
import re

import httpx

from arena.config import ROOT
from arena.models import Market


WEATHER_KEYWORDS = ("temperature", "weather", "rain", "precipitation")
WEATHER_SEED_PAGES = [
    "https://polymarket.com/predictions/weather",
    "https://polymarket.com/predictions/hong-kong",
]
SLUG_CACHE_MINUTES = 30
EVENT_REFRESH_MINUTES = 15
MAX_EVENT_SLUGS = 24


@dataclass(slots=True)
class WeatherDiscoveryResult:
    markets: list[dict[str, Any]]
    warnings: list[str]


class PolymarketWeatherDiscovery:
    def __init__(self, timeout: float = 30.0, cache_path: Path | None = None) -> None:
        self.timeout = timeout
        self.cache_path = cache_path or (ROOT / "data" / "polymarket_weather_cache.json")

    async def discover_raw_markets(self) -> WeatherDiscoveryResult:
        warnings: list[str] = []
        cache = self._load_cache()
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            slugs, page_warnings = await self._discover_weather_slugs(client, cache)
            warnings.extend(page_warnings)
            raw_markets, event_warnings = await self._fetch_event_markets(client, slugs, cache)
            warnings.extend(event_warnings)
            if not raw_markets:
                fallback_markets, fallback_warnings = await self._fallback_gamma_search(client)
                raw_markets.extend(fallback_markets)
                warnings.extend(fallback_warnings)
        self._save_cache(cache)
        deduped: dict[str, dict[str, Any]] = {}
        for market in raw_markets:
            deduped[str(market.get("id"))] = market
        return WeatherDiscoveryResult(markets=list(deduped.values()), warnings=warnings)

    async def _discover_weather_slugs(self, client: httpx.AsyncClient, cache: dict[str, Any]) -> tuple[set[str], list[str]]:
        warnings: list[str] = []
        slug_cache = cache.setdefault("slugs", {})
        cached_at = self._parse_dt(slug_cache.get("fetched_at"))
        if cached_at and cached_at > self._utc_now() - timedelta(minutes=SLUG_CACHE_MINUTES):
            return set(slug_cache.get("values", [])), warnings

        slugs: set[str] = set()
        pages_to_fetch = list(WEATHER_SEED_PAGES)
        seen_pages: set[str] = set()
        while pages_to_fetch:
            url = pages_to_fetch.pop(0)
            if url in seen_pages:
                continue
            seen_pages.add(url)
            try:
                response = await client.get(url)
                response.raise_for_status()
                payload = self._extract_page_props(response.text)
                page_props = payload.get("props", {}).get("pageProps", {})
                slugs.update(self._extract_weather_slugs_from_page(page_props))
                if url.endswith("/predictions/weather"):
                    for sub in page_props.get("footerData", {}).get("subcategories", []):
                        label = str(sub.get("label", "")).lower()
                        slug = sub.get("slug")
                        if slug and label in {"hong kong", "nyc", "new york", "chicago", "london", "weather", "taipei", "seattle", "toronto"}:
                            pages_to_fetch.append(f"https://polymarket.com/predictions/{slug}")
            except Exception as exc:
                warnings.append(f"weather_page_parse_failed:{url}:{exc}")

        slug_cache["values"] = sorted(slugs)
        slug_cache["fetched_at"] = self._utc_now().isoformat()
        return slugs, warnings

    def _extract_weather_slugs_from_page(self, page_props: dict[str, Any]) -> set[str]:
        slugs: set[str] = set()
        candidates = []
        candidates.extend(page_props.get("schemaData", {}).get("events", []))
        footer = page_props.get("footerData", {})
        for section in ("popularMarkets", "newestMarkets"):
            candidates.extend(footer.get(section, []))
        for item in candidates:
            slug = str(item.get("slug", ""))
            title = str(item.get("title", ""))
            text = f"{slug} {title}".lower()
            if slug and any(keyword in text for keyword in WEATHER_KEYWORDS):
                slugs.add(slug)
        return slugs

    async def _fetch_event_markets(self, client: httpx.AsyncClient, slugs: set[str], cache: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
        warnings: list[str] = []
        event_cache = cache.setdefault("events", {})
        semaphore = Semaphore(5)

        async def load_slug(slug: str) -> list[dict[str, Any]]:
            async with semaphore:
                cached = event_cache.get(slug, {})
                cached_at = self._parse_dt(cached.get("fetched_at"))
                if cached_at and cached_at > self._utc_now() - timedelta(minutes=EVENT_REFRESH_MINUTES):
                    return cached.get("markets", [])
                try:
                    response = await client.get(f"https://polymarket.com/event/{slug}")
                    response.raise_for_status()
                    payload = self._extract_page_props(response.text)
                    markets = self._extract_event_markets(payload, slug)
                    enriched = await self._attach_orderbooks(client, markets)
                    event_cache[slug] = {"fetched_at": self._utc_now().isoformat(), "markets": enriched}
                    return enriched
                except Exception as exc:
                    warnings.append(f"weather_event_parse_failed:{slug}:{exc}")
                    return cached.get("markets", [])

        results = await gather(*(load_slug(slug) for slug in sorted(slugs)[:MAX_EVENT_SLUGS]))
        all_markets = [market for group in results for market in group]
        return all_markets, warnings

    async def _attach_orderbooks(self, client: httpx.AsyncClient, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        enriched: list[dict[str, Any]] = []
        semaphore = Semaphore(12)
        token_cache: dict[str, dict[str, Any] | Exception] = {}
        stale_token_ids: set[str] = set()

        async def load_token(token_id: str) -> dict[str, Any]:
            if token_id in stale_token_ids:
                raise httpx.HTTPStatusError(
                    "stale token skipped",
                    request=httpx.Request("GET", "https://clob.polymarket.com/book", params={"token_id": token_id}),
                    response=httpx.Response(404),
                )
            if token_id in token_cache:
                cached = token_cache[token_id]
                if isinstance(cached, Exception):
                    raise cached
                return cached
            async with semaphore:
                response = await client.get("https://clob.polymarket.com/book", params={"token_id": token_id})
                if response.status_code == 404:
                    stale_token_ids.add(token_id)
                    error = httpx.HTTPStatusError(
                        f"stale token_id {token_id}",
                        request=response.request,
                        response=response,
                    )
                    token_cache[token_id] = error
                    raise error
                response.raise_for_status()
                payload = response.json()
                token_cache[token_id] = payload
                return payload

        for market in markets:
            token_ids = market.get("clobTokenIds") or []
            labels = market.get("outcomes") or []
            prices = market.get("outcomePrices") or []
            tokens = []
            payloads = await gather(*(load_token(token_id) for token_id in token_ids), return_exceptions=True)
            for idx, token_id in enumerate(token_ids):
                token = {
                    "token_id": token_id,
                    "outcome": labels[idx] if idx < len(labels) else f"Outcome {idx + 1}",
                    "price": prices[idx] if idx < len(prices) else 0.0,
                    "best_bid": market.get("bestBid") if idx == 0 else None,
                    "best_ask": market.get("bestAsk") if idx == 0 else None,
                    "bid_depth": [],
                    "ask_depth": [],
                    "last_trade_price": prices[idx] if idx < len(prices) else 0.0,
                }
                payload = payloads[idx]
                if not isinstance(payload, Exception):
                    bids = sorted(
                        [(float(level["price"]), float(level["size"])) for level in payload.get("bids", [])],
                        key=lambda level: level[0],
                        reverse=True,
                    )
                    asks = sorted(
                        [(float(level["price"]), float(level["size"])) for level in payload.get("asks", [])],
                        key=lambda level: level[0],
                    )
                    token["bid_depth"] = bids
                    token["ask_depth"] = asks
                    if bids:
                        token["best_bid"] = bids[0][0]
                    if asks:
                        token["best_ask"] = asks[0][0]
                else:
                    response = getattr(payload, "response", None)
                    if getattr(response, "status_code", None) == 404:
                        token["best_bid"] = 0.0
                        token["best_ask"] = 1.0
                        token["stale"] = True
                tokens.append(token)
            enriched_market = dict(market)
            enriched_market["tokens"] = tokens
            enriched_market["tags"] = market.get("tags") or [{"slug": "weather", "label": "Weather"}]
            enriched_market["question"] = market.get("title") or market.get("question")
            enriched.append(enriched_market)
        return enriched

    def _extract_event_markets(self, payload: dict[str, Any], event_slug: str) -> list[dict[str, Any]]:
        stack: list[Any] = [payload.get("props", {}).get("pageProps", {})]
        matches: list[dict[str, Any]] = []
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                slug = str(item.get("slug", ""))
                tags = item.get("tags") or []
                tag_slugs = {str(tag.get("slug", "")).lower() for tag in tags if isinstance(tag, dict)}
                if "id" in item and "clobTokenIds" in item:
                    if slug.startswith(event_slug) or "weather" in tag_slugs or "temperature" in tag_slugs:
                        matches.append(item)
                stack.extend(item.values())
            elif isinstance(item, list):
                stack.extend(item)
        return matches

    async def _fallback_gamma_search(self, client: httpx.AsyncClient) -> tuple[list[dict[str, Any]], list[str]]:
        warnings: list[str] = []
        markets: list[dict[str, Any]] = []
        for query in ("temperature hong kong", "highest temperature", "precipitation"):
            try:
                response = await client.get("https://gamma-api.polymarket.com/markets", params={"query": query, "limit": 50})
                response.raise_for_status()
                for item in response.json():
                    text = f"{item.get('question', '')} {json.dumps(item.get('tags', []))}".lower()
                    if any(keyword in text for keyword in WEATHER_KEYWORDS):
                        item["question"] = item.get("question") or item.get("title")
                        markets.append(item)
            except Exception as exc:
                warnings.append(f"weather_gamma_fallback_failed:{query}:{exc}")
        return markets, warnings

    def _extract_page_props(self, html: str) -> dict[str, Any]:
        for script in re.findall(r"<script[^>]*>(.*?)</script>", html, flags=re.DOTALL):
            candidate = script.strip()
            if candidate.startswith('{"props":'):
                return json.loads(unescape(candidate))
        raise ValueError("Could not locate page props JSON")

    def _load_cache(self) -> dict[str, Any]:
        if self.cache_path.exists():
            try:
                return json.loads(self.cache_path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save_cache(self, cache: dict[str, Any]) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")

    def _parse_dt(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None

    def _utc_now(self) -> datetime:
        return datetime.now(timezone.utc)
