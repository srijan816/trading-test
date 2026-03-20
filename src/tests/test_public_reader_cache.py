from __future__ import annotations

import asyncio

from arena.exchanges.polymarket_limit import PolymarketPublicReader


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeHTTPClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[tuple[str, str], ...] | None]] = []

    async def get(self, path: str, params: dict | None = None) -> _FakeResponse:
        frozen = tuple(sorted((params or {}).items()))
        self.calls.append((path, frozen))
        if path == "/tick-size":
            return _FakeResponse({"minimum_tick_size": "0.01"})
        return _FakeResponse({"bids": [{"price": "0.41", "size": "25"}], "asks": [{"price": "0.43", "size": "18"}]})

    async def aclose(self) -> None:
        return None


def test_public_reader_caches_orderbooks_and_tick_sizes():
    async def _run() -> None:
        reader = PolymarketPublicReader(cache_ttl_seconds=5.0)
        fake = _FakeHTTPClient()
        reader._http = fake

        await reader.get_raw_orderbook("token-1")
        await reader.get_raw_orderbook("token-1")
        assert len(fake.calls) == 1

        reader._orderbook_cache["token-1"] = (0.0, reader._orderbook_cache["token-1"][1])
        await reader.get_raw_orderbook("token-1")
        assert len(fake.calls) == 2

        await reader.get_tick_size("token-1")
        await reader.get_tick_size("token-1")
        assert len(fake.calls) == 3

    asyncio.run(_run())
