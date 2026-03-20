from __future__ import annotations

from os import getenv

import httpx

from arena.adapters.base import SearchClient
from arena.models import SearchResult


class SerperSearchClient(SearchClient):
    def __init__(self, base_url: str, api_key_env: str = "SERPER_API_KEY", timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key_env = api_key_env
        self.timeout = timeout

    async def search(self, query: str, num_results: int = 5) -> list[SearchResult]:
        api_key = getenv(self.api_key_env)
        if not api_key:
            raise RuntimeError(f"Missing {self.api_key_env}")
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/search",
                headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
                json={"q": query, "num": num_results},
            )
            response.raise_for_status()
            payload = response.json()
        return [
            SearchResult(title=item.get("title", ""), url=item.get("link", ""), snippet=item.get("snippet", ""))
            for item in payload.get("organic", [])
        ]
