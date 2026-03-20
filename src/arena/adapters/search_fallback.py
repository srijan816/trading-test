from __future__ import annotations

from arena.adapters.base import SearchClient
from arena.models import SearchResult


class FallbackSearchClient(SearchClient):
    def __init__(self, primary: SearchClient, secondary: SearchClient | None = None) -> None:
        self.primary = primary
        self.secondary = secondary

    async def search(self, query: str, num_results: int = 5) -> list[SearchResult]:
        try:
            results = await self.primary.search(query, num_results=num_results)
            if results:
                return results
        except Exception:
            if not self.secondary:
                raise
        if self.secondary:
            return await self.secondary.search(query, num_results=num_results)
        return []
