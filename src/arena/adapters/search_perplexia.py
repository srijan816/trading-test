from __future__ import annotations

import asyncio
import logging
from os import getenv

import httpx

from arena.adapters.base import SearchClient
from arena.models import SearchResult

logger = logging.getLogger(__name__)

VALID_NEXUS_MODES = {"quick", "standard", "deep"}
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class PerplexiaSearchClient(SearchClient):
    """Use a local Nexus/Perplexia research API as a deep-research search source."""

    def __init__(
        self,
        base_url: str | None = None,
        api_key_env: str = "NEXUS_API_KEY",
        timeout: float = 30.0,
        mode: str = "standard",
        output_length: str = "short",
        model: str | None = None,
        max_sources: int = 5,
        session_id: str | None = None,
    ) -> None:
        nexus_root = getenv("NEXUS_URL", f"http://localhost:{getenv('NEXUS_PORT', '3001')}")
        resolved_base_url = getenv("NEXUS_URL") or getenv("PERPLEXIA_BASE_URL") or base_url or nexus_root
        if resolved_base_url.rstrip("/").endswith("/api/v1"):
            self.base_url = resolved_base_url.rstrip("/")
        else:
            self.base_url = f"{resolved_base_url.rstrip('/')}/api/v1"
        self.api_key_env = api_key_env
        self.timeout = timeout
        self.mode = self._normalize_mode(mode)
        self.output_length = output_length
        self.model = model
        self.max_sources = max_sources
        self.session_id = session_id

    def _http_timeout(self) -> httpx.Timeout:
        read_timeout = max(float(self.timeout), 240.0 if self.mode in {"standard", "deep"} else 30.0)
        return httpx.Timeout(connect=30.0, read=read_timeout, write=30.0, pool=30.0)

    def _normalize_mode(self, mode: str) -> str:
        normalized = str(mode or "").strip().lower()
        if normalized not in VALID_NEXUS_MODES:
            logger.warning("Invalid Nexus research mode '%s' — falling back to 'standard'", mode)
            return "standard"
        return normalized

    def _auth_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        api_key = getenv(self.api_key_env, "").strip()
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    async def search(self, query: str, num_results: int = 5) -> list[SearchResult]:
        payload = {
            "query": query,
            "mode": self.mode,
            "output_length": self.output_length,
            "stream": False,
            "max_sources": max(self.max_sources, num_results),
        }
        if self.model:
            payload["model"] = self.model
        if self.session_id:
            payload["session_id"] = self.session_id

        async with httpx.AsyncClient(timeout=self._http_timeout()) as client:
            response = None
            for attempt in range(3):
                response = await client.post(f"{self.base_url}/research", headers=self._auth_headers(), json=payload)
                if response.status_code == 200:
                    break
                body_preview = response.text[:500]
                if response.status_code in RETRYABLE_STATUS_CODES and attempt < 2:
                    logger.warning(
                        "Transient Nexus research failure on attempt %s/3: %s — %s",
                        attempt + 1,
                        response.status_code,
                        body_preview,
                    )
                    await asyncio.sleep(5 * (attempt + 1))
                    continue
                logger.error("Nexus research failed: %s — %s", response.status_code, body_preview)
                response.raise_for_status()
            assert response is not None
            research = response.json()

        report = str(research.get("report", "") or "").strip()
        sources = research.get("sources", []) or []
        follow_ups = [str(item) for item in research.get("follow_ups", []) if item]
        session_id = research.get("session_id")
        response_meta = research.get("metadata", {}) if isinstance(research.get("metadata"), dict) else {}
        model_used = (
            response_meta.get("model")
            or response_meta.get("synthesis_model")
            or (response_meta.get("models_used", {}) or {}).get("synthesis")
            or self.model
        )
        results: list[SearchResult] = []

        if report:
            results.append(
                SearchResult(
                    title=f"Perplexia research brief: {query[:80]}",
                    url="perplexia://report",
                    snippet=report[:500],
                    metadata={
                        "provider": "perplexia",
                        "report": report,
                        "follow_ups": follow_ups,
                        "session_id": session_id,
                        "mode": self.mode,
                        "model_used": model_used,
                        "duration_seconds": response_meta.get("duration_seconds"),
                        "endpoint": "/api/v1/research",
                    },
                )
            )

        for source in sources[:num_results]:
            snippet = str(source.get("snippet", "") or source.get("content", "") or report[:200])
            results.append(
                SearchResult(
                    title=str(source.get("title", "") or "Untitled source"),
                    url=str(source.get("url", "") or "perplexia://source"),
                    snippet=snippet[:300],
                    metadata={
                        "provider": "perplexia",
                        "session_id": session_id,
                        "model_used": model_used,
                        "endpoint": "/api/v1/research",
                    },
                )
            )

        if not results and research.get("follow_ups"):
            results.append(
                SearchResult(
                    title="Perplexia follow-ups",
                    url="perplexia://follow-ups",
                    snippet=" | ".join(str(item) for item in research.get("follow_ups", [])[:3]),
                    metadata={
                        "provider": "perplexia",
                        "follow_ups": follow_ups,
                        "session_id": session_id,
                        "model_used": model_used,
                        "endpoint": "/api/v1/research",
                    },
                )
            )

        return results[: max(1, num_results)]

    async def market_research(self, payload: dict) -> dict:
        async with httpx.AsyncClient(timeout=self._http_timeout()) as client:
            response = None
            for attempt in range(3):
                response = await client.post(
                    f"{self.base_url}/market-research",
                    headers=self._auth_headers(),
                    json=payload,
                )
                if response.status_code == 200:
                    break
                body_preview = response.text[:500]
                if response.status_code in RETRYABLE_STATUS_CODES and attempt < 2:
                    logger.warning(
                        "Transient Nexus market-research failure on attempt %s/3: %s — %s",
                        attempt + 1,
                        response.status_code,
                        body_preview,
                    )
                    await asyncio.sleep(5 * (attempt + 1))
                    continue
                logger.error("Nexus market-research failed: %s — %s", response.status_code, body_preview)
                response.raise_for_status()
            assert response is not None
            return response.json()
