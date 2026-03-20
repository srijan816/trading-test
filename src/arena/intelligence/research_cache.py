from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import logging
import os
import time

logger = logging.getLogger("arena.research_cache")

CACHE_DIR = Path("data/cache/research")


class ResearchCache:
    """
    Caches Nexus research results per market, avoiding redundant full-pipeline calls.

    Cache invalidation rules:
    - Weather markets: cache valid for 2 hours (forecasts update ~4x/day)
    - Event markets: cache valid for 6 hours (news changes slower)
    - Any market: force refresh if market price moved > 5 cents since last research
    - Any market: force refresh if ensemble forecast changed significantly
    """

    TTL = {
        "weather": int(float(os.getenv("RESEARCH_CACHE_WEATHER_TTL_HOURS", "2")) * 3600),
        "event": int(float(os.getenv("RESEARCH_CACHE_EVENT_TTL_HOURS", "6")) * 3600),
        "crypto": 1 * 3600,
        "default": 4 * 3600,
    }

    PRICE_CHANGE_THRESHOLD = 0.05
    ENSEMBLE_CHANGE_THRESHOLD = 0.5

    def __init__(self) -> None:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _cache_key(self, market_id: str, strategy: str = "shared") -> Path:
        return CACHE_DIR / f"{market_id}_{strategy}.json"

    def _load_entry(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None

    def has_any(self, market_id: str, strategy: str = "shared") -> bool:
        return self._cache_key(market_id, strategy).exists()

    def get(
        self,
        market_id: str,
        market_type: str = "default",
        current_price: float | None = None,
        current_ensemble_mu: float | None = None,
        strategy: str = "shared",
        *,
        allow_stale: bool = False,
    ) -> dict[str, Any] | None:
        path = self._cache_key(market_id, strategy)
        cached = self._load_entry(path)
        if cached is None:
            return None

        cached_at = float(cached.get("cached_at", 0) or 0)
        ttl = self.TTL.get(market_type, self.TTL["default"])
        age_seconds = time.time() - cached_at
        if not allow_stale and age_seconds > ttl:
            logger.info(
                "Research cache expired for market %s (age: %.1fh, ttl: %.1fh)",
                market_id,
                age_seconds / 3600,
                ttl / 3600,
            )
            return None

        if current_price is not None and "market_price" in cached and cached.get("market_price") is not None:
            price_delta = abs(float(current_price) - float(cached["market_price"]))
            if not allow_stale and price_delta > self.PRICE_CHANGE_THRESHOLD:
                logger.info(
                    "Research cache invalidated for %s: price moved %.3f > %.3f",
                    market_id,
                    price_delta,
                    self.PRICE_CHANGE_THRESHOLD,
                )
                return None

        if current_ensemble_mu is not None and "ensemble_mu" in cached and cached.get("ensemble_mu") is not None:
            mu_delta = abs(float(current_ensemble_mu) - float(cached["ensemble_mu"]))
            if not allow_stale and mu_delta > self.ENSEMBLE_CHANGE_THRESHOLD:
                logger.info(
                    "Research cache invalidated for %s: ensemble shifted %.2f > %.2f",
                    market_id,
                    mu_delta,
                    self.ENSEMBLE_CHANGE_THRESHOLD,
                )
                return None

        logger.debug(
            "Research cache %s for market %s (age: %.0fm)",
            "STALE-HIT" if allow_stale and age_seconds > ttl else "HIT",
            market_id,
            age_seconds / 60,
        )
        return cached.get("result")

    def put(
        self,
        market_id: str,
        result: dict[str, Any],
        market_price: float | None = None,
        ensemble_mu: float | None = None,
        strategy: str = "shared",
    ) -> None:
        path = self._cache_key(market_id, strategy)
        entry = {
            "cached_at": time.time(),
            "market_id": market_id,
            "market_price": market_price,
            "ensemble_mu": ensemble_mu,
            "result": result,
        }
        path.write_text(json.dumps(entry), encoding="utf-8")
        logger.info("Research cache stored for market %s", market_id)

    def stats(self) -> dict[str, int]:
        files = list(CACHE_DIR.glob("*.json"))
        valid = 0
        expired = 0
        now = time.time()
        max_ttl = max(self.TTL.values())
        for file_path in files:
            data = self._load_entry(file_path)
            if data is None:
                expired += 1
                continue
            age = now - float(data.get("cached_at", 0) or 0)
            if age < max_ttl:
                valid += 1
            else:
                expired += 1
        return {"total": len(files), "valid": valid, "expired": expired}
