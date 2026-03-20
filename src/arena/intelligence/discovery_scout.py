from __future__ import annotations

import json
import logging
import math
import re
from datetime import UTC, datetime

from arena.adapters.weather_openmeteo import CITY_COORDS
from arena.db import ArenaDB
from arena.exchanges.kalshi_adapter import KalshiAdapter as KalshiExchangeAdapter
from arena.intelligence.discovery import DiscoverySignal, SignalType
from arena.intelligence.discovery_logger import DiscoveryLogger
from arena.strategies.algo_forecast import parse_weather_question

logger = logging.getLogger(__name__)

WATCH_CATEGORIES = ("crypto", "politics", "economics")


def _parse_end_time(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _safe_float(value) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def score_adjacent_market(row) -> float:
    question = str(row["question"] or "")
    volume = _safe_float(row["volume_usd"])
    liquidity = _safe_float(row["liquidity_usd"])
    end_time = _parse_end_time(row["end_time"])
    hours_to_end = max(((end_time - datetime.now(UTC)).total_seconds() / 3600.0), 0.0) if end_time else 24.0

    liquidity_score = min(math.log10(max(volume, 1.0)) / 5.0, 1.0)
    depth_score = min(math.log10(max(liquidity, 1.0)) / 5.0, 1.0)
    timing_score = 1.0 if 6.0 <= hours_to_end <= 168.0 else 0.6 if hours_to_end <= 336.0 else 0.25
    structure_score = 0.2 if len(re.findall(r"\d+\.?\d*", question)) >= 2 else 0.0
    bracket_bonus = 0.15 if any(token in question.lower() for token in ("between", "above", "below", "range", "or higher", "or lower")) else 0.0
    return round((0.4 * liquidity_score) + (0.3 * depth_score) + (0.2 * timing_score) + structure_score + bracket_bonus, 4)


def extract_yes_no_asks(row) -> tuple[float | None, float | None]:
    outcomes_json = row["outcomes_json"]
    try:
        outcomes = json.loads(outcomes_json) if isinstance(outcomes_json, str) else (outcomes_json or [])
    except json.JSONDecodeError:
        return None, None

    yes_ask = None
    no_ask = None
    for outcome in outcomes:
        label = str(outcome.get("label", "")).strip().lower()
        best_ask = outcome.get("best_ask")
        if best_ask is None:
            continue
        if label == "yes":
            yes_ask = float(best_ask)
        elif label == "no":
            no_ask = float(best_ask)
    return yes_ask, no_ask


class DiscoveryUniverseScanner:
    def __init__(
        self,
        db: ArenaDB,
        *,
        discovery_logger: DiscoveryLogger | None = None,
        kalshi_adapter: KalshiExchangeAdapter | None = None,
        max_adjacent_candidates: int = 6,
        min_adjacent_score: float = 0.95,
    ) -> None:
        self.db = db
        self.discovery_logger = discovery_logger or DiscoveryLogger(str(db.path))
        self.kalshi_adapter = kalshi_adapter or KalshiExchangeAdapter()
        self.max_adjacent_candidates = int(max_adjacent_candidates)
        self.min_adjacent_score = float(min_adjacent_score)

    async def scan(self) -> dict[str, object]:
        with self.db.connect() as conn:
            active_rows = list(conn.execute("SELECT * FROM markets WHERE status = 'active' ORDER BY volume_usd DESC, end_time ASC"))

        adjacent_signals = self._build_adjacent_market_signals(active_rows)
        weather_city_signals = self._build_weather_city_signals(active_rows)
        cross_venue_signals = self._build_cross_venue_signals(active_rows)

        logged = 0
        for row, signal in [*adjacent_signals, *weather_city_signals, *cross_venue_signals]:
            if self._recent_duplicate_exists(str(row["market_id"]), signal.signal_type.value, signal.headline):
                continue
            self.discovery_logger.log_signal(
                signal,
                strategy_id="discovery_scout",
                market_question=str(row["question"]),
                category=str(row["category"]),
            )
            logged += 1

        summary = {
            "signals_logged": logged,
            "adjacent_candidates": len(adjacent_signals),
            "weather_city_candidates": len(weather_city_signals),
            "cross_venue_candidates": len(cross_venue_signals),
        }
        logger.info("Discovery universe scout summary: %s", summary)
        return summary

    def _build_adjacent_market_signals(self, rows: list) -> list[tuple[object, DiscoverySignal]]:
        candidates: list[tuple[float, object]] = []
        for row in rows:
            if str(row["venue"]) != "polymarket":
                continue
            if str(row["category"]) not in WATCH_CATEGORIES:
                continue
            score = score_adjacent_market(row)
            if score < self.min_adjacent_score:
                continue
            candidates.append((score, row))

        signals: list[tuple[object, DiscoverySignal]] = []
        for score, row in sorted(candidates, key=lambda item: item[0], reverse=True)[: self.max_adjacent_candidates]:
            signal = DiscoverySignal(
                signal_type=SignalType.MARKET_EXPANSION,
                headline=f"Watchlist candidate: {row['category']} market",
                detail=(
                    f"High-liquidity {row['category']} market scored {score:.2f} for discovery-only monitoring. "
                    f"Question: {row['question']}"
                ),
                source_url="",
                source_name="Arena discovery scout",
                recency_minutes=0,
                relevance_score=score,
                market_id=str(row["market_id"]),
                direction="watch_only",
            )
            signals.append((row, signal))
        return signals

    def _build_weather_city_signals(self, rows: list) -> list[tuple[object, DiscoverySignal]]:
        seen_cities: set[str] = set()
        signals: list[tuple[object, DiscoverySignal]] = []
        for row in rows:
            if str(row["category"]) != "weather":
                continue
            params = parse_weather_question(str(row["question"]))
            if not params:
                continue
            city_key = params.canonical_city.lower()
            if city_key in seen_cities:
                continue
            seen_cities.add(city_key)

            has_forecast_history = self._city_has_forecast_history(params.canonical_city)
            needs_coordinates = city_key not in CITY_COORDS
            if has_forecast_history and not needs_coordinates:
                continue

            detail_bits = []
            if not has_forecast_history:
                detail_bits.append("no forecast history recorded yet")
            if needs_coordinates:
                detail_bits.append("city missing coordinate support")
            signal = DiscoverySignal(
                signal_type=SignalType.MARKET_EXPANSION,
                headline=f"Weather city expansion candidate: {params.canonical_city}",
                detail=(
                    f"Active weather market detected for {params.canonical_city}; "
                    + ", ".join(detail_bits)
                    + ". Keep this city on the discovery watchlist before enabling execution."
                ),
                source_url="",
                source_name="Arena discovery scout",
                recency_minutes=0,
                relevance_score=0.98 if needs_coordinates else 0.82,
                market_id=str(row["market_id"]),
                direction="watch_only",
            )
            signals.append((row, signal))
        return signals

    def _build_cross_venue_signals(self, rows: list) -> list[tuple[object, DiscoverySignal]]:
        if not getattr(self.kalshi_adapter, "enabled", False):
            return []

        signals: list[tuple[object, DiscoverySignal]] = []
        for row in rows:
            if str(row["venue"]) != "polymarket" or str(row["category"]) != "weather":
                continue
            params = parse_weather_question(str(row["question"]))
            if not params or params.metric != "high":
                continue
            yes_ask, no_ask = extract_yes_no_asks(row)
            if yes_ask is None or no_ask is None:
                continue
            comparison = self.kalshi_adapter.compare_market_prices(
                params.canonical_city,
                params.date.isoformat(),
                str(row["question"]),
                yes_ask,
                no_ask,
            )
            if not comparison:
                continue
            yes_gap = abs(_safe_float(comparison.get("kalshi_yes_ask")) - yes_ask)
            no_gap = abs(_safe_float(comparison.get("kalshi_no_ask")) - no_ask)
            if max(yes_gap, no_gap) < 0.05:
                continue
            signal = DiscoverySignal(
                signal_type=SignalType.CROSS_VENUE,
                headline=f"Cross-venue weather gap: {params.canonical_city}",
                detail=(
                    f"Kalshi vs Polymarket gap detected for {params.canonical_city} {params.date.isoformat()}: "
                    f"YES gap {yes_gap:.3f}, NO gap {no_gap:.3f}. Preferred YES venue: "
                    f"{comparison.get('preferred_yes_platform')}, preferred NO venue: {comparison.get('preferred_no_platform')}."
                ),
                source_url="",
                source_name="Arena discovery scout",
                recency_minutes=0,
                relevance_score=round(max(yes_gap, no_gap) * 10.0, 3),
                market_id=str(row["market_id"]),
                direction="watch_only",
            )
            signals.append((row, signal))
        return signals

    def _recent_duplicate_exists(self, market_id: str, signal_type: str, headline: str, lookback_hours: int = 24) -> bool:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM discovery_alerts
                WHERE market_id = ?
                  AND signal_type = ?
                  AND headline = ?
                  AND created_at >= datetime('now', ?)
                LIMIT 1
                """,
                (market_id, signal_type, headline, f"-{int(lookback_hours)} hours"),
            ).fetchone()
        return row is not None

    def _city_has_forecast_history(self, city: str) -> bool:
        with self.db.connect() as conn:
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'forecast_history'"
            ).fetchone()
            if not table:
                return False
            row = conn.execute(
                "SELECT 1 FROM forecast_history WHERE lower(location) = lower(?) LIMIT 1",
                (city,),
            ).fetchone()
        return row is not None
