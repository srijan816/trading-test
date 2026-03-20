from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Iterable
import json
import logging
import os
import re
import time

from arena.adapters.base import SearchClient, WeatherDataSource
from arena.adapters.weather_openmeteo import CITY_COORDS
from arena.calibration.crps_tracker import CRPSTracker
from arena.data_sources.station_observations import ObservationUnavailable, get_current_observations
from arena.db import ArenaDB
from arena.exchanges.kalshi_adapter import KalshiAdapter
from arena.intelligence.discovery import DiscoveryQueryBuilder, DiscoverySignal, SignalClassifier, SignalType, should_spend_on_research
from arena.intelligence.discovery_logger import DiscoveryLogger
from arena.intelligence.rate_limiter import NexusRateLimiter
from arena.intelligence.research import format_research_for_packet, research_market, research_topic
from arena.intelligence.research_cache import ResearchCache
from arena.models import Market, ResearchBrief, SearchRecord, SearchResult, utc_now
from arena.strategies.algo_forecast import ForecastConsensusStrategy

logger = logging.getLogger(__name__)

research_cache = ResearchCache()
nexus_rate_limiter = NexusRateLimiter()


class InfoPacketBuilder:
    def __init__(self, db: ArenaDB, search_client: SearchClient | None = None, weather_sources: list[WeatherDataSource] | None = None) -> None:
        self.db = db
        self.search_client = search_client
        self.weather_sources = weather_sources or []
        self._weather_cache: dict[tuple[str, date], list[dict]] = {}
        self._research_cache: dict[str, dict | None] = {}
        self._kalshi_adapter = KalshiAdapter()
        self._crps_tracker = CRPSTracker()
        self._discovery_query_builder = DiscoveryQueryBuilder()
        self._signal_classifier = SignalClassifier()
        self._discovery_logger = DiscoveryLogger(str(self.db.path))
        self._research_stats = {
            "markets_evaluated": 0,
            "cache_hits": 0,
            "nexus_calls": 0,
        }
        self._forecast_strategy = ForecastConsensusStrategy(
            db=db,
            strategy_config={
                "id": "packet_forecast_helper",
                "starting_balance": 1000.0,
                "scope": {},
                "risk": {
                    "max_position_pct": 0.15,
                    "max_positions": 12,
                    "max_daily_loss_pct": 0.20,
                    "min_edge_bps": 300,
                },
            },
        )
        # Tracks whether the research assistant is available (not in cooldown).
        # Reset at start of each build() call via rate-limiter state.
        self._research_assistant_available = True

    async def build(self, strategy_config: dict, strategy_id: str) -> dict:
        self._research_stats = {
            "markets_evaluated": 0,
            "cache_hits": 0,
            "nexus_calls": 0,
        }
        # Reset research assistant availability if rate limiter has recovered
        if nexus_rate_limiter.can_call() and not nexus_rate_limiter.is_in_cooldown():
            if not self._research_assistant_available:
                logger.info("Research assistant recovered from cooldown — re-enabling Nexus calls for %s", strategy_id)
            self._research_assistant_available = True
        portfolio = self.db.get_portfolio(strategy_id)
        scope = strategy_config.get("scope", {})
        markets = self.db.list_markets(status="active")
        allowed_categories = self._normalize_scope_categories(scope.get("categories", []))
        allowed_venues = set(scope.get("venues", []))
        allowed_formats = set(scope.get("supported_formats", []))
        min_volume = float(scope.get("min_volume_usd", 0.0) or 0.0)
        min_time_remaining = float(scope.get("min_time_remaining_hours", 0.0) or 0.0)
        filtered = []
        for row in markets:
            if row["category"] not in allowed_categories:
                continue
            if allowed_formats:
                row_fmt = row["market_format"] if "market_format" in row.keys() else None
                if row_fmt and row_fmt not in allowed_formats:
                    continue
            if allowed_venues and row["venue"] not in allowed_venues:
                continue
            if float(row["volume_usd"]) < min_volume:
                continue
            end_time = datetime.fromisoformat(row["end_time"])
            time_remaining = (end_time - utc_now()).total_seconds() / 3600
            if time_remaining < min_time_remaining:
                continue
            if row["category"] == "weather":
                contract = self._forecast_strategy._parse_weather_contract(row["question"])
                if contract and contract.get("dated", True) and contract["forecast_date"] <= utc_now().date():
                    continue
            filtered.append(row)
        filtered.sort(key=lambda row: float(row["volume_usd"]), reverse=True)
        max_opportunities = int(scope.get("max_opportunities", 12) or 12)
        self._research_stats["markets_evaluated"] = min(len(filtered), max_opportunities)
        search_budget = int(strategy_config.get("search", {}).get("max_searches_per_cycle", 0) or 0)
        search_slots_used = 0
        opportunities: list[dict] = []
        discovery_signals: list[DiscoverySignal] = []
        search_records: list[SearchRecord] = []
        research_briefs: list[ResearchBrief] = []
        research_contexts: list[str] = []
        for row in filtered[:max_opportunities]:
            outcomes = json.loads(row["outcomes_json"])
            end_time = datetime.fromisoformat(row["end_time"])
            parsed_outcomes = [
                {
                    "outcome_id": outcome.get("outcome_id"),
                    "label": outcome.get("label"),
                    "best_bid": outcome.get("best_bid"),
                    "best_ask": outcome.get("best_ask"),
                    "mid_price": outcome.get("mid_price"),
                    "last_trade_price": outcome.get("last_trade_price"),
                    "volume_usd": outcome.get("volume_usd"),
                }
                for outcome in outcomes
            ]
            item = {
                "market_id": row["market_id"],
                "venue": row["venue"],
                "question": row["question"],
                "category": row["category"],
                "volume_usd": row["volume_usd"],
                "liquidity_usd": row["liquidity_usd"],
                "end_time": row["end_time"],
                "time_remaining_hours": round((end_time - utc_now()).total_seconds() / 3600, 2),
                "outcomes": parsed_outcomes,
                "discovery_signals": [],
                "has_breaking_signal": False,
                "signal_direction": "none",
            }
            if row["category"] == "weather":
                item["weather_forecasts"] = await self._build_weather_context(row["question"])
                signal = await self._build_weather_signal(row, outcomes)
                if signal:
                    item["algo_forecast_signal"] = signal
                # For same-day markets, add live observations
                market_date = self._infer_market_date(row["question"])
                if market_date == utc_now().date():
                    obs = await self._build_current_conditions(row["question"])
                    if obs:
                        item["current_conditions"] = obs
            can_research = search_slots_used < search_budget if search_budget > 0 else False
            if can_research:
                enrichment = await self._maybe_apply_research_modes(
                    row,
                    strategy_config,
                    remaining_budget=search_budget - search_slots_used,
                )
                search_slots_used += int(enrichment.get("calls_used", 0) or 0)
                research_result = enrichment.get("research_context")
                if research_result:
                    item["research_context"] = research_result
                    research_text = format_research_for_packet(research_result)
                    if research_text:
                        research_contexts.append(research_text)
                discovery_result = enrichment.get("discovery_context")
                if discovery_result:
                    item["discovery_context"] = discovery_result
                item["discovery_signals"] = enrichment.get("discovery_signals") or []
                item["has_breaking_signal"] = bool(
                    item["discovery_signals"]
                    and any(signal.signal_type != SignalType.NO_SIGNAL for signal in item["discovery_signals"])
                )
                item["signal_direction"] = self._resolve_signal_direction(item["discovery_signals"])
                discovery_signals.extend(
                    signal for signal in item["discovery_signals"] if signal.signal_type != SignalType.NO_SIGNAL
                )
                discovery_text = self._format_discovery_for_packet(
                    item["discovery_signals"],
                    discovery_result,
                )
                if discovery_text:
                    research_contexts.append(discovery_text)
            opportunities.append(item)
            search_cfg = strategy_config.get("search", {})
            research_mode_raw = str(search_cfg.get("research_mode", "probability") or "probability").strip().lower()
            if (
                can_research
                and research_mode_raw not in {"off", "disabled", "none", "false"}
                and not search_cfg.get("research_assistant_enabled", search_cfg.get("provider") == "perplexia")
            ):
                should_search, _ = await self._should_search(row, strategy_config, call_type="search")
                if should_search:
                    records, briefs = await self._run_searches(row, strategy_config)
                    search_records.extend(records)
                    research_briefs.extend(briefs)
                    search_slots_used += 1
        risk_warnings: list[str] = []
        if portfolio:
            if portfolio.peak_value and portfolio.total_value < portfolio.peak_value * 0.85:
                risk_warnings.append("Portfolio drawdown exceeds 15% from peak.")
            if portfolio.positions and len(portfolio.positions) >= strategy_config.get("risk", {}).get("max_positions", 5):
                risk_warnings.append("Portfolio is at or near max open positions.")
        packet = {
            "portfolio": {
                "cash": portfolio.cash if portfolio else 0.0,
                "total_value": portfolio.total_value if portfolio else 0.0,
                "realized_pnl": portfolio.realized_pnl if portfolio else 0.0,
                "unrealized_pnl": portfolio.unrealized_pnl if portfolio else 0.0,
                "positions": [asdict(position) for position in portfolio.positions] if portfolio else [],
            },
            "opportunities": opportunities,
            "recent_decisions": [self._summarize_recent_decision(row) for row in self.db.list_recent_decisions(strategy_id, limit=5)],
            "risk_warnings": risk_warnings,
            "research_contexts": research_contexts,
            "discovery_signals": discovery_signals,
            "research_briefs": [asdict(item) for item in research_briefs],
            "web_searches": [asdict(record) for record in search_records],
        }
        return packet

    async def render_for_prompt(self, strategy_config: dict, strategy_id: str) -> tuple[str, list[SearchRecord]]:
        packet = await self.build(strategy_config, strategy_id)
        return self.render_packet(packet)

    def render_packet(self, packet: dict) -> tuple[str, list[SearchRecord]]:
        lines = [
            "## Portfolio",
            self._render_json(packet["portfolio"]),
            "",
            "## Opportunities",
        ]
        for item in packet["opportunities"]:
            lines.append(self._render_json(item))
        lines.extend(["", "## Recent Decisions"])
        for item in packet["recent_decisions"]:
            lines.append(self._render_json(item))
        lines.extend(["", "## Risk Warnings"])
        lines.extend(packet["risk_warnings"] or ["None"])
        lines.extend(["", "## Research Context"])
        lines.extend(packet.get("research_contexts", []) or ["None"])
        lines.extend(["", "## Discovery Signals"])
        discovery_items = packet.get("discovery_signals", []) or []
        if discovery_items:
            for item in discovery_items:
                lines.append(self._render_json(item))
        else:
            lines.append("None")
        lines.extend(["", "## Research Briefs"])
        for item in packet.get("research_briefs", []):
            lines.append(self._render_json(item))
        lines.extend(["", "## Web Searches"])
        for item in packet["web_searches"]:
            lines.append(self._render_json(item))
        search_records: list[SearchRecord] = []
        for item in packet["web_searches"]:
            retrieved_at = item["retrieved_at"]
            if isinstance(retrieved_at, str):
                retrieved_at = datetime.fromisoformat(retrieved_at)
            search_records.append(
                SearchRecord(
                    query=item["query"],
                    results_summary=item["results_summary"],
                    source_urls=item["source_urls"],
                    retrieved_at=retrieved_at,
                )
            )
        return "\n".join(lines), search_records

    def _render_json(self, value: object) -> str:
        return json.dumps(self._serialize_dates(value), indent=2, ensure_ascii=False, sort_keys=False)

    @staticmethod
    def _serialize_dates(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if is_dataclass(obj):
            return {k: InfoPacketBuilder._serialize_dates(v) for k, v in asdict(obj).items()}
        if isinstance(obj, dict):
            return {k: InfoPacketBuilder._serialize_dates(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [InfoPacketBuilder._serialize_dates(item) for item in obj]
        return obj

    def _summarize_recent_decision(self, row) -> dict:
        actions_json = row["actions_json"]
        actions_count = 0
        if isinstance(actions_json, str):
            try:
                actions_count = len(json.loads(actions_json))
            except Exception:
                actions_count = 0
        summary = {
            "decision_id": row["decision_id"],
            "timestamp": row["timestamp"],
            "expected_edge_bps": row["expected_edge_bps"],
            "actions_count": actions_count,
            "llm_model_used": row["llm_model_used"],
        }
        no_action_reason = row["no_action_reason"]
        if no_action_reason:
            summary["no_action_reason"] = str(no_action_reason)[:240]
        return summary

    async def _build_weather_context(self, question: str) -> list[dict]:
        city = self._infer_city(question)
        target_date = self._infer_market_date(question)
        if not city or not self.weather_sources:
            return []
        cache_key = (city.lower(), target_date)
        cached = self._weather_cache.get(cache_key)
        if cached is not None:
            return cached
        forecasts: list[dict] = []
        for source in self.weather_sources:
            try:
                forecast = await source.get_forecast(city, target_date)
            except Exception:
                continue
            forecasts.append(asdict(forecast))
        self._weather_cache[cache_key] = forecasts
        return forecasts

    async def _build_weather_signal(self, row, outcomes: list[dict]) -> dict | None:
        contract = self._forecast_strategy._parse_weather_contract(row["question"])
        if not contract:
            return None
        yes_outcome, no_outcome = self._forecast_strategy._binary_outcomes(outcomes)
        if not yes_outcome or not no_outcome:
            return None
        ensemble = await self._forecast_strategy._get_ensemble(contract["city"], contract["forecast_date"])
        forecast_value_c = self._forecast_strategy._forecast_value_for_metric(contract, ensemble) if ensemble else None
        if forecast_value_c is None:
            return None
        ensemble_sigma = self._forecast_strategy._sigma_for_metric(contract.get("metric"), ensemble) if ensemble else None
        predicted_yes = self._forecast_strategy._estimate_probability(contract, forecast_value_c, sigma_override=ensemble_sigma)
        predicted_no = 1.0 - predicted_yes
        yes_price = self._forecast_strategy._buy_price(yes_outcome)
        no_price = self._forecast_strategy._buy_price(no_outcome)
        yes_edge_bps = int((predicted_yes - yes_price) * 10000)
        no_edge_bps = int((predicted_no - no_price) * 10000)
        best_side = "BUY_YES" if yes_edge_bps >= no_edge_bps else "BUY_NO"
        best_edge_bps = max(yes_edge_bps, no_edge_bps)
        priced_outcome = yes_outcome if best_side == "BUY_YES" else no_outcome
        priced_probability = predicted_yes if best_side == "BUY_YES" else predicted_no
        market_price = yes_price if best_side == "BUY_YES" else no_price
        signal = {
            "forecast_temperature_c": round(forecast_value_c, 2),
            "forecast_implied_yes_probability": round(predicted_yes, 3),
            "forecast_implied_no_probability": round(predicted_no, 3),
            "buy_yes_ask": round(yes_price, 4),
            "buy_no_ask": round(no_price, 4),
            "yes_best_bid": round(float(yes_outcome.get("best_bid", 0.0) or 0.0), 4),
            "yes_best_ask": round(float(yes_outcome.get("best_ask", yes_price) or yes_price), 4),
            "no_best_bid": round(float(no_outcome.get("best_bid", 0.0) or 0.0), 4),
            "no_best_ask": round(float(no_outcome.get("best_ask", no_price) or no_price), 4),
            "edge_on_yes_bps": yes_edge_bps,
            "edge_on_no_bps": no_edge_bps,
            "algo_would_trade": best_side,
            "algo_edge_bps": best_edge_bps,
            "algo_confidence": self._describe_forecast_confidence(contract, forecast_value_c, best_edge_bps),
            "precomputed_edge_hint": (
                f"Forecast says {forecast_value_c:.1f}°C. The algorithm estimates {priced_probability:.3f} fair probability for "
                f"{priced_outcome.get('label')} and sees executable ask {market_price:.4f}, implying {best_edge_bps} bps of edge on {best_side}."
            ),
            "your_job": (
                "Validate or override this signal. Consider forecast reliability, local weather effects, and any news or anomalies "
                "the algorithm cannot see. If you agree, trade it. If you disagree, explain exactly why."
            ),
        }
        if ensemble:
            signal["ensemble_sources_used"] = ensemble["sources_used"]
            signal["ensemble_source_names"] = ensemble["source_names"]
            signal["ensemble_confidence"] = ensemble["confidence"]
            signal["ensemble_sigma_c"] = ensemble_sigma
            signal["ensemble_sigma_high_c"] = ensemble.get("ensemble_sigma_high_c")
            signal["ensemble_sigma_low_c"] = ensemble.get("ensemble_sigma_low_c")
            signal["bias_correction_applied_c"] = ensemble["bias_correction_applied_c"]
        if self._kalshi_adapter.enabled:
            target_date = contract["forecast_date"].isoformat()
            kalshi_markets = self._kalshi_adapter.find_weather_markets(contract["city"], target_date)
            if kalshi_markets:
                signal["kalshi_market_prices"] = kalshi_markets[:5]
                comparison = self._kalshi_adapter.compare_market_prices(
                    city=contract["city"],
                    target_date=target_date,
                    polymarket_question=row["question"],
                    polymarket_yes_ask=yes_price,
                    polymarket_no_ask=no_price,
                )
                if comparison:
                    signal["cross_platform_comparison"] = comparison
        return signal

    async def _build_current_conditions(self, question: str) -> dict | None:
        """Fetch live weather observations for same-day markets."""
        city = self._infer_city(question)
        if not city:
            return None
        coords = CITY_COORDS.get(city.lower())
        if not coords:
            return None
        lat, lon = coords
        try:
            obs = await get_current_observations(lat, lon, city)
            return {
                "location": obs["location"],
                "current_temperature_c": obs["current_temp_c"],
                "observation_time": obs["observation_time"],
                "high_so_far_c": obs["max_temp_so_far_c"],
                "low_so_far_c": obs["min_temp_so_far_c"],
                "trending": obs["trending"],
                "hours_of_daylight_remaining": obs["hours_remaining"],
                "sources_used": obs["sources_used"],
            }
        except ObservationUnavailable:
            return None

    def _describe_forecast_confidence(self, contract: dict, forecast_high_c: float, best_edge_bps: int) -> str:
        unit_value = forecast_high_c if contract["unit"] == "c" else (forecast_high_c * 9 / 5) + 32
        if contract["shape"] == "between":
            lower_gap = abs(unit_value - contract["lower"])
            upper_gap = abs(unit_value - contract["upper"])
            gap = min(lower_gap, upper_gap)
        else:
            threshold = contract.get("threshold", unit_value)
            gap = abs(unit_value - threshold)
        if best_edge_bps >= 3000 and gap >= 0.4:
            return f"high — forecast is {gap:.1f}{contract['unit'].upper()} from the critical bucket boundary"
        if best_edge_bps >= 1000:
            return f"moderate — forecast is {gap:.1f}{contract['unit'].upper()} from the critical bucket boundary"
        return f"low — forecast is only {gap:.1f}{contract['unit'].upper()} from the critical bucket boundary"

    def _infer_city(self, question: str) -> str | None:
        lowered = question.lower()
        for city in [
            "hong kong",
            "new york",
            "chicago",
            "london",
            "atlanta",
            "ankara",
            "buenos aires",
            "seattle",
            "toronto",
            "taipei",
        ]:
            if city in lowered:
                return city
        for pattern in (
            r"\bin ([a-z .'-]+?) be\b",
            r"\bin ([a-z .'-]+?) on\b",
        ):
            match = re.search(pattern, lowered)
            if match:
                return match.group(1).strip()
        return None

    def _infer_market_date(self, question: str) -> date:
        match = re.search(r"\bon ([A-Z][a-z]+) (\d{1,2})(?:,? (\d{4}))?", question)
        if not match:
            return utc_now().date()
        month_name, day_text, year_text = match.groups()
        year = int(year_text) if year_text else utc_now().year
        try:
            return datetime.strptime(f"{month_name} {int(day_text)} {year}", "%B %d %Y").date()
        except ValueError:
            return utc_now().date()

    def _normalize_scope_categories(self, categories: Iterable[str]) -> set[str]:
        aliases = {
            "events": "event",
            "event": "event",
            "regulation": "legal",
            "tech": "science_tech",
        }
        return {aliases.get(category, category) for category in categories}

    def _resolve_research_plan(self, search_cfg: dict) -> list[str]:
        raw_mode = str(search_cfg.get("research_mode", "probability") or "probability").strip().lower()
        if raw_mode in {"off", "disabled", "none", "false"}:
            return []
        if raw_mode == "discovery":
            return ["discovery"]
        if raw_mode == "both":
            return ["discovery", "probability"]
        if raw_mode == "probability":
            return ["probability"]
        if raw_mode in {"quick", "standard", "deep"}:
            return ["probability"]
        return ["probability"]

    def _resolve_probability_depth(self, search_cfg: dict) -> str:
        raw_mode = str(search_cfg.get("research_mode", "") or "").strip().lower()
        if raw_mode in {"quick", "standard", "deep"}:
            return raw_mode
        return str(search_cfg.get("market_research_depth") or search_cfg.get("mode") or "standard")

    def _resolve_discovery_depth(self, search_cfg: dict) -> str:
        return str(search_cfg.get("mode") or "standard")

    def _estimated_call_cost(self, search_cfg: dict) -> float:
        return float(search_cfg.get("estimated_call_cost_usd") or os.getenv("NEXUS_ESTIMATED_CALL_COST_USD", "0.05"))

    def _build_research_cost_context(self, row, strategy_config: dict) -> dict:
        risk_cfg = strategy_config.get("risk", {})
        max_position_size = float(
            risk_cfg.get("max_order_usd")
            or risk_cfg.get("max_trade_size_usd")
            or os.getenv("RISK_MAX_SINGLE_TRADE_SIZE", "50")
        )
        edge_estimate = float(risk_cfg.get("min_edge_bps", 0) or 0) / 10000.0
        if edge_estimate <= 0:
            edge_estimate = 0.02
        end_time = datetime.fromisoformat(row["end_time"])
        return {
            "market_id": str(row["market_id"]),
            "category": str(row["category"] or ""),
            "volume_usd": float(row["volume_usd"] or 0.0),
            "resolution_hours": max(0.0, (end_time - utc_now()).total_seconds() / 3600),
            "max_position_size": max_position_size,
            "edge_estimate": edge_estimate,
            "breaking_news_candidate": str(row["category"] or "").lower() in {"politics", "legal", "geopolitics"},
        }

    def _resolve_signal_direction(self, signals: list[DiscoverySignal]) -> str:
        directions = [signal.direction for signal in signals if signal.signal_type != SignalType.NO_SIGNAL and signal.direction]
        unique = {direction for direction in directions if direction not in {"none", "ambiguous"}}
        if len(unique) == 1:
            return next(iter(unique))
        if len(unique) > 1:
            return "ambiguous"
        if any(direction == "ambiguous" for direction in directions):
            return "ambiguous"
        return "none"

    def _format_discovery_for_packet(self, signals: list[DiscoverySignal], discovery_result: dict | None) -> str:
        if not signals:
            return ""
        lines = [
            "=== DISCOVERY CONTEXT ===",
            "Use this as an alert feed for new information, not as a probability override.",
        ]
        if discovery_result:
            lines.append(f"Query: \"{discovery_result.get('query', 'N/A')}\"")
            lines.append(f"Mode: {discovery_result.get('mode', 'standard')}")
        for index, signal in enumerate(signals[:3], 1):
            lines.append(
                f"  {index}. [{signal.signal_type.value}] {signal.headline} "
                f"(direction={signal.direction}, relevance={signal.relevance_score:.2f}, recency_min={signal.recency_minutes})"
            )
        return "\n".join(lines)

    async def _maybe_apply_research_modes(self, row, strategy_config: dict, remaining_budget: int) -> dict:
        search_cfg = strategy_config.get("search", {})
        plan = self._resolve_research_plan(search_cfg)
        result = {
            "research_context": None,
            "discovery_context": None,
            "discovery_signals": [],
            "calls_used": 0,
        }
        if remaining_budget <= 0 or not plan:
            return result

        for call_type in plan:
            if result["calls_used"] >= remaining_budget:
                break
            if call_type == "discovery":
                discovery_result, used_call = await self._maybe_discover_signals(row, strategy_config)
                result["calls_used"] += int(used_call)
                if discovery_result:
                    result["discovery_context"] = discovery_result.get("discovery_context")
                    result["discovery_signals"] = discovery_result.get("discovery_signals") or []
            elif call_type == "probability":
                research_result, used_call = await self._maybe_research_market(
                    row,
                    strategy_config,
                    research_depth=self._resolve_probability_depth(search_cfg),
                )
                result["calls_used"] += int(used_call)
                if research_result:
                    result["research_context"] = research_result
        return result

    async def _should_search(self, row, strategy_config: dict, *, call_type: str = "research") -> tuple[bool, str | None]:
        """Returns (should_search, reason_if_not). Logs research_call_attempted/blocked events."""
        search_cfg = strategy_config.get("search", {})
        strategy_id = str(strategy_config.get("id") or "unknown")
        market_id = str(row["market_id"])

        if not search_cfg.get("enabled"):
            self.db.record_event(
                "research_call_blocked",
                {"strategy_id": strategy_id, "market_id": market_id, "reason": "search_disabled", "call_type": call_type},
            )
            return False, "search_disabled"

        # Check cooldown
        if nexus_rate_limiter.is_in_cooldown():
            expires_in = nexus_rate_limiter.cooldown_expires_in() or 0
            self.db.record_event(
                "research_call_blocked",
                {
                    "strategy_id": strategy_id,
                    "market_id": market_id,
                    "reason": "cooldown",
                    "cooldown_expires_in_seconds": round(expires_in),
                    "call_type": call_type,
                },
            )
            return False, "cooldown"

        # Check rate limit
        if not nexus_rate_limiter.can_call():
            self.db.record_event(
                "research_call_blocked",
                {
                    "strategy_id": strategy_id,
                    "market_id": market_id,
                    "reason": "rate_limit",
                    "remaining_calls": nexus_rate_limiter.remaining(),
                    "call_type": call_type,
                },
            )
            return False, "rate_limit"

        # Check trigger conditions
        triggers = set(search_cfg.get("trigger_conditions", []))
        if "always" not in triggers:
            end_time = datetime.fromisoformat(row["end_time"])
            if "near_resolution" not in triggers or end_time > utc_now() + timedelta(hours=24):
                self.db.record_event(
                    "research_call_blocked",
                    {
                        "strategy_id": strategy_id,
                        "market_id": market_id,
                        "reason": "trigger_conditions_not_met",
                        "call_type": call_type,
                    },
                )
                return False, "trigger_conditions_not_met"

        # All gates passed — log the actual attempt
        self.db.record_event(
            "research_call_attempted",
            {"strategy_id": strategy_id, "market_id": market_id, "call_type": call_type},
        )
        return True, None

    async def _run_searches(self, row, strategy_config: dict) -> tuple[list[SearchRecord], list[ResearchBrief]]:
        if not self.search_client:
            return [], []
        query = str(row["question"])
        market_id = str(row["market_id"])
        strategy_id = str(strategy_config.get("id") or "")
        num_results = int(strategy_config.get("search", {}).get("max_sources", 5) or 5)
        is_nexus_search = self.search_client.__class__.__name__ == "PerplexiaSearchClient"
        if is_nexus_search and not nexus_rate_limiter.can_call():
            logger.warning("Nexus rate limit reached, skipping search brief for query '%s'", query)
            return [], []
        start_time = time.time()
        try:
            results = await self.search_client.search(query, num_results=num_results)
            if is_nexus_search:
                nexus_rate_limiter.record_call()
                self._research_stats["nexus_calls"] += 1
        except Exception as exc:
            duration_ms = int((time.time() - start_time) * 1000)
            if is_nexus_search:
                self.db.log_research_entry(
                    strategy=strategy_id,
                    market_id=market_id,
                    market_question=query,
                    query_sent=query,
                    endpoint="/api/v1/research",
                    mode=getattr(self.search_client, "mode", None),
                    duration_ms=duration_ms,
                    error=str(exc),
                )
            self.db.record_event(
                "search_warning",
                {
                    "strategy_id": strategy_config.get("id"),
                    "market_id": market_id,
                    "query": query,
                    "error": str(exc),
                },
                strategy_id=strategy_config.get("id"),
            )
            return [], []
        if not results:
            return [], []
        records = [
            SearchRecord(
                query=query,
                results_summary=" | ".join(result.snippet for result in results),
                source_urls=[result.url for result in results],
                retrieved_at=utc_now(),
            )
        ]
        briefs: list[ResearchBrief] = []
        for result in results:
            metadata = result.metadata if isinstance(result.metadata, dict) else {}
            if metadata.get("provider") != "perplexia":
                continue
            report = str(metadata.get("report", "") or "").strip()
            if not report:
                continue
            briefs.append(
                ResearchBrief(
                    query=query,
                    provider="perplexia",
                    report_summary=report[:1200],
                    source_urls=[item.url for item in results if item.url and not item.url.startswith("perplexia://")],
                    follow_ups=[str(item) for item in metadata.get("follow_ups", []) if item],
                    session_id=str(metadata.get("session_id")) if metadata.get("session_id") else None,
                )
            )
            break
        if is_nexus_search:
            primary = results[0] if results else None
            primary_meta = primary.metadata if primary and isinstance(primary.metadata, dict) else {}
            full_report = str(primary_meta.get("report", "") or "")
            source_rows = [
                {"url": item.url, "title": item.title}
                for item in results
                if item.url and not item.url.startswith("perplexia://")
            ]
            self.db.log_research_entry(
                strategy=strategy_id,
                market_id=market_id,
                market_question=query,
                query_sent=query,
                endpoint=str(primary_meta.get("endpoint", "/api/v1/research")),
                mode=str(primary_meta.get("mode", getattr(self.search_client, "mode", "standard"))),
                model_used=primary_meta.get("model_used"),
                duration_ms=int((time.time() - start_time) * 1000),
                report_length=len(full_report),
                sources_count=len(source_rows),
                sources_json=source_rows,
                report_summary=full_report[:500] if full_report else None,
                full_report=full_report or None,
                from_cache=False,
            )
        return records, briefs

    async def _maybe_discover_signals(self, row, strategy_config: dict) -> tuple[dict | None, int]:
        search_cfg = strategy_config.get("search", {})
        strategy_id = str(strategy_config.get("id") or "unknown")
        market_id = str(row["market_id"])

        if not search_cfg.get("enabled"):
            return None, 0
        if not search_cfg.get("research_assistant_enabled", search_cfg.get("provider") == "perplexia"):
            return None, 0

        should_search, _ = await self._should_search(row, strategy_config, call_type="discovery")
        if not should_search:
            return None, 0

        spend_ok, spend_reason = should_spend_on_research(
            self._build_research_cost_context(row, strategy_config),
            estimated_call_cost_usd=self._estimated_call_cost(search_cfg),
        )
        if not spend_ok:
            self.db.record_event(
                "research_call_blocked",
                {
                    "strategy_id": strategy_id,
                    "market_id": market_id,
                    "reason": "cost_gate",
                    "call_type": "discovery",
                    "detail": spend_reason,
                },
            )
            return None, 0

        cache_key = f"discovery:{market_id}"
        cached = self._research_cache.get(cache_key)
        if cached is not None:
            self._research_stats["cache_hits"] += 1
            return cached, 0

        market_payload = await self._build_market_research_payload(row, strategy_config)
        discovery_query = self._discovery_query_builder.build_query(
            market_id=market_id,
            question=str(row["question"]),
            category=str(row["category"] or "event"),
            ensemble_data=market_payload.get("ensemble_data"),
            market_data=market_payload.get("market_data"),
        )

        start_time = time.time()
        try:
            result = await research_topic(
                discovery_query.query_text,
                mode=self._resolve_discovery_depth(search_cfg),
                output_length="short",
                max_sources=discovery_query.max_sources,
                model=market_payload.get("model"),
            )
            nexus_rate_limiter.record_call()
            self._research_stats["nexus_calls"] += 1
        except Exception as exc:
            duration_ms = int((time.time() - start_time) * 1000)
            nexus_rate_limiter.set_cooldown()
            self._log_market_research(
                row=row,
                strategy_config=strategy_config,
                market_payload=market_payload,
                mode="discovery",
                duration_ms=duration_ms,
                error=str(exc),
                query_sent_override=discovery_query.query_text,
                report_summary_override="Discovery call failed",
            )
            self.db.record_event(
                "research_call_blocked",
                {
                    "strategy_id": strategy_id,
                    "market_id": market_id,
                    "reason": "nexus_error_triggered_cooldown",
                    "call_type": "discovery",
                    "error": str(exc),
                },
            )
            return None, 1

        duration_ms = int((time.time() - start_time) * 1000)
        report_text = ""
        sources = []
        if result:
            report_text = str(result.get("full_report") or result.get("summary") or "").strip()
            sources = result.get("sources_detail") or []
        signals = self._signal_classifier.classify(report_text, sources, str(row["question"]), str(row["category"] or "event"))
        for signal in signals:
            signal.market_id = market_id

        if signals and signals[0].signal_type == SignalType.NO_SIGNAL:
            self._discovery_logger.log_no_signal(
                market_id=market_id,
                market_question=str(row["question"]),
                category=str(row["category"] or "event"),
                strategy_id=strategy_id,
            )
        else:
            for signal in signals:
                self._discovery_logger.log_signal(
                    signal,
                    strategy_id=strategy_id,
                    market_question=str(row["question"]),
                    category=str(row["category"] or "event"),
                )

        summary = next(
            (signal.headline for signal in signals if signal.signal_type != SignalType.NO_SIGNAL),
            "No new signals detected",
        )
        self._log_market_research(
            row=row,
            strategy_config=strategy_config,
            market_payload=market_payload,
            mode="discovery",
            result=result,
            duration_ms=duration_ms,
            from_cache=False,
            query_sent_override=discovery_query.query_text,
            report_summary_override=summary,
        )
        payload = {
            "discovery_query": discovery_query,
            "discovery_context": result,
            "discovery_signals": signals,
        }
        self._research_cache[cache_key] = payload
        return payload, 1

    async def _maybe_research_market(self, row, strategy_config: dict, *, research_depth: str | None = None) -> tuple[dict | None, int]:
        search_cfg = strategy_config.get("search", {})
        strategy_id = str(strategy_config.get("id") or "unknown")
        market_id = str(row["market_id"])

        if not search_cfg.get("enabled"):
            return None, 0
        if not search_cfg.get("research_assistant_enabled", search_cfg.get("provider") == "perplexia"):
            return None, 0

        # Use the new _should_search that returns (should_search, reason)
        should_search, block_reason = await self._should_search(row, strategy_config, call_type="probability")
        if not should_search:
            return None, 0

        spend_ok, spend_reason = should_spend_on_research(
            self._build_research_cost_context(row, strategy_config),
            estimated_call_cost_usd=self._estimated_call_cost(search_cfg),
        )
        if not spend_ok:
            self.db.record_event(
                "research_call_blocked",
                {
                    "strategy_id": strategy_id,
                    "market_id": market_id,
                    "reason": "cost_gate",
                    "call_type": "probability",
                    "detail": spend_reason,
                },
            )
            return None, 0

        market_payload = await self._build_market_research_payload(row, strategy_config)
        if market_id in self._research_cache:
            cached = self._research_cache[market_id]
            if cached is not None:
                self._research_stats["cache_hits"] += 1
            return cached, 0

        market_type = str(market_payload.get("market_type") or "event")
        market_data = market_payload.get("market_data") or {}
        ensemble_data = market_payload.get("ensemble_data") or {}
        current_price = market_data.get("current_price_yes")
        current_ensemble_mu = ensemble_data.get("mu")
        cached = research_cache.get(
            market_id=market_id,
            market_type=market_type,
            current_price=float(current_price) if current_price is not None else None,
            current_ensemble_mu=float(current_ensemble_mu) if current_ensemble_mu is not None else None,
        )
        if cached is not None:
            self._research_stats["cache_hits"] += 1
            self._log_market_research(
                row=row,
                strategy_config=strategy_config,
                market_payload=market_payload,
                mode=str(cached.get("mode") or search_cfg.get("research_mode", "quick")),
                result=cached,
                duration_ms=0,
                from_cache=True,
            )
            self._research_cache[market_id] = cached
            return cached, 0

        requested_mode = str(research_depth or self._resolve_probability_depth(search_cfg))
        has_prior_research = research_cache.has_any(market_id)
        mode = self._select_research_mode(market_type, requested_mode, has_prior_research)

        # Rate limit: try stale cache before giving up
        if not nexus_rate_limiter.can_call():
            logger.warning("Nexus rate limit reached, using cached/stale research or skipping")
            stale = research_cache.get(
                market_id=market_id,
                market_type=market_type,
                current_price=float(current_price) if current_price is not None else None,
                current_ensemble_mu=float(current_ensemble_mu) if current_ensemble_mu is not None else None,
                allow_stale=True,
            )
            if stale is not None:
                self._research_stats["cache_hits"] += 1
                self._log_market_research(
                    row=row,
                    strategy_config=strategy_config,
                    market_payload=market_payload,
                    mode=str(stale.get("mode") or mode),
                    result=stale,
                    duration_ms=0,
                    from_cache=True,
                )
                self.db.record_event(
                    "research_call_blocked",
                    {
                        "strategy_id": strategy_id,
                        "market_id": market_id,
                        "reason": "rate_limit_fell_back_to_stale_cache",
                        "remaining_calls": nexus_rate_limiter.remaining(),
                    },
                )
            else:
                self.db.record_event(
                    "research_call_blocked",
                    {
                        "strategy_id": strategy_id,
                        "market_id": market_id,
                        "reason": "rate_limit_no_stale_cache",
                        "remaining_calls": nexus_rate_limiter.remaining(),
                    },
                )
            self._research_cache[market_id] = stale
            return stale, 0

        start_time = time.time()
        result = None
        try:
            result = await research_market(
                str(row["question"]),
                mode=mode,
                market_type=market_payload["market_type"],
                market_data=market_payload["market_data"],
                ensemble_data=market_payload.get("ensemble_data"),
                calibration_data=market_payload.get("calibration_data"),
                model=market_payload.get("model"),
            )
            nexus_rate_limiter.record_call()
            self._research_stats["nexus_calls"] += 1
        except Exception as exc:
            duration_ms = int((time.time() - start_time) * 1000)
            self._log_market_research(
                row=row,
                strategy_config=strategy_config,
                market_payload=market_payload,
                mode=mode,
                duration_ms=duration_ms,
                error=str(exc),
            )
            # Set cooldown so next scan cycle can retry
            nexus_rate_limiter.set_cooldown()
            self.db.record_event(
                "research_call_blocked",
                {
                    "strategy_id": strategy_id,
                    "market_id": market_id,
                    "reason": "nexus_error_triggered_cooldown",
                    "call_type": "probability",
                    "error": str(exc),
                },
            )
            self.db.record_event(
                "research_warning",
                {
                    "strategy_id": strategy_id,
                    "market_id": market_id,
                    "query": row["question"],
                    "error": str(exc),
                },
                strategy_id=strategy_id,
            )
            result = None

        if result is None:
            # No result and no exception: enter cooldown so next cycle can retry
            nexus_rate_limiter.set_cooldown()
            self._research_cache[market_id] = None
            return None, 1
        else:
            self._log_market_research(
                row=row,
                strategy_config=strategy_config,
                market_payload=market_payload,
                mode=mode,
                result=result,
                duration_ms=int((time.time() - start_time) * 1000),
                from_cache=False,
            )
            research_cache.put(
                market_id=market_id,
                result=result,
                market_price=float(current_price) if current_price is not None else None,
                ensemble_mu=float(current_ensemble_mu) if current_ensemble_mu is not None else None,
            )
        self._research_cache[market_id] = result
        return result, 1

    def _extract_binary_market_prices(self, outcomes: list[dict]) -> tuple[float | None, float | None]:
        yes_price = None
        no_price = None
        for outcome in outcomes:
            label = str(outcome.get("label", "")).strip().lower()
            ask = outcome.get("best_ask")
            mid = outcome.get("mid_price")
            price = ask if ask is not None else mid
            if price is None:
                continue
            if label == "yes":
                yes_price = float(price)
            elif label == "no":
                no_price = float(price)
        return yes_price, no_price

    def _build_calibration_context(self, city: str | None) -> dict | None:
        if not city:
            return None
        relevant = [entry for entry in self._crps_tracker.history if str(entry.get("city", "")).lower() == city.lower()]
        if not relevant:
            return None
        recent = relevant[-5:]
        recent_crps = sum(float(entry.get("crps", 0.0)) for entry in recent) / len(recent)
        calibration_ratio = sum(float(entry.get("calibration_ratio", 1.0)) for entry in recent) / len(recent)
        suggestion = self._crps_tracker.suggest_sigma_adjustment(city=city)
        sigma_direction = suggestion.get("direction") if suggestion.get("status") == "ready" else "insufficient_data"
        return {
            "recent_crps": round(recent_crps, 4),
            "calibration_ratio": round(calibration_ratio, 3),
            "sigma_suggestion": sigma_direction,
            "city_track_record": f"{len(relevant)} resolved, avg calibration ratio {round(calibration_ratio, 3)}",
        }

    def _select_research_mode(self, market_type: str, requested_mode: str, has_prior_research: bool) -> str:
        normalized_mode = str(requested_mode or "quick").lower()
        if normalized_mode == "deep":
            return "deep"
        if market_type == "weather":
            return "quick"
        if market_type == "event":
            return "quick" if has_prior_research else "standard"
        if market_type == "crypto":
            return "quick"
        return normalized_mode

    def _log_market_research(
        self,
        *,
        row,
        strategy_config: dict,
        market_payload: dict,
        mode: str,
        duration_ms: int,
        result: dict | None = None,
        from_cache: bool = False,
        error: str | None = None,
        query_sent_override: str | None = None,
        report_summary_override: str | None = None,
    ) -> None:
        strategy_id = str(strategy_config.get("id") or "")
        market_id = str(row["market_id"])
        market_question = str(row["question"])
        if query_sent_override is not None:
            query_sent = query_sent_override
        elif result and result.get("fallback_used"):
            query_sent = str((result or {}).get("query") or market_question)
        else:
            query_sent = json.dumps(market_payload, ensure_ascii=False, default=str)
        sources_detail = self._normalize_research_sources(result)
        full_report = self._extract_research_report(result)
        confidence = None
        probability = None
        edge_assessment = None
        reasoning_trace = None
        model_used = None
        endpoint = "/api/v1/research" if mode == "discovery" else "/api/v1/market-research"
        resolved_mode = mode
        if result:
            confidence = result.get("confidence_label") or result.get("confidence")
            probability = result.get("probability")
            edge_assessment = result.get("edge_assessment")
            reasoning_trace = result.get("reasoning_trace")
            model_used = result.get("model_used")
            endpoint = str(result.get("endpoint") or endpoint)
            resolved_mode = str(result.get("mode") or mode)
        probability_value = None
        if probability is not None:
            try:
                probability_value = float(probability)
            except (TypeError, ValueError):
                probability_value = None
        self.db.log_research_entry(
            strategy=strategy_id,
            market_id=market_id,
            market_question=market_question,
            query_sent=query_sent,
            endpoint=endpoint,
            mode=resolved_mode,
            model_used=model_used,
            duration_ms=duration_ms,
            report_length=len(full_report),
            sources_count=len(sources_detail),
            sources_json=sources_detail,
            report_summary=report_summary_override or (full_report[:500] if full_report else None),
            full_report=full_report or None,
            reasoning_trace=str(reasoning_trace).strip() if reasoning_trace else None,
            probability=probability_value,
            confidence=str(confidence) if confidence is not None else None,
            edge_assessment=edge_assessment,
            from_cache=from_cache,
            error=error,
        )

    def _normalize_research_sources(self, result: dict | None) -> list[dict[str, str]]:
        if not result:
            return []
        sources_detail = result.get("sources_detail")
        if isinstance(sources_detail, list):
            normalized = []
            for item in sources_detail:
                if isinstance(item, dict):
                    url = str(item.get("url", "") or "")
                    title = str(item.get("title", "") or "Untitled source")
                else:
                    url = str(item or "")
                    title = url or "Untitled source"
                if not url:
                    continue
                normalized.append({"url": url, "title": title})
            if normalized:
                return normalized[:10]
        return [
            {"url": str(item), "title": str(item)}
            for item in (result.get("sources") or [])
            if item
        ][:10]

    def _extract_research_report(self, result: dict | None) -> str:
        if not result:
            return ""
        structured = result.get("structured") if isinstance(result.get("structured"), dict) else {}
        return str(
            structured.get("reasoning")
            or result.get("full_report")
            or result.get("summary")
            or ""
        ).strip()[:12000]

    def get_research_stats(self) -> dict[str, int]:
        return {
            **self._research_stats,
            "remaining_in_rate_window": nexus_rate_limiter.remaining(),
            "cache_files_total": research_cache.stats()["total"],
        }

    async def _build_market_research_payload(self, row, strategy_config: dict) -> dict:
        outcomes = json.loads(row["outcomes_json"])
        yes_price, no_price = self._extract_binary_market_prices(outcomes)
        market_type = str(row["category"] or "event")
        if market_type not in {"weather", "event", "crypto"}:
            market_type = "event"

        payload = {
            "market_type": market_type,
            "market_data": {
                "current_price_yes": float(yes_price if yes_price is not None else 0.5),
                "current_price_no": float(no_price if no_price is not None else (1.0 - (yes_price if yes_price is not None else 0.5))),
                "volume": float(row["volume_usd"] or 0.0),
                "resolution_time": row["end_time"],
                "platform": row["venue"],
                "market_id": str(row["market_id"]),
            },
            "model": strategy_config.get("search", {}).get("model_id"),
        }

        if market_type == "weather":
            contract = self._forecast_strategy._parse_weather_contract(str(row["question"]))
            if contract:
                ensemble = await self._forecast_strategy._get_ensemble(contract["city"], contract["forecast_date"])
                if ensemble:
                    threshold_c = self._forecast_strategy._contract_threshold_c(contract)
                    forecast_value_c = self._forecast_strategy._forecast_value_for_metric(contract, ensemble)
                    if forecast_value_c is None:
                        payload["calibration_data"] = self._build_calibration_context(contract["city"])
                        return payload
                    ensemble_sigma = self._forecast_strategy._sigma_for_metric(contract.get("metric"), ensemble)
                    payload["ensemble_data"] = {
                        "mu": float(forecast_value_c),
                        "sigma": float(ensemble_sigma),
                        "unit": "celsius",
                        "sources": {
                            str(item["source"]): float(
                                item["temp_low_c"] if contract.get("metric") == "low" else item["temp_high_c"]
                            )
                            for item in ensemble.get("raw_forecasts", [])
                            if (item.get("temp_low_c") if contract.get("metric") == "low" else item.get("temp_high_c")) is not None
                        },
                        "threshold": float(threshold_c) if threshold_c is not None else None,
                        "ensemble_probability": float(
                            self._forecast_strategy._estimate_probability(
                                contract,
                                float(forecast_value_c),
                                sigma_override=float(ensemble_sigma),
                            )
                        ),
                    }
                payload["calibration_data"] = self._build_calibration_context(contract["city"])

        return payload
