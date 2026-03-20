from __future__ import annotations

import json
import logging
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Mapping

from arena.adapters.weather_openmeteo import CITY_COORDS, OpenMeteoSource
from arena.data_sources.station_observations import ObservationUnavailable, get_current_observations
from arena.data_sources.weather_constants import normalize_weather_city
from arena.data_sources.weather_ensemble import WeatherDataUnavailable, get_ensemble_forecast
from arena.intelligence.output_parser import parse_decision_payload
from arena.strategies.base import Strategy

logger = logging.getLogger(__name__)

DEFAULT_TIME_DECAY = {
    "hours_24_plus": 1.0,
    "hours_12_to_24": 0.8,
    "hours_6_to_12": 0.6,
    "hours_2_to_6": 0.4,
    "hours_under_2": 0.2,
}

TEMPERATURE_PATTERN = re.compile(
    r"^Will the (?P<metric>highest|lowest) temperature in (?P<city>.+?) "
    r"be (?P<body>.+?) on (?P<date>[A-Z][a-z]+ \d{1,2}(?:, \d{4})?)\?$",
    re.IGNORECASE,
)
RAIN_PATTERN = re.compile(
    r"^Will it rain in (?P<city>.+?) on (?P<date>[A-Z][a-z]+ \d{1,2}(?:, \d{4})?)\?$",
    re.IGNORECASE,
)


@dataclass(slots=True)
class WeatherMarketParams:
    city: str
    canonical_city: str
    metric: str
    direction: str
    date: date
    unit: str
    threshold: float | None = None
    lower_bound: float | None = None
    upper_bound: float | None = None
    dated: bool = True

    @property
    def shape(self) -> str:
        mapping = {
            "above": "at_or_above",
            "below": "at_or_below",
            "between": "between",
            "exact": "exact",
            "rain": "rain",
        }
        return mapping[self.direction]

    def to_contract_dict(self) -> dict:
        return {
            "city": self.canonical_city,
            "display_city": self.city,
            "forecast_date": self.date,
            "metric": self.metric,
            "direction": self.direction,
            "shape": self.shape,
            "unit": self.unit,
            "threshold": self.threshold,
            "lower": self.lower_bound,
            "upper": self.upper_bound,
            "dated": self.dated,
        }


def _parse_question_date(date_text: str) -> date:
    now = datetime.now(timezone.utc)
    text = date_text.strip()
    for fmt in ("%B %d, %Y", "%B %d %Y", "%B %d"):
        try:
            parsed = datetime.strptime(text, fmt).date()
            if fmt == "%B %d":
                return parsed.replace(year=now.year)
            return parsed
        except ValueError:
            continue
    raise ValueError(f"Unsupported weather market date: {date_text}")


def parse_weather_question(question: str) -> WeatherMarketParams | None:
    text = " ".join(str(question or "").strip().split())
    if not text:
        return None

    rain_match = RAIN_PATTERN.match(text)
    if rain_match:
        city = rain_match.group("city").strip()
        parsed_date = _parse_question_date(rain_match.group("date"))
        return WeatherMarketParams(
            city=city,
            canonical_city=normalize_weather_city(city),
            metric="rain",
            direction="rain",
            date=parsed_date,
            unit="probability",
        )

    match = TEMPERATURE_PATTERN.match(text)
    if not match:
        return None

    metric = "high" if match.group("metric").lower() == "highest" else "low"
    city = match.group("city").strip()
    parsed_date = _parse_question_date(match.group("date"))
    body = match.group("body").strip()

    between_match = re.search(
        r"between\s+(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\s*(?:and|-)\s*(-?\d+(?:\.\d+)?)\s*°?\s*\2",
        body,
        re.IGNORECASE,
    )
    if not between_match:
        between_match = re.search(
            r"between\s+(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*°?\s*([CF])",
            body,
            re.IGNORECASE,
        )
    if between_match:
        lower = float(between_match.group(1))
        if between_match.lastindex == 3 and between_match.group(2).isalpha():
            unit = between_match.group(2).lower()
            upper = float(between_match.group(3))
        else:
            upper = float(between_match.group(2))
            unit = between_match.group(3).lower()
        return WeatherMarketParams(
            city=city,
            canonical_city=normalize_weather_city(city),
            metric=metric,
            direction="between",
            date=parsed_date,
            unit=unit,
            threshold=(lower + upper) / 2.0,
            lower_bound=lower,
            upper_bound=upper,
        )

    threshold_match = re.search(
        r"(-?\d+(?:\.\d+)?)\s*°?\s*([CF])(?:\s*or\s*(above|below|higher|lower))?$",
        body,
        re.IGNORECASE,
    )
    if not threshold_match:
        return None

    threshold = float(threshold_match.group(1))
    unit = threshold_match.group(2).lower()
    direction_text = (threshold_match.group(3) or "").lower()
    direction = "exact"
    if direction_text in {"above", "higher"}:
        direction = "above"
    elif direction_text in {"below", "lower"}:
        direction = "below"

    return WeatherMarketParams(
        city=city,
        canonical_city=normalize_weather_city(city),
        metric=metric,
        direction=direction,
        date=parsed_date,
        unit=unit,
        threshold=threshold,
    )


def compute_time_decay_multiplier(
    end_time: datetime,
    now: datetime | None = None,
    decay_config: Mapping[str, float] | None = None,
) -> float:
    now = now or datetime.now(timezone.utc)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)
    hours_remaining = (end_time - now).total_seconds() / 3600.0
    if hours_remaining <= 0:
        return 0.0
    config = {**DEFAULT_TIME_DECAY, **(dict(decay_config or {}))}
    if hours_remaining >= 24:
        return float(config["hours_24_plus"])
    if hours_remaining >= 12:
        return float(config["hours_12_to_24"])
    if hours_remaining >= 6:
        return float(config["hours_6_to_12"])
    if hours_remaining >= 2:
        return float(config["hours_2_to_6"])
    return float(config["hours_under_2"])


class ForecastConsensusStrategy(Strategy):
    def __init__(self, db, strategy_config: dict) -> None:
        super().__init__(db, strategy_config)
        self.supported_formats = (
            strategy_config.get("scope", {}).get("supported_formats")
            or strategy_config.get("supported_formats")
            or ["binary", "numeric_bracket"]
        )
        self._ensemble_cache: dict[tuple[str, date], dict | None] = {}
        self._observation_cache: dict[str, dict | None] = {}
        self._coord_cache: dict[str, tuple[float, float]] = {}
        self._observations_recorded: set[str] = set()
        self._geo_source = OpenMeteoSource()

    async def generate_decision(self):
        self._log_pending_adjustments()
        now = datetime.now(timezone.utc)
        scoped_markets = [
            row
            for row in self.db.list_markets(category="weather", status="active")
            if self.is_market_eligible(row) and self._has_time_remaining(row, now)
        ]
        if not scoped_markets and self.strategy_config.get("scope", {}).get("skip_if_no_markets"):
            payload = self._build_skip_payload(
                now,
                "No active weather markets available; cycle skipped.",
                "Strategy skipped because no weather markets were available.",
            )
            return parse_decision_payload(payload, strategy_type="algo")

        ranked = await self.rank_opportunities(scoped_markets, now=now)
        best_candidate = ranked[0] if ranked else None
        selected_candidate = None
        actions = []
        evidence = []
        kelly_result = None
        risk_result = None
        min_edge = int(self.strategy_config.get("risk", {}).get("min_edge_bps", 200))

        for candidate in ranked:
            if candidate["best_edge_bps"] < min_edge:
                break
            selected_candidate = candidate
            evidence = self._build_candidate_evidence(candidate, include_rank_summary=True)
            if not self.should_execute_trade():
                logger.info(
                    "Forecast strategy %s generated signal but trade_enabled=false, recording as research-only",
                    self.strategy_id,
                )
                break

            kelly_result = self._size_candidate(candidate)
            if kelly_result["action"] != "trade":
                logger.info("Kelly says no trade for %s: %s", candidate["market"]["market_id"], kelly_result["reason"])
                continue

            risk_result = await self._risk_check(candidate, float(kelly_result["amount_usd"]))
            if not risk_result["approved"]:
                logger.info(
                    "Risk manager rejected %s: %s",
                    candidate["market"]["market_id"],
                    risk_result["reason"],
                )
                continue

            actions = [self._build_action(candidate, float(kelly_result["amount_usd"]))]
            break

        display_candidate = selected_candidate or best_candidate
        if display_candidate and not evidence:
            evidence = self._build_candidate_evidence(display_candidate, include_rank_summary=True)
        if ranked:
            evidence.append(
                {
                    "source": "opportunity_ranking",
                    "content": (
                        f"Ranked {len(ranked)} weather opportunities this cycle; "
                        f"top {min(len(ranked), self._max_opportunities_per_cycle())} retained by liquidity-weighted edge."
                    ),
                }
            )

        payload = {
            "timestamp": now.isoformat(),
            "strategy_id": self.strategy_id,
            "markets_considered": [item["market"]["market_id"] for item in ranked],
            "predicted_probability": display_candidate["best_side_probability"] if display_candidate else None,
            "market_implied_probability": display_candidate["best_side_market_price"] if display_candidate else None,
            "expected_edge_bps": display_candidate["best_edge_bps"] if display_candidate else None,
            "confidence": 0.74 if actions else (0.61 if display_candidate else None),
            "evidence_items": evidence,
            "risk_notes": (
                "Weather edge decays into resolution, and unsupported or illiquid contracts are skipped rather than forced."
            ),
            "exit_plan": "Hold until resolution unless the ensemble or intraday observation path moves materially against the trade.",
            "thinking": (
                "ALGO-1 parses weather market questions, prices the relevant weather outcome from the ensemble, "
                "applies liquidity and time-decay gates, ranks opportunities by edge weighted by volume, "
                "and sizes only the strongest surviving candidate."
            ),
            "web_searches_used": [],
            "actions": actions,
            "no_action_reason": None
            if actions
            else self._build_no_action_reason(ranked, min_edge, kelly_result, risk_result),
        }
        return parse_decision_payload(payload, strategy_type="algo")

    async def rank_opportunities(self, markets: list, now: datetime | None = None) -> list[dict]:
        now = now or datetime.now(timezone.utc)
        opportunities = []
        for row in markets:
            opportunity = await self._evaluate_market(row, now)
            if opportunity is None:
                continue
            opportunities.append(opportunity)
        opportunities.sort(key=lambda item: (item["rank_score"], item["best_edge_bps"]), reverse=True)
        return opportunities[: self._max_opportunities_per_cycle()]

    async def _evaluate_market(self, row, now: datetime) -> dict | None:
        question = str(row["question"])
        params = parse_weather_question(question)
        if params is None:
            logger.info("Skipping unrecognized weather market question: %s", question)
            return None
        if params.metric == "rain":
            logger.info(
                "Skipping rain market %s because the ensemble path does not expose precipitation probability yet",
                row["market_id"],
            )
            return None

        volume_usd = float(row["volume_usd"] or 0.0)
        if volume_usd < self._min_market_volume_usd():
            logger.info(
                "Skipping %s for low volume: %.2f < %.2f",
                row["market_id"],
                volume_usd,
                self._min_market_volume_usd(),
            )
            return None

        end_time = self._parse_end_time(row["end_time"])
        if end_time <= now:
            return None
        if params.date < now.date():
            logger.info("Skipping stale weather market %s: %s", row["market_id"], question)
            return None

        try:
            outcomes = json.loads(row["outcomes_json"])
        except json.JSONDecodeError as exc:
            logger.warning("Skipping weather market %s due to malformed outcomes_json: %s", row["market_id"], exc)
            return None
        yes_outcome, no_outcome = self._binary_outcomes(outcomes)
        if not yes_outcome or not no_outcome:
            logger.info("Skipping weather market %s because it is missing YES/NO outcomes", row["market_id"])
            return None

        contract = params.to_contract_dict()
        ensemble = await self._get_ensemble(params.canonical_city, params.date)
        if ensemble is None:
            logger.info("Skipping weather market %s because ensemble data is unavailable", row["market_id"])
            return None

        observations = None
        used_intraday = False
        predicted_yes = None
        is_same_day = params.date == now.date()
        market_type = "low" if params.metric == "low" else "high"
        forecast_value_c = self._forecast_value_for_metric(contract, ensemble)
        sigma_override = self._sigma_for_metric(params.metric, ensemble)

        if is_same_day:
            observations = await self._get_observations(params.canonical_city)
            if observations is not None:
                predicted_yes = self._compute_intraday_probability(ensemble, observations, contract, market_type=market_type)
                used_intraday = True
                location_key = params.canonical_city.lower()
                if location_key not in self._observations_recorded:
                    self._record_observation(observations)
                    self._observations_recorded.add(location_key)

        if predicted_yes is None:
            if forecast_value_c is None:
                logger.info("Skipping weather market %s because the ensemble lacks %s forecast data", row["market_id"], params.metric)
                return None
            predicted_yes = self._estimate_probability(contract, forecast_value_c, sigma_override=sigma_override)

        allow_intraday_extreme = self._allow_intraday_extreme(contract, observations)
        predicted_yes = self._apply_probability_safety(predicted_yes, allow_intraday_extreme=allow_intraday_extreme)

        yes_buy_price = self._buy_price(yes_outcome)
        no_buy_price = self._buy_price(no_outcome)
        no_probability = 1.0 - predicted_yes
        raw_yes_edge_bps = self._apply_edge_safety(
            int(round((predicted_yes - yes_buy_price) * 10000)),
            row["market_id"],
            predicted_yes,
            yes_buy_price,
        )
        raw_no_edge_bps = self._apply_edge_safety(
            int(round((no_probability - no_buy_price) * 10000)),
            row["market_id"],
            no_probability,
            no_buy_price,
        )

        decay_multiplier = 1.0
        if self._time_decay_enabled():
            decay_multiplier = compute_time_decay_multiplier(end_time, now=now, decay_config=self._time_decay_config())
        yes_edge_bps = int(round(raw_yes_edge_bps * decay_multiplier))
        no_edge_bps = int(round(raw_no_edge_bps * decay_multiplier))
        best_side = "BUY_YES" if yes_edge_bps >= no_edge_bps else "BUY_NO"
        best_edge_bps = max(yes_edge_bps, no_edge_bps)
        best_side_probability = predicted_yes if best_side == "BUY_YES" else no_probability
        best_side_market_price = yes_buy_price if best_side == "BUY_YES" else no_buy_price
        rank_score = best_edge_bps * math.sqrt(max(volume_usd, 1.0))

        return {
            "market": row,
            "params": params,
            "contract": contract,
            "ensemble": ensemble,
            "observations": observations,
            "used_intraday": used_intraday,
            "predicted_yes": predicted_yes,
            "yes_outcome": yes_outcome,
            "no_outcome": no_outcome,
            "yes_buy_price": yes_buy_price,
            "no_buy_price": no_buy_price,
            "raw_yes_edge_bps": raw_yes_edge_bps,
            "raw_no_edge_bps": raw_no_edge_bps,
            "yes_edge_bps": yes_edge_bps,
            "no_edge_bps": no_edge_bps,
            "best_side": best_side,
            "best_edge_bps": best_edge_bps,
            "best_side_probability": best_side_probability,
            "best_side_market_price": best_side_market_price,
            "volume_usd": volume_usd,
            "decay_multiplier": decay_multiplier,
            "rank_score": rank_score,
        }

    async def _get_ensemble(self, city: str, forecast_date: date) -> dict | None:
        normalized_city = normalize_weather_city(city)
        cache_key = (normalized_city.lower(), forecast_date)
        if cache_key in self._ensemble_cache:
            return self._ensemble_cache[cache_key]
        coords = await self._resolve_city_coordinates(normalized_city)
        if not coords:
            logger.warning("No coordinates for city: %s", normalized_city)
            self._ensemble_cache[cache_key] = None
            return None
        lat, lon = coords
        try:
            ensemble = await get_ensemble_forecast(lat, lon, normalized_city, forecast_date.isoformat(), db=self.db)
            logger.info(
                "Ensemble forecast for %s: %.1fC +/-%.1fC from %s sources",
                normalized_city,
                ensemble["ensemble_high_c"],
                ensemble["ensemble_sigma_c"],
                ensemble["sources_used"],
            )
            self._ensemble_cache[cache_key] = ensemble
            return ensemble
        except WeatherDataUnavailable as exc:
            logger.warning("Ensemble unavailable for %s: %s", normalized_city, exc)
            self._ensemble_cache[cache_key] = None
            return None

    async def _resolve_city_coordinates(self, city: str) -> tuple[float, float] | None:
        normalized_city = normalize_weather_city(city)
        cache_key = normalized_city.lower()
        if cache_key in self._coord_cache:
            return self._coord_cache[cache_key]
        direct = CITY_COORDS.get(cache_key)
        if direct:
            self._coord_cache[cache_key] = direct
            return direct
        try:
            coords = await self._geo_source._resolve_coords(normalized_city)
        except Exception as exc:
            logger.warning("Geocoding failed for %s: %s", normalized_city, exc)
            return None
        self._coord_cache[cache_key] = coords
        return coords

    def _binary_outcomes(self, outcomes: list[dict]) -> tuple[dict | None, dict | None]:
        yes = next((item for item in outcomes if str(item.get("label", "")).lower() == "yes"), None)
        no = next((item for item in outcomes if str(item.get("label", "")).lower() == "no"), None)
        if yes and no:
            return yes, no
        if len(outcomes) >= 2:
            return outcomes[0], outcomes[1]
        return None, None

    def _buy_price(self, outcome: dict) -> float:
        best_ask = outcome.get("best_ask")
        if best_ask is not None:
            return float(best_ask)
        mid = outcome.get("mid_price")
        if mid is not None:
            return float(mid)
        return 1.0

    def _parse_weather_contract(self, question: str) -> dict | None:
        params = parse_weather_question(question)
        return params.to_contract_dict() if params else None

    def _estimate_probability(self, contract: dict, forecast_value_c: float, sigma_override: float | None = None) -> float:
        if contract.get("shape") == "rain":
            return 0.5
        mean = forecast_value_c if contract["unit"] == "c" else (forecast_value_c * 9 / 5) + 32
        sigma = (sigma_override or 2.0) if contract["unit"] == "c" else (sigma_override or 2.0) * 1.8
        if contract["shape"] == "between":
            return max(
                0.0,
                min(
                    1.0,
                    self._cdf(contract["upper"] + 0.5, mean, sigma)
                    - self._cdf(contract["lower"] - 0.5, mean, sigma),
                ),
            )
        if contract["shape"] == "at_or_above":
            return max(0.0, min(1.0, 1.0 - self._cdf(contract["threshold"] - 0.5, mean, sigma)))
        if contract["shape"] == "at_or_below":
            return max(0.0, min(1.0, self._cdf(contract["threshold"] + 0.5, mean, sigma)))
        if contract["shape"] == "exact":
            return max(
                0.0,
                min(
                    1.0,
                    self._cdf(contract["threshold"] + 0.5, mean, sigma)
                    - self._cdf(contract["threshold"] - 0.5, mean, sigma),
                ),
            )
        return 0.5

    def _compute_intraday_probability(
        self,
        ensemble_forecast: dict,
        observations: dict,
        contract: dict,
        market_type: str = "high",
    ) -> float:
        threshold_c = self._contract_threshold_c(contract)
        shape = contract.get("shape", "at_or_above")
        current_temp = float(observations["current_temp_c"])
        max_so_far = float(observations["max_temp_so_far_c"])
        min_so_far = float(observations["min_temp_so_far_c"])
        trending = str(observations["trending"])
        hours_remaining = float(observations["hours_remaining"])
        sigma = float(self._sigma_for_metric("low" if market_type == "low" else "high", ensemble_forecast))

        if market_type == "high":
            forecast_high = ensemble_forecast["ensemble_high_c"]
            remaining_warming = forecast_high - current_temp
            if trending == "cooling" and threshold_c is not None and current_temp < threshold_c:
                adjusted_forecast = current_temp + (remaining_warming * 0.3)
            else:
                adjusted_forecast = forecast_high

            if threshold_c is not None and shape == "at_or_above" and max_so_far >= threshold_c:
                return 0.98
            if threshold_c is not None and hours_remaining <= 1.0 and current_temp < threshold_c - 2.0:
                return 0.05
            return self._shape_probability_from_bounds(contract, adjusted_forecast, sigma, observed_extreme=max_so_far)

        forecast_low = ensemble_forecast.get("ensemble_low_c")
        if forecast_low is None:
            return 0.5
        remaining_cooling = current_temp - forecast_low
        if trending == "warming" and threshold_c is not None and current_temp > threshold_c:
            adjusted_forecast = current_temp - (remaining_cooling * 0.3)
        else:
            adjusted_forecast = forecast_low

        if threshold_c is not None and shape == "at_or_below" and min_so_far <= threshold_c:
            return 0.98
        if threshold_c is not None and shape == "at_or_above" and min_so_far < threshold_c:
            return 0.01
        if threshold_c is not None and hours_remaining <= 1.0 and current_temp > threshold_c + 2.0 and shape == "at_or_below":
            return 0.05
        return self._shape_probability_from_bounds(contract, adjusted_forecast, sigma, observed_extreme=min_so_far, low_market=True)

    def _shape_probability_from_bounds(
        self,
        contract: dict,
        mean_c: float,
        sigma_c: float,
        observed_extreme: float | None = None,
        low_market: bool = False,
    ) -> float:
        contract_local = dict(contract)
        if contract_local.get("unit") == "f":
            mean = (mean_c * 9 / 5) + 32
            sigma = sigma_c * 1.8
            observed = (observed_extreme * 9 / 5) + 32 if observed_extreme is not None else None
        else:
            mean = mean_c
            sigma = sigma_c
            observed = observed_extreme

        shape = contract_local.get("shape")
        threshold = contract_local.get("threshold")
        lower = contract_local.get("lower")
        upper = contract_local.get("upper")

        if shape == "exact" and threshold is not None:
            if observed is not None:
                if not low_market and observed > threshold + 0.5:
                    return 0.01
                if low_market and observed < threshold - 0.5:
                    return 0.01
            return max(0.0, min(1.0, self._cdf(threshold + 0.5, mean, sigma) - self._cdf(threshold - 0.5, mean, sigma)))

        if shape == "between" and lower is not None and upper is not None:
            if observed is not None:
                if not low_market and observed > upper + 0.5:
                    return 0.01
                if low_market and observed < lower - 0.5:
                    return 0.01
            return max(0.0, min(1.0, self._cdf(upper + 0.5, mean, sigma) - self._cdf(lower - 0.5, mean, sigma)))

        if shape == "at_or_above" and threshold is not None:
            return max(0.0, min(1.0, 1.0 - self._cdf(threshold - 0.5, mean, sigma)))
        if shape == "at_or_below" and threshold is not None:
            return max(0.0, min(1.0, self._cdf(threshold + 0.5, mean, sigma)))
        return 0.5

    def _apply_probability_safety(self, probability: float, allow_intraday_extreme: bool = False) -> float:
        upper = 0.98 if allow_intraday_extreme else 0.95
        return max(0.05, min(upper, probability))

    def _apply_edge_safety(self, edge_bps: int, market_id: str, probability: float, market_price: float) -> int:
        if edge_bps > 5000:
            logger.error(
                "SUSPICIOUS edge %s bps for market %s (probability %.4f vs price %.4f)",
                edge_bps,
                market_id,
                probability,
                market_price,
            )
        if edge_bps > 3000:
            logger.warning("Edge estimate %s bps exceeds safety cap for %s; clamping to 3000", edge_bps, market_id)
            return 3000
        return edge_bps

    async def _get_observations(self, city: str) -> dict | None:
        normalized_city = normalize_weather_city(city)
        cache_key = normalized_city.lower()
        if cache_key in self._observation_cache:
            return self._observation_cache[cache_key]
        coords = await self._resolve_city_coordinates(normalized_city)
        if not coords:
            self._observation_cache[cache_key] = None
            return None
        lat, lon = coords
        try:
            observations = await get_current_observations(lat, lon, normalized_city)
            self._observation_cache[cache_key] = observations
            return observations
        except ObservationUnavailable as exc:
            logger.warning("Observations unavailable for %s: %s", normalized_city, exc)
            self._observation_cache[cache_key] = None
            return None

    def _contract_threshold_c(self, contract: dict) -> float | None:
        threshold = contract.get("threshold")
        if threshold is None:
            lower = contract.get("lower")
            upper = contract.get("upper")
            if lower is not None and upper is not None:
                threshold = (lower + upper) / 2.0
            else:
                return None
        if contract["unit"] == "f":
            return (threshold - 32) * 5 / 9
        return threshold

    def _record_observation(self, observations: dict) -> None:
        try:
            with self.db.connect() as conn:
                conn.execute(
                    "INSERT INTO station_observations (location, source, observation_time, temperature_c, trending) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        str(observations["location"]).lower(),
                        ",".join(observations["source_names"]),
                        observations["observation_time"],
                        observations["current_temp_c"],
                        observations["trending"],
                    ),
                )
        except Exception as exc:
            logger.warning("Failed to record observation: %s", exc)

    def _build_no_action_reason(self, candidates, min_edge, kelly_result, risk_result) -> str:
        if not candidates:
            return "No supported weather markets passed parsing, ensemble, liquidity, and timing gates."
        if candidates[0]["best_edge_bps"] < min_edge:
            return f"Best decayed edge {candidates[0]['best_edge_bps']} bps below minimum {min_edge} bps."
        if not self.should_execute_trade():
            return "Trade execution disabled for this strategy; signal recorded as research-only."
        if kelly_result and kelly_result["action"] != "trade":
            return f"Kelly sizing rejected: {kelly_result['reason']}"
        if risk_result and not risk_result["approved"]:
            return f"Risk check rejected: {risk_result['reason']}"
        return "No ranked weather opportunity survived the post-ranking trade gates."

    def _cdf(self, x: float, mean: float, sigma: float) -> float:
        if sigma <= 0:
            return 1.0 if x >= mean else 0.0
        z = (x - mean) / (sigma * math.sqrt(2))
        return 0.5 * (1 + math.erf(z))

    def _log_pending_adjustments(self) -> None:
        try:
            with self.db.connect() as conn:
                pending = list(
                    conn.execute(
                        "SELECT parameter_name, current_value, recommended_value, reason "
                        "FROM parameter_adjustments "
                        "WHERE strategy_id = ? AND auto_applied = 0 "
                        "ORDER BY created_at DESC",
                        (self.strategy_id,),
                    )
                )
        except Exception:
            return
        for adjustment in pending:
            logger.warning(
                "Pending calibration adjustment: %s %s -> %s (%s)",
                adjustment["parameter_name"],
                adjustment["current_value"],
                adjustment["recommended_value"],
                adjustment["reason"],
            )

    def _has_time_remaining(self, row, now: datetime) -> bool:
        end_time = self._parse_end_time(row["end_time"])
        min_time_remaining = float(self.strategy_config.get("scope", {}).get("min_time_remaining_hours", 0.0) or 0.0)
        return ((end_time - now).total_seconds() / 3600.0) >= min_time_remaining

    def _parse_end_time(self, value: str) -> datetime:
        end_time = datetime.fromisoformat(str(value))
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        return end_time

    def _min_market_volume_usd(self) -> float:
        return float(self.strategy_config.get("min_market_volume_usd", 500.0) or 500.0)

    def _max_opportunities_per_cycle(self) -> int:
        return int(self.strategy_config.get("max_opportunities_per_cycle", 5) or 5)

    def _time_decay_enabled(self) -> bool:
        return bool(self.strategy_config.get("time_decay_enabled", True))

    def _time_decay_config(self) -> dict[str, float]:
        raw = self.strategy_config.get("time_decay", {}) or {}
        return {
            key: float(raw.get(key, default))
            for key, default in DEFAULT_TIME_DECAY.items()
        }

    def _forecast_value_for_metric(self, contract: dict, ensemble: dict) -> float | None:
        if contract.get("metric") == "low":
            low_value = ensemble.get("ensemble_low_c")
            return float(low_value) if low_value is not None else None
        high_value = ensemble.get("ensemble_high_c")
        return float(high_value) if high_value is not None else None

    def _sigma_for_metric(self, metric: str | None, ensemble: dict) -> float:
        if str(metric or "high").lower() == "low":
            low_sigma = ensemble.get("ensemble_sigma_low_c")
            if low_sigma is not None:
                return float(low_sigma)
        high_sigma = ensemble.get("ensemble_sigma_high_c")
        if high_sigma is not None:
            return float(high_sigma)
        return float(ensemble.get("ensemble_sigma_c", 2.0) or 2.0)

    def _allow_intraday_extreme(self, contract: dict, observations: dict | None) -> bool:
        if not observations:
            return False
        threshold_c = self._contract_threshold_c(contract)
        if threshold_c is None:
            return False
        if contract.get("metric") == "low":
            return contract.get("shape") == "at_or_below" and float(observations["min_temp_so_far_c"]) <= threshold_c
        return contract.get("shape") == "at_or_above" and float(observations["max_temp_so_far_c"]) >= threshold_c

    def _size_candidate(self, candidate: dict) -> dict:
        from arena.risk.kelly import compute_position_size

        portfolio = self.db.get_portfolio(self.strategy_id)
        bankroll = portfolio.cash if portfolio else float(self.strategy_config.get("starting_balance", 1000.0))
        sizing_cfg = self.strategy_config.get("position_sizing", {})
        return compute_position_size(
            predicted_probability=float(candidate["best_side_probability"]),
            market_ask_price=float(candidate["best_side_market_price"]),
            bankroll=bankroll,
            kelly_fraction=float(sizing_cfg.get("kelly_fraction", 0.25)),
            max_position_pct=float(
                sizing_cfg.get("max_position_pct", self.strategy_config.get("risk", {}).get("max_position_pct", 0.15))
            ),
            min_position_usd=float(sizing_cfg.get("min_position_usd", 1.0)),
            max_position_usd=float(sizing_cfg.get("max_position_usd", 25.0)),
            fee_rate=float(sizing_cfg.get("fee_rate", 0.02)),
            yes_side_probability=float(candidate["predicted_yes"]),
        )

    async def _risk_check(self, candidate: dict, amount_usd: float) -> dict:
        from arena.risk.risk_manager import RiskManager

        risk_cfg = self.strategy_config.get("risk_management", self.strategy_config.get("risk", {}))
        risk_mgr = RiskManager(self.db, risk_cfg)
        return await risk_mgr.check_trade(
            self.strategy_id,
            candidate["market"]["market_id"],
            amount_usd,
            "BUY",
            venue=candidate["market"]["venue"],
        )

    def _build_action(self, candidate: dict, amount_usd: float) -> dict:
        outcome = candidate["yes_outcome"] if candidate["best_side"] == "BUY_YES" else candidate["no_outcome"]
        return {
            "action_type": "BUY",
            "market_id": candidate["market"]["market_id"],
            "venue": candidate["market"]["venue"],
            "outcome_id": outcome["outcome_id"],
            "outcome_label": outcome["label"],
            "amount_usd": amount_usd,
            "limit_price": outcome.get("best_ask"),
            "reasoning_summary": (
                f"Weather ensemble edge {candidate['best_edge_bps']} bps after time decay "
                f"({candidate['decay_multiplier']:.2f}x) and liquidity weighting."
            ),
        }

    def _build_candidate_evidence(self, candidate: dict, include_rank_summary: bool = False) -> list[dict]:
        params = candidate["params"]
        ensemble = candidate["ensemble"]
        evidence = [
            {
                "source": "forecast_ensemble",
                "content": (
                    f"{params.canonical_city} {params.metric} ensemble: "
                    f"high={ensemble.get('ensemble_high_c')}C low={ensemble.get('ensemble_low_c')}C "
                    f"high_sigma={ensemble.get('ensemble_sigma_high_c', ensemble.get('ensemble_sigma_c'))}C "
                    f"low_sigma={ensemble.get('ensemble_sigma_low_c', ensemble.get('ensemble_sigma_c'))}C "
                    f"from {ensemble.get('sources_used')} sources."
                ),
            },
            {
                "source": "market_pricing",
                "content": (
                    f"YES ask={candidate['yes_buy_price']:.3f}, NO ask={candidate['no_buy_price']:.3f}, "
                    f"predicted YES={candidate['predicted_yes']:.3f}, decayed edge={candidate['best_edge_bps']} bps, "
                    f"volume=${candidate['volume_usd']:.0f}."
                ),
            },
            {
                "source": "time_decay",
                "content": (
                    f"Resolution-aware edge multiplier for {candidate['market']['market_id']} = "
                    f"{candidate['decay_multiplier']:.2f}x."
                ),
            },
        ]
        observations = candidate.get("observations")
        if candidate.get("used_intraday") and observations:
            evidence.append(
                {
                    "source": "station_observations",
                    "content": (
                        f"Intraday path: current={observations['current_temp_c']}C, "
                        f"max={observations['max_temp_so_far_c']}C, min={observations['min_temp_so_far_c']}C, "
                        f"trend={observations['trending']}."
                    ),
                }
            )
        if include_rank_summary:
            evidence.append(
                {
                    "source": "opportunity_score",
                    "content": f"Rank score=edge_bps*sqrt(volume)={candidate['rank_score']:.2f}.",
                }
            )
        return evidence

    def _build_skip_payload(self, now: datetime, no_action_reason: str, risk_notes: str) -> dict:
        return {
            "timestamp": now.isoformat(),
            "strategy_id": self.strategy_id,
            "markets_considered": [],
            "predicted_probability": None,
            "market_implied_probability": None,
            "expected_edge_bps": None,
            "confidence": None,
            "evidence_items": [],
            "risk_notes": risk_notes,
            "exit_plan": "No positions opened.",
            "thinking": "ALGO-1 skipped its cycle because no eligible weather markets were available.",
            "web_searches_used": [],
            "actions": [],
            "no_action_reason": no_action_reason,
        }
