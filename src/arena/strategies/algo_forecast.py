from __future__ import annotations

import logging
from datetime import date, datetime, timezone
import json
import math
import re

from arena.adapters.weather_openmeteo import CITY_COORDS
from arena.data_sources.station_observations import ObservationUnavailable, get_current_observations
from arena.data_sources.weather_ensemble import WeatherDataUnavailable, get_ensemble_forecast
from arena.intelligence.output_parser import parse_decision_payload
from arena.strategies.base import Strategy

logger = logging.getLogger(__name__)


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
        self._observations_recorded: set[str] = set()

    async def generate_decision(self):
        # Log any pending calibration adjustments
        # To enable auto-calibration:
        # 1. Query parameter_adjustments WHERE auto_applied = FALSE
        # 2. Apply recommended_value to self.strategy_config[parameter_name]
        # 3. UPDATE parameter_adjustments SET auto_applied = TRUE WHERE id = ?
        # 4. Only enable when sample_size >= 50 and rolling_brier < 0.20
        try:
            with self.db.connect() as conn:
                pending = list(conn.execute(
                    "SELECT parameter_name, current_value, recommended_value, reason "
                    "FROM parameter_adjustments "
                    "WHERE strategy_id = ? AND auto_applied = 0 "
                    "ORDER BY created_at DESC",
                    (self.strategy_id,),
                ))
            for adj in pending:
                logger.warning(
                    f"Pending calibration adjustment: {adj['parameter_name']} "
                    f"{adj['current_value']} -> {adj['recommended_value']} ({adj['reason']})"
                )
        except Exception:
            pass  # Table may not exist yet

        now = datetime.now(timezone.utc)
        scope = self.strategy_config.get("scope", {})
        min_volume = float(scope.get("min_volume_usd", 0.0) or 0.0)
        min_time_remaining = float(scope.get("min_time_remaining_hours", 0.0) or 0.0)
        scoped_markets = []
        for row in self.db.list_markets(category="weather", status="active"):
            if not self.is_market_eligible(row):
                continue
            if float(row["volume_usd"]) < min_volume:
                continue
            end_time = datetime.fromisoformat(row["end_time"])
            if (end_time - now).total_seconds() / 3600 < min_time_remaining:
                continue
            scoped_markets.append(row)
        if not scoped_markets and scope.get("skip_if_no_markets"):
            payload = {
                "timestamp": now.isoformat(),
                "strategy_id": self.strategy_id,
                "markets_considered": [],
                "predicted_probability": None,
                "market_implied_probability": None,
                "expected_edge_bps": None,
                "confidence": None,
                "evidence_items": [],
                "risk_notes": "Strategy skipped because no weather markets were available.",
                "exit_plan": "No positions opened.",
                "thinking": "ALGO-1 skipped its cycle because the scanner found no active weather markets and skip_if_no_markets is enabled.",
                "web_searches_used": [],
                "actions": [],
                "no_action_reason": "No active weather markets available; cycle skipped.",
            }
            return parse_decision_payload(payload, strategy_type="algo")

        candidates = []
        considered = []
        for row in scoped_markets:
            contract = self._parse_weather_contract(row["question"])
            if not contract:
                continue
            # Skip markets that already resolved (past dates), but allow same-day markets
            if contract.get("dated", True) and contract["forecast_date"] < now.date():
                continue
            is_same_day = contract.get("dated", True) and contract["forecast_date"] == now.date()
            yes_outcome, no_outcome = self._binary_outcomes(json.loads(row["outcomes_json"]))
            if not yes_outcome or not no_outcome:
                continue
            yes_buy_price = self._buy_price(yes_outcome)
            no_buy_price = self._buy_price(no_outcome)
            ensemble = (
                await self._get_ensemble(contract["city"], contract["forecast_date"])
                if contract.get("dated", True)
                else None
            )
            forecast_high = ensemble["ensemble_high_c"] if ensemble else None
            ensemble_sigma = ensemble["ensemble_sigma_c"] if ensemble else None

            # Intraday path: if market resolves today, use real-time observations
            used_intraday = False
            observations = None
            if is_same_day and ensemble is not None:
                observations = await self._get_observations(contract["city"])
                if observations is not None:
                    threshold_c = self._contract_threshold_c(contract)
                    if threshold_c is not None:
                        predicted_yes = self._compute_intraday_probability(
                            ensemble, observations, contract, "high"
                        )
                        used_intraday = True
                        logger.info(
                            f"Using intraday observations for {contract['city']} "
                            f"(current: {observations['current_temp_c']}C, trending: {observations['trending']})"
                        )
                        # Record observation to DB (once per city per cycle)
                        loc_key = contract["city"].lower()
                        if loc_key not in self._observations_recorded:
                            self._record_observation(observations)
                            self._observations_recorded.add(loc_key)

            if not used_intraday:
                if forecast_high is None:
                    yes_implied = yes_buy_price
                    predicted_yes = min(yes_implied + 0.10, 0.95)
                else:
                    predicted_yes = self._estimate_probability(contract, forecast_high, sigma_override=ensemble_sigma)
                    if is_same_day:
                        logger.info(
                            f"Using ensemble forecast for {contract['city']} "
                            f"(resolves today but no observations available)"
                        )
                    else:
                        days_out = (contract["forecast_date"] - now.date()).days
                        logger.info(f"Using ensemble forecast for {contract['city']} (resolves in {days_out} days)")

            allow_intraday_extreme = False
            if used_intraday and observations is not None:
                threshold_c = self._contract_threshold_c(contract)
                allow_intraday_extreme = (
                    contract.get("shape") == "at_or_above"
                    and threshold_c is not None
                    and observations["max_temp_so_far_c"] >= threshold_c
                )
            predicted_yes = self._apply_probability_safety(predicted_yes, allow_intraday_extreme=allow_intraday_extreme)

            yes_implied = yes_buy_price
            no_implied = no_buy_price
            no_probability = 1.0 - predicted_yes
            yes_edge_bps = self._apply_edge_safety(
                int((predicted_yes - yes_implied) * 10000),
                row["market_id"],
                predicted_yes,
                yes_implied,
            )
            no_edge_bps = self._apply_edge_safety(
                int((no_probability - no_implied) * 10000),
                row["market_id"],
                no_probability,
                no_implied,
            )
            best_side = "BUY_YES" if yes_edge_bps >= no_edge_bps else "BUY_NO"
            edge_bps = max(yes_edge_bps, no_edge_bps)
            considered.append(row["market_id"])
            candidates.append(
                {
                    "market": row,
                    "contract": contract,
                    "forecast_high_c": forecast_high,
                    "ensemble": ensemble,
                    "observations": observations,
                    "used_intraday": used_intraday,
                    "predicted_yes": predicted_yes,
                    "yes_outcome": yes_outcome,
                    "no_outcome": no_outcome,
                    "yes_buy_price": yes_buy_price,
                    "no_buy_price": no_buy_price,
                    "yes_edge_bps": yes_edge_bps,
                    "no_edge_bps": no_edge_bps,
                    "best_side": best_side,
                    "best_edge_bps": edge_bps,
                }
            )

        candidates.sort(key=lambda item: item["best_edge_bps"], reverse=True)
        actions = []
        evidence = []
        kelly_result = None
        risk_result = None
        min_edge = int(self.strategy_config.get("risk", {}).get("min_edge_bps", 200))
        if candidates and candidates[0]["best_edge_bps"] >= min_edge:
            top = candidates[0]
            outcome = top["yes_outcome"] if top["best_side"] == "BUY_YES" else top["no_outcome"]
            action_side = "BUY"
            predicted_prob = top["predicted_yes"] if top["best_side"] == "BUY_YES" else (1.0 - top["predicted_yes"])
            market_price = top["yes_buy_price"] if top["best_side"] == "BUY_YES" else top["no_buy_price"]

            # Kelly position sizing
            from arena.risk.kelly import compute_position_size
            portfolio = self.db.get_portfolio(self.strategy_id)
            bankroll = portfolio.cash if portfolio else float(self.strategy_config.get("starting_balance", 1000.0))
            sizing_cfg = self.strategy_config.get("position_sizing", {})
            kelly_result = compute_position_size(
                predicted_probability=predicted_prob,
                market_ask_price=market_price,
                bankroll=bankroll,
                kelly_fraction=float(sizing_cfg.get("kelly_fraction", 0.25)),
                max_position_pct=float(sizing_cfg.get("max_position_pct", self.strategy_config["risk"]["max_position_pct"])),
                min_position_usd=float(sizing_cfg.get("min_position_usd", 1.0)),
                max_position_usd=float(sizing_cfg.get("max_position_usd", 25.0)),
                fee_rate=float(sizing_cfg.get("fee_rate", 0.02)),
                yes_side_probability=top["predicted_yes"],
            )

            if kelly_result["action"] != "trade":
                logger.info(f"Kelly says no trade: {kelly_result['reason']}")
            else:
                amount = kelly_result["amount_usd"]

                # Risk manager check
                from arena.risk.risk_manager import RiskManager
                risk_cfg = self.strategy_config.get("risk_management", self.strategy_config.get("risk", {}))
                risk_mgr = RiskManager(self.db, risk_cfg)
                risk_result = await risk_mgr.check_trade(
                    self.strategy_id,
                    top["market"]["market_id"],
                    amount,
                    action_side,
                    venue=top["market"]["venue"],
                )
                logger.info(f"Risk check: {risk_result}")

                if not risk_result["approved"]:
                    logger.info(f"Risk manager rejected: {risk_result['reason']}")
                else:
                    actions.append(
                        {
                            "action_type": action_side,
                            "market_id": top["market"]["market_id"],
                            "venue": top["market"]["venue"],
                            "outcome_id": outcome["outcome_id"],
                            "outcome_label": outcome["label"],
                            "amount_usd": amount,
                            "limit_price": outcome.get("best_ask"),
                            "reasoning_summary": f"Forecast-consensus edge of {top['best_edge_bps']} bps vs market (Kelly: {kelly_result['kelly_bet_fraction']:.3f}).",
                        }
                    )
            ens = top.get("ensemble")
            if ens:
                forecast_detail = (
                    f"{top['contract']['city']} ensemble forecast: {ens['ensemble_high_c']:.1f}C "
                    f"\u00b1{ens['ensemble_sigma_c']:.1f}C from {ens['sources_used']} sources "
                    f"({', '.join(ens['source_names'])}), bias correction {ens['bias_correction_applied_c']:+.1f}C"
                )
            elif top["forecast_high_c"] is not None:
                forecast_detail = f"{top['contract']['city']} forecast high={top['forecast_high_c']:.2f}C for {top['contract']['forecast_date']}"
            else:
                forecast_detail = f"{top['contract']['city']} used fallback heuristic — live forecast data unavailable"
            evidence.append({"source": "forecast_ensemble", "content": forecast_detail})
            # Add intraday observation evidence if used
            obs = top.get("observations")
            if top.get("used_intraday") and obs:
                obs_detail = (
                    f"INTRADAY: {top['contract']['city']} current {obs['current_temp_c']}C, "
                    f"high so far {obs['max_temp_so_far_c']}C, trending {obs['trending']}, "
                    f"{obs['hours_remaining']}h daylight remaining, "
                    f"{obs['sources_used']} observation sources ({', '.join(obs['source_names'])})"
                )
                evidence.append({"source": "station_observations", "content": obs_detail})
            evidence.append(
                {
                    "source": "market_data",
                    "content": (
                        f"Predicted YES={top['predicted_yes']:.3f}, buy YES ask={top['yes_buy_price']:.3f}, "
                        f"buy NO ask={top['no_buy_price']:.3f}"
                    ),
                },
            )
        best_candidate = candidates[0] if candidates else None
        best_side_probability = None
        best_side_market_price = None
        if best_candidate:
            if best_candidate["best_side"] == "BUY_YES":
                best_side_probability = best_candidate["predicted_yes"]
                best_side_market_price = best_candidate["yes_buy_price"]
            else:
                best_side_probability = 1.0 - best_candidate["predicted_yes"]
                best_side_market_price = best_candidate["no_buy_price"]

        payload = {
            "timestamp": now.isoformat(),
            "strategy_id": self.strategy_id,
            "markets_considered": considered[:15],
            "predicted_probability": best_side_probability,
            "market_implied_probability": best_side_market_price,
            "expected_edge_bps": best_candidate["best_edge_bps"] if best_candidate else None,
            "confidence": 0.68 if actions else None,
            "evidence_items": evidence,
            "risk_notes": "Weather forecast errors and local microclimate effects can erase the modeled edge.",
            "exit_plan": "Hold until resolution unless the forecast consensus moves materially against the position.",
            "thinking": "ALGO-1 parses weather thresholds, estimates outcome probability from forecast highs, and trades the side with the largest forecast-versus-market edge.",
            "web_searches_used": [],
            "actions": actions,
            "no_action_reason": None if actions else self._build_no_action_reason(candidates, min_edge, kelly_result, risk_result),
        }
        return parse_decision_payload(payload, strategy_type="algo")

    async def _get_ensemble(self, city: str, forecast_date: date) -> dict | None:
        cache_key = (city.lower(), forecast_date)
        if cache_key in self._ensemble_cache:
            return self._ensemble_cache[cache_key]
        coords = CITY_COORDS.get(city.lower())
        if not coords:
            logger.warning(f"No coordinates for city: {city}")
            self._ensemble_cache[cache_key] = None
            return None
        lat, lon = coords
        try:
            ensemble = await get_ensemble_forecast(lat, lon, city, forecast_date.isoformat(), db=self.db)
            logger.info(
                f"Ensemble forecast for {city}: {ensemble['ensemble_high_c']:.1f}C "
                f"\u00b1{ensemble['ensemble_sigma_c']:.1f}C from {ensemble['sources_used']} sources "
                f"using {ensemble.get('ensemble_method', 'simple_mean')} "
                f"(sigma mult: {ensemble.get('sigma_multiplier_used', 1.0):.2f}, "
                f"bias corrections: {ensemble.get('bias_corrections_applied', {})})"
            )
            self._ensemble_cache[cache_key] = ensemble
            return ensemble
        except WeatherDataUnavailable as e:
            logger.warning(f"Ensemble unavailable for {city}: {e}")
            self._ensemble_cache[cache_key] = None
            return None

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
        city_match = re.search(r"in ([A-Za-z .'-]+?) be", question)
        simple_hit = re.search(r"Will ([A-Za-z .'-]+?) hit (\d+)\s*([CF])\??", question)
        if city_match:
            city = city_match.group(1).strip()
        elif simple_hit:
            city = simple_hit.group(1).strip()
        else:
            return None
        date_match = re.search(r"on ([A-Z][a-z]+ \d{1,2})(?:,? (\d{4}))?", question)
        if date_match:
            month_day, year_text = date_match.groups()
            year = int(year_text) if year_text else datetime.now(timezone.utc).year
            forecast_date = datetime.strptime(f"{month_day} {year}", "%B %d %Y").date()
            dated = True
        else:
            forecast_date = datetime.now(timezone.utc).date()
            dated = False
        unit = "f" if "°F" in question else "c"
        if simple_hit:
            threshold = float(simple_hit.group(2))
            unit = simple_hit.group(3).lower()
            return {"city": city, "forecast_date": forecast_date, "unit": unit, "shape": "at_or_above", "threshold": threshold, "dated": dated}
        if between := re.search(r"between (\d+)-(\d+)°[CF]", question):
            lower, upper = float(between.group(1)), float(between.group(2))
            return {"city": city, "forecast_date": forecast_date, "unit": unit, "shape": "between", "lower": lower, "upper": upper, "dated": dated}
        if higher := re.search(r"(\d+)°[CF] or higher", question):
            threshold = float(higher.group(1))
            return {"city": city, "forecast_date": forecast_date, "unit": unit, "shape": "at_or_above", "threshold": threshold, "dated": dated}
        if lower := re.search(r"(\d+)°[CF] or below", question):
            threshold = float(lower.group(1))
            return {"city": city, "forecast_date": forecast_date, "unit": unit, "shape": "at_or_below", "threshold": threshold, "dated": dated}
        if exact := re.search(r"be (\d+)°[CF] on", question):
            threshold = float(exact.group(1))
            return {"city": city, "forecast_date": forecast_date, "unit": unit, "shape": "exact", "threshold": threshold, "dated": dated}
        return None

    def _estimate_probability(self, contract: dict, forecast_high_c: float, sigma_override: float | None = None) -> float:
        mean = forecast_high_c if contract["unit"] == "c" else (forecast_high_c * 9 / 5) + 32
        if sigma_override is not None:
            sigma = sigma_override if contract["unit"] == "c" else sigma_override * 1.8
        else:
            sigma = 2.0 if contract["unit"] == "c" else 3.5
        if contract["shape"] == "between":
            return max(0.0, min(1.0, self._cdf(contract["upper"] + 0.5, mean, sigma) - self._cdf(contract["lower"] - 0.5, mean, sigma)))
        if contract["shape"] == "at_or_above":
            return max(0.0, min(1.0, 1.0 - self._cdf(contract["threshold"] - 0.5, mean, sigma)))
        if contract["shape"] == "at_or_below":
            return max(0.0, min(1.0, self._cdf(contract["threshold"] + 0.5, mean, sigma)))
        if contract["shape"] == "exact":
            return max(0.0, min(1.0, self._cdf(contract["threshold"] + 0.5, mean, sigma) - self._cdf(contract["threshold"] - 0.5, mean, sigma)))
        return 0.5

    def _compute_intraday_probability(
        self,
        ensemble_forecast: dict,
        observations: dict,
        contract: dict,
        market_type: str = "high",
    ) -> float:
        """Compute probability using real-time observations blended with ensemble forecast.

        For "high" markets: will the high temperature exceed threshold_c?
        For "low" markets: will the low temperature go below threshold_c?
        """
        threshold_c = self._contract_threshold_c(contract)
        if threshold_c is None:
            return 0.5
        shape = contract.get("shape", "at_or_above")
        current_temp = observations["current_temp_c"]
        max_so_far = observations["max_temp_so_far_c"]
        min_so_far = observations["min_temp_so_far_c"]
        trending = observations["trending"]
        hours_remaining = observations["hours_remaining"]

        if market_type == "high":
            if shape == "at_or_above" and max_so_far >= threshold_c:
                return 0.98

            # Very unlikely to jump 2+ degrees in the last hour
            if hours_remaining <= 1.0 and current_temp < threshold_c - 2.0:
                return 0.05

            # Blend ensemble forecast with observations
            forecast_high = ensemble_forecast["ensemble_high_c"]
            sigma = ensemble_forecast["ensemble_sigma_c"]
            remaining_warming = forecast_high - current_temp

            if trending == "cooling" and current_temp < threshold_c:
                # May have already peaked below threshold
                adjusted_forecast = current_temp + (remaining_warming * 0.3)
                # sigma stays as-is (more uncertainty when cooling)
            elif trending == "warming":
                # Still warming — tighten sigma (real-time confirms trend)
                adjusted_forecast = forecast_high
            else:
                # Stable — use ensemble as-is
                adjusted_forecast = forecast_high

            if shape == "exact":
                upper_bound = threshold_c + 0.5
                lower_bound = threshold_c - 0.5
                if max_so_far > upper_bound:
                    return 0.01
                if hours_remaining <= 1.0 and current_temp < lower_bound - 2.0:
                    return 0.01
                return max(
                    0.0,
                    min(
                        1.0,
                        self._cdf(upper_bound, adjusted_forecast, sigma)
                        - self._cdf(lower_bound, adjusted_forecast, sigma),
                    ),
                )

            if shape == "between":
                lower = contract.get("lower")
                upper = contract.get("upper")
                if lower is None or upper is None:
                    return 0.5
                if contract["unit"] == "f":
                    lower = (lower - 32) * 5 / 9
                    upper = (upper - 32) * 5 / 9
                lower_bound = lower - 0.5
                upper_bound = upper + 0.5
                if max_so_far > upper_bound:
                    return 0.01
                return max(
                    0.0,
                    min(
                        1.0,
                        self._cdf(upper_bound, adjusted_forecast, sigma)
                        - self._cdf(lower_bound, adjusted_forecast, sigma),
                    ),
                )

            return max(0.0, min(1.0, 1.0 - self._cdf(threshold_c - 0.5, adjusted_forecast, sigma)))

        else:  # "low" market: will the low go below threshold_c?
            if min_so_far <= threshold_c:
                return 0.98

            if hours_remaining <= 1.0 and current_temp > threshold_c + 2.0:
                return 0.05

            forecast_low = ensemble_forecast.get("ensemble_low_c")
            if forecast_low is None:
                return 0.5
            sigma = ensemble_forecast["ensemble_sigma_c"]
            remaining_cooling = current_temp - forecast_low

            if trending == "warming" and current_temp > threshold_c:
                adjusted_forecast = current_temp - (remaining_cooling * 0.3)
            elif trending == "cooling":
                adjusted_forecast = forecast_low
            else:
                adjusted_forecast = forecast_low

            return max(0.0, min(1.0, self._cdf(threshold_c + 0.5, adjusted_forecast, sigma)))

    def _apply_probability_safety(self, probability: float, allow_intraday_extreme: bool = False) -> float:
        upper = 0.98 if allow_intraday_extreme else 0.95
        return max(0.05, min(upper, probability))

    def _apply_edge_safety(self, edge_bps: int, market_id: str, probability: float, market_price: float) -> int:
        if edge_bps > 5000:
            logger.error(
                f"SUSPICIOUS: edge {edge_bps} bps for market {market_id} — "
                f"probability {probability:.4f}, market_price {market_price:.4f} — likely calculation error"
            )
        if edge_bps > 3000:
            logger.warning(f"Edge estimate {edge_bps} bps exceeds safety cap — clamping to 3000")
            return 3000
        return edge_bps

    async def _get_observations(self, city: str) -> dict | None:
        """Fetch real-time observations for a city, with caching."""
        cache_key = city.lower()
        if cache_key in self._observation_cache:
            return self._observation_cache[cache_key]
        coords = CITY_COORDS.get(cache_key)
        if not coords:
            self._observation_cache[cache_key] = None
            return None
        lat, lon = coords
        try:
            obs = await get_current_observations(lat, lon, city)
            self._observation_cache[cache_key] = obs
            return obs
        except ObservationUnavailable as e:
            logger.warning(f"Observations unavailable for {city}: {e}")
            self._observation_cache[cache_key] = None
            return None

    def _contract_threshold_c(self, contract: dict) -> float | None:
        """Extract the threshold in Celsius from a parsed contract."""
        threshold = contract.get("threshold")
        if threshold is None:
            # For between contracts, use the midpoint as a rough threshold
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
        """Persist observation to DB for historical analysis."""
        try:
            with self.db.connect() as conn:
                conn.execute(
                    "INSERT INTO station_observations (location, source, observation_time, temperature_c, trending) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        observations["location"].lower(),
                        ",".join(observations["source_names"]),
                        observations["observation_time"],
                        observations["current_temp_c"],
                        observations["trending"],
                    ),
                )
        except Exception as e:
            logger.warning(f"Failed to record observation: {e}")

    def _build_no_action_reason(self, candidates, min_edge, kelly_result, risk_result) -> str:
        if not candidates:
            return "No weather market exceeded the minimum forecast edge threshold."
        if candidates[0]["best_edge_bps"] < min_edge:
            return f"Best edge {candidates[0]['best_edge_bps']} bps below minimum {min_edge} bps."
        if kelly_result and kelly_result["action"] != "trade":
            return f"Kelly sizing rejected: {kelly_result['reason']}"
        if risk_result and not risk_result["approved"]:
            return f"Risk check rejected: {risk_result['reason']}"
        return "No weather market exceeded the minimum forecast edge threshold."

    def _cdf(self, x: float, mean: float, sigma: float) -> float:
        if sigma <= 0:
            return 1.0 if x >= mean else 0.0
        z = (x - mean) / (sigma * math.sqrt(2))
        return 0.5 * (1 + math.erf(z))
