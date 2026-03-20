from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timezone

from arena.intelligence.output_parser import parse_decision_payload
from arena.strategies.algo_forecast import DEFAULT_TIME_DECAY, compute_time_decay_multiplier, parse_weather_question
from arena.strategies.base import Strategy

logger = logging.getLogger(__name__)


class PartitionArbitrageStrategy(Strategy):
    def __init__(self, db, strategy_config: dict) -> None:
        super().__init__(db, strategy_config)
        self.supported_formats = (
            strategy_config.get("scope", {}).get("supported_formats")
            or strategy_config.get("supported_formats")
            or ["numeric_bracket"]
        )

    async def generate_decision(self):
        now = datetime.now(timezone.utc)
        evidence = []
        skip_reasons = []
        grouped_markets = defaultdict(list)

        for row in self.db.list_markets(category="weather", status="active"):
            if not self.is_market_eligible(row):
                continue
            if float(row["volume_usd"] or 0.0) < self._min_market_volume_usd():
                skip_reasons.append(f"{row['market_id']}: volume below ${self._min_market_volume_usd():.0f}")
                continue
            end_time = self._parse_end_time(row["end_time"])
            if end_time <= now:
                continue
            params = parse_weather_question(str(row["question"]))
            if params is None or params.direction != "between":
                skip_reasons.append(f"{row['market_id']}: unsupported bracket question")
                continue
            group_key = (
                params.canonical_city.lower(),
                params.metric,
                params.date.isoformat(),
                params.unit,
            )
            grouped_markets[group_key].append((row, params))

        candidates = []
        for group_key, members in grouped_markets.items():
            candidate = self._evaluate_group(group_key, members, now)
            if candidate is None:
                continue
            candidates.append(candidate)
            evidence.append(candidate["group_evidence"])

        candidates.sort(key=lambda item: (item["rank_score"], item["best_edge_bps"]), reverse=True)
        best_candidate = candidates[0] if candidates else None
        actions = []
        if best_candidate and best_candidate["best_edge_bps"] >= int(self.strategy_config.get("risk", {}).get("min_edge_bps", 200)):
            if not self.should_execute_trade():
                skip_reasons.append("trade_enabled=false")
            else:
                actions = best_candidate["actions"]

        payload = {
            "timestamp": now.isoformat(),
            "strategy_id": self.strategy_id,
            "markets_considered": [item["group_id"] for item in candidates],
            "predicted_probability": None,
            "market_implied_probability": None,
            "expected_edge_bps": best_candidate["best_edge_bps"] if best_candidate else 0,
            "confidence": 0.72 if actions else (0.58 if best_candidate else None),
            "evidence_items": evidence,
            "risk_notes": (
                "Partition baskets are only traded when the grouped YES asks are coherent, contiguous, "
                "and liquid enough to avoid stale-book and downstream 404 orderbook errors."
            ),
            "exit_plan": "Hold the full bracket basket to resolution or stand down if the grouped prices normalize.",
            "thinking": (
                "ALGO-3 groups related weather brackets, rejects malformed or stale baskets, applies time-aware edge decay, "
                "and only lifts the highest-ranked underpriced partition."
            ),
            "web_searches_used": [],
            "actions": actions,
            "no_action_reason": None if actions else self._build_no_action_reason(best_candidate, skip_reasons),
        }
        return parse_decision_payload(payload, strategy_type="algo")

    def _evaluate_group(self, group_key, members: list[tuple], now: datetime) -> dict | None:
        city, metric, forecast_date, unit = group_key
        group_id = f"{city}:{metric}:{forecast_date}:{unit}"
        quotes = []
        total_volume = 0.0
        end_time = None

        for row, params in sorted(members, key=lambda item: float(item[1].lower_bound or -9999.0)):
            try:
                outcomes = json.loads(row["outcomes_json"])
            except json.JSONDecodeError as exc:
                logger.warning("Skipping partition group %s due to malformed outcomes_json in %s: %s", group_id, row["market_id"], exc)
                return None
            yes_outcome = next((item for item in outcomes if str(item.get("label", "")).lower() == "yes"), None)
            if not yes_outcome:
                logger.info("Skipping partition group %s because market %s has no YES outcome", group_id, row["market_id"])
                return None

            outcome_id = yes_outcome.get("outcome_id")
            if outcome_id in (None, ""):
                logger.info("Skipping partition group %s because market %s is missing outcome_id", group_id, row["market_id"])
                return None

            yes_ask = yes_outcome.get("best_ask")
            yes_bid = yes_outcome.get("best_bid")
            mid_price = yes_outcome.get("mid_price")
            if yes_ask is None and mid_price is not None:
                yes_ask = mid_price
            if yes_bid is None and mid_price is not None:
                yes_bid = max(float(mid_price) - 0.01, 0.0)
            if yes_ask is None or yes_bid is None:
                logger.info(
                    "Skipping partition group %s because market %s has incomplete quotes; likely downstream orderbook error risk",
                    group_id,
                    row["market_id"],
                )
                return None

            yes_ask = float(yes_ask)
            yes_bid = float(yes_bid)
            if yes_ask < yes_bid:
                logger.info("Skipping partition group %s because market %s has crossed quotes", group_id, row["market_id"])
                return None
            if yes_bid <= 0.0 and yes_ask >= 1.0:
                logger.info(
                    "Skipping partition group %s because market %s looks stale and often leads to 404 orderbook errors",
                    group_id,
                    row["market_id"],
                )
                return None

            quotes.append(
                {
                    "row": row,
                    "params": params,
                    "yes_outcome": yes_outcome,
                    "yes_ask": yes_ask,
                    "yes_bid": yes_bid,
                }
            )
            total_volume += float(row["volume_usd"] or 0.0)
            row_end_time = self._parse_end_time(row["end_time"])
            end_time = row_end_time if end_time is None else min(end_time, row_end_time)

        if len(quotes) < 2:
            return None
        if not self._is_contiguous_partition(quotes):
            logger.info("Skipping partition group %s because bracket bounds are not contiguous", group_id)
            return None

        sum_yes_ask = sum(item["yes_ask"] for item in quotes)
        sum_yes_bid = sum(item["yes_bid"] for item in quotes)
        raw_edge_bps = int(round(max(0.0, 1.0 - sum_yes_ask) * 10000))
        decay_multiplier = 1.0
        if self._time_decay_enabled() and end_time is not None:
            decay_multiplier = compute_time_decay_multiplier(end_time, now=now, decay_config=self._time_decay_config())
        best_edge_bps = int(round(raw_edge_bps * decay_multiplier))
        if best_edge_bps <= 0:
            return None

        position_size = self._position_size_per_leg(len(quotes))
        actions = [
            {
                "action_type": "BUY",
                "market_id": item["row"]["market_id"],
                "venue": item["row"]["venue"],
                "outcome_id": item["yes_outcome"]["outcome_id"],
                "outcome_label": item["yes_outcome"]["label"],
                "amount_usd": position_size,
                "limit_price": item["yes_ask"],
                "reasoning_summary": (
                    f"Weather bracket basket under 1.00 by {best_edge_bps} bps after {decay_multiplier:.2f}x time decay."
                ),
            }
            for item in quotes
        ]
        rank_score = best_edge_bps * math.sqrt(max(total_volume, 1.0))
        bounds_text = ", ".join(
            f"{int(item['params'].lower_bound)}-{int(item['params'].upper_bound)}{item['params'].unit.upper()}"
            for item in quotes
        )
        return {
            "group_id": group_id,
            "actions": actions,
            "best_edge_bps": best_edge_bps,
            "rank_score": rank_score,
            "group_evidence": {
                "source": "partition_group",
                "content": (
                    f"{group_id}: brackets=[{bounds_text}], sum_yes_ask={sum_yes_ask:.4f}, "
                    f"sum_yes_bid={sum_yes_bid:.4f}, decay={decay_multiplier:.2f}, volume=${total_volume:.0f}."
                ),
            },
        }

    def _is_contiguous_partition(self, quotes: list[dict]) -> bool:
        previous_upper = None
        for item in quotes:
            lower = item["params"].lower_bound
            upper = item["params"].upper_bound
            if lower is None or upper is None:
                return False
            if previous_upper is not None and abs(lower - (previous_upper + 1.0)) > 1e-6:
                return False
            previous_upper = upper
        return True

    def _build_no_action_reason(self, best_candidate: dict | None, skip_reasons: list[str]) -> str:
        if best_candidate is None:
            if skip_reasons:
                return "; ".join(dict.fromkeys(skip_reasons))
            return "No weather bracket basket survived parsing, liquidity, and stale-book checks."
        min_edge = int(self.strategy_config.get("risk", {}).get("min_edge_bps", 200))
        if best_candidate["best_edge_bps"] < min_edge:
            return f"Best bracket edge {best_candidate['best_edge_bps']} bps below minimum {min_edge} bps."
        if not self.should_execute_trade():
            return "Trade execution disabled for this strategy; bracket signal recorded as research-only."
        return "No weather bracket basket exceeded the threshold after time decay."

    def _parse_end_time(self, value: str) -> datetime:
        end_time = datetime.fromisoformat(str(value))
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        return end_time

    def _min_market_volume_usd(self) -> float:
        return float(self.strategy_config.get("min_market_volume_usd", 500.0) or 500.0)

    def _time_decay_enabled(self) -> bool:
        return bool(self.strategy_config.get("time_decay_enabled", True))

    def _time_decay_config(self) -> dict[str, float]:
        raw = self.strategy_config.get("time_decay", {}) or {}
        return {
            key: float(raw.get(key, default))
            for key, default in DEFAULT_TIME_DECAY.items()
        }

    def _position_size_per_leg(self, leg_count: int) -> float:
        portfolio = self.db.get_portfolio(self.strategy_id)
        bankroll = portfolio.cash if portfolio else float(self.strategy_config.get("starting_balance", 10000.0))
        basket_budget = min(bankroll * 0.03, 30.0)
        return round(max(basket_budget / max(leg_count, 1), 5.0), 2)
