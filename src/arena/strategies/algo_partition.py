from __future__ import annotations

from datetime import datetime, timezone
from collections import defaultdict
import json

from arena.intelligence.output_parser import parse_decision_payload
from arena.strategies.base import Strategy


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
        actions = []
        evidence = []
        considered = []
        skip_reasons = []
        deviation = 0.0
        groups = defaultdict(list)
        for row in self.db.list_markets(status="active"):
            end_time = datetime.fromisoformat(row["end_time"])
            if end_time <= now:
                continue
            if not self.is_market_eligible(row):
                continue
            event_group = row["event_group"]
            if not event_group:
                continue
            groups[event_group].append(row)
        for event_group, members in groups.items():
            if len(members) < 2:
                continue
            considered.append(event_group)
            member_quotes: list[dict] = []
            skip_group_reason = None
            for row in members:
                outcomes = json.loads(row["outcomes_json"])
                yes_outcome = next((item for item in outcomes if str(item.get("label", "")).lower() == "yes"), None)
                if not yes_outcome:
                    continue
                yes_ask = float(yes_outcome.get("best_ask", yes_outcome.get("mid_price", 1.0)) or 1.0)
                yes_bid = float(yes_outcome.get("best_bid", yes_outcome.get("mid_price", 0.0)) or 0.0)
                model_probability = float(yes_outcome.get("mid_price", yes_ask) or yes_ask)
                if model_probability < 0.01:
                    skip_group_reason = "Model probability too low for reliable edge estimate"
                    evidence.append(
                        {
                            "source": "partition_skip",
                            "content": f"{event_group}: skipped {row['market_id']} because proxy probability {model_probability:.4f} < 0.01",
                        }
                    )
                    break
                if yes_bid <= 0.0 and yes_ask >= 1.0:
                    skip_group_reason = f"Orderbook stale for market {row['market_id']}"
                    evidence.append(
                        {
                            "source": "partition_skip",
                            "content": f"{event_group}: skipped {row['market_id']} because the orderbook appears stale or unavailable",
                        }
                    )
                    break
                member_quotes.append(
                    {
                        "row": row,
                        "yes_outcome": yes_outcome,
                        "yes_ask": yes_ask,
                        "yes_bid": yes_bid,
                        "model_probability": model_probability,
                    }
                )
            if skip_group_reason:
                skip_reasons.append(skip_group_reason)
                continue
            if len(member_quotes) < 2:
                continue
            sum_yes_ask = sum(item["yes_ask"] for item in member_quotes)
            sum_yes_bid = sum(item["yes_bid"] for item in member_quotes)
            deviation = max(abs(1.0 - sum_yes_ask), abs(sum_yes_bid - 1.0))
            evidence.append(
                {
                    "source": "partition_group",
                    "content": f"{event_group}: members={len(member_quotes)} sum_yes_ask={sum_yes_ask:.4f} sum_yes_bid={sum_yes_bid:.4f}",
                }
            )
            if sum_yes_ask < 0.97:
                portfolio = self.db.get_portfolio(self.strategy_id)
                position_size = min(portfolio.cash * 0.02, 25.0) if portfolio else 25.0
                for item in member_quotes:
                    actions.append(
                        {
                            "action_type": "BUY",
                            "market_id": item["row"]["market_id"],
                            "venue": item["row"]["venue"],
                            "outcome_id": item["yes_outcome"]["outcome_id"],
                            "outcome_label": item["yes_outcome"]["label"],
                            "amount_usd": position_size,
                            "limit_price": item["yes_ask"],
                            "reasoning_summary": "Partition basket under 1.00 across grouped YES contracts.",
                        }
                    )
                break
        # Only carry non-zero edge when actions were actually generated
        capped_edge_bps = min(int(deviation * 10000), 3000) if actions else 0
        payload = {
            "timestamp": now.isoformat(),
            "strategy_id": self.strategy_id,
            "markets_considered": considered,
            "predicted_probability": None,
            "market_implied_probability": None,
            "expected_edge_bps": capped_edge_bps,
            "confidence": 0.7 if actions else None,
            "evidence_items": evidence,
            "risk_notes": "Grouped partition mispricings can persist if binary buckets are stale or slippage erases the basket edge.",
            "exit_plan": "Hold the full basket to resolution or exit if the grouped YES sum normalizes materially.",
            "thinking": "ALGO-3 groups related binary contracts into logical partition events and checks whether grouped YES prices materially deviate from one dollar.",
            "web_searches_used": [],
            "actions": actions,
            "no_action_reason": None if actions else ("; ".join(dict.fromkeys(skip_reasons)) if skip_reasons else "No grouped partition deviation exceeded the threshold."),
        }
        return parse_decision_payload(payload, strategy_type="algo")
