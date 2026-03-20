from __future__ import annotations

from datetime import datetime, timezone
import json

from arena.intelligence.output_parser import parse_decision_payload
from arena.strategies.base import Strategy


class MomentumFollowStrategy(Strategy):
    async def generate_decision(self):
        actions = []
        evidence = []
        considered = []
        for row in self.db.list_markets(category="weather", status="active"):
            outcomes = json.loads(row["outcomes_json"])
            if not outcomes:
                continue
            considered.append(row["market_id"])
            outcome = max(outcomes, key=lambda item: float(item.get("mid_price", 0.0)))
            price = float(outcome.get("mid_price", 0.0))
            if 0.55 <= price <= 0.75:
                actions.append(
                    {
                        "action_type": "BUY",
                        "market_id": row["market_id"],
                        "venue": row["venue"],
                        "outcome_id": outcome["outcome_id"],
                        "outcome_label": outcome["label"],
                        "amount_usd": 45.0,
                        "limit_price": outcome.get("best_ask"),
                        "reasoning_summary": "Weather price trend is strong enough to follow but not yet saturated.",
                    }
                )
                evidence.append({"source": "momentum_signal", "content": f"Current price {price:.2f} inside momentum band"})
                break
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "strategy_id": self.strategy_id,
            "markets_considered": considered,
            "predicted_probability": 0.7 if actions else None,
            "market_implied_probability": float(actions[0]["limit_price"]) if actions and actions[0]["limit_price"] is not None else None,
            "expected_edge_bps": 300 if actions else None,
            "confidence": 0.62 if actions else None,
            "evidence_items": evidence,
            "risk_notes": "Momentum can reverse on new forecast updates.",
            "exit_plan": "Reduce or close if the lead bucket loses relative strength.",
            "thinking": "ALGO-5 follows moderate weather-market momentum when the favorite is strengthening without being fully priced.",
            "web_searches_used": [],
            "actions": actions,
            "no_action_reason": None if actions else "No qualifying weather momentum setup found.",
        }
        return parse_decision_payload(payload, strategy_type="algo")
