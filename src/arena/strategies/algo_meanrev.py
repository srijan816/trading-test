from __future__ import annotations

from datetime import datetime, timezone
import json

from arena.intelligence.output_parser import parse_decision_payload
from arena.strategies.base import Strategy


class MeanReversionStrategy(Strategy):
    async def generate_decision(self):
        actions = []
        evidence = []
        considered = []
        for row in self.db.list_markets(category="crypto", status="active"):
            outcomes = json.loads(row["outcomes_json"])
            if not outcomes:
                continue
            considered.append(row["market_id"])
            outcome = outcomes[0]
            price = float(outcome.get("mid_price", 0.5))
            z_score = (price - 0.5) / 0.1
            if abs(z_score) >= 1.5:
                target = outcome
                actions.append(
                    {
                        "action_type": "BUY",
                        "market_id": row["market_id"],
                        "venue": row["venue"],
                        "outcome_id": target["outcome_id"],
                        "outcome_label": target["label"],
                        "amount_usd": 35.0,
                        "limit_price": target.get("best_ask"),
                        "reasoning_summary": "Crypto market move exceeds mean-reversion z-score threshold.",
                    }
                )
                evidence.append({"source": "z_score", "content": f"Observed z-score {z_score:.2f}"})
                break
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "strategy_id": self.strategy_id,
            "markets_considered": considered,
            "predicted_probability": 0.55 if actions else None,
            "market_implied_probability": None,
            "expected_edge_bps": 250 if actions else None,
            "confidence": 0.58 if actions else None,
            "evidence_items": evidence,
            "risk_notes": "Momentum may overpower short-term mean reversion.",
            "exit_plan": "Exit on reversion to mean or at market resolution.",
            "thinking": "ALGO-4 uses a simple z-score threshold to fade extreme short-term crypto moves.",
            "web_searches_used": [],
            "actions": actions,
            "no_action_reason": None if actions else "No crypto market exceeded the z-score threshold.",
        }
        return parse_decision_payload(payload, strategy_type="algo")
