from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

from arena.intelligence.output_parser import parse_decision_payload
from arena.strategies.base import Strategy


class LateStageHarvesterStrategy(Strategy):
    def __init__(self, db, strategy_config: dict) -> None:
        super().__init__(db, strategy_config)
        self.supported_formats = (
            strategy_config.get("scope", {}).get("supported_formats")
            or strategy_config.get("supported_formats")
            or ["binary"]
        )

    async def generate_decision(self):
        now = datetime.now(timezone.utc)
        actions = []
        evidence = []
        considered = []
        for row in self.db.list_markets(status="active"):
            if not self.is_market_eligible(row):
                continue
            end_time = datetime.fromisoformat(row["end_time"])
            if end_time <= now:
                continue
            if end_time - now > timedelta(hours=6):
                continue
            outcomes = json.loads(row["outcomes_json"])
            considered.append(row["market_id"])
            for outcome in outcomes:
                price = float(outcome.get("best_ask", outcome.get("mid_price", 1.0)))
                if 0.88 <= price <= 0.92:
                    actions.append(
                        {
                            "action_type": "BUY",
                            "market_id": row["market_id"],
                            "venue": row["venue"],
                            "outcome_id": outcome["outcome_id"],
                            "outcome_label": outcome["label"],
                            "amount_usd": 50.0,
                            "limit_price": price,
                            "reasoning_summary": "Late-stage discount capture on near-certain outcome.",
                        }
                    )
                    evidence.append({"source": "late_stage_price", "content": f"{row['market_id']} trading at {price:.2f} inside harvester band"})
                    break
            if actions:
                break
        payload = {
            "timestamp": now.isoformat(),
            "strategy_id": self.strategy_id,
            "markets_considered": considered,
            "predicted_probability": 0.96 if actions else None,
            "market_implied_probability": actions[0]["limit_price"] if actions else None,
            "expected_edge_bps": int((0.96 - actions[0]["limit_price"]) * 10000) if actions else None,
            "confidence": 0.80 if actions else None,
            "evidence_items": evidence,
            "risk_notes": "Late reversals and thin liquidity near expiry can reduce edge.",
            "exit_plan": "Usually hold to resolution; do not chase if price rerates above threshold.",
            "thinking": "ALGO-2 scans only final-window markets and buys small discounts on high-probability outcomes.",
            "web_searches_used": [],
            "actions": actions,
            "no_action_reason": None if actions else "No qualifying late-stage discount found.",
        }
        return parse_decision_payload(payload, strategy_type="algo")
