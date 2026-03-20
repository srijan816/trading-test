from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json

from arena.config import ROOT


def generate_recommendations(decision_rows: list[dict], portfolio_rows: list[dict]) -> dict:
    by_strategy: dict[str, dict] = {}
    for row in portfolio_rows:
        by_strategy[row["strategy_id"]] = {
            "portfolio_value": row["total_value"],
            "realized_pnl": row["realized_pnl"],
            "unrealized_pnl": row["unrealized_pnl"],
            "recommendations": [],
        }
        if row["max_drawdown"] > 0.10:
            by_strategy[row["strategy_id"]]["recommendations"].append({"parameter": "max_position_pct", "direction": "decrease", "delta": 0.01, "reason": "Max drawdown exceeded 10%."})
    for row in decision_rows:
        if (row.get("expected_edge_bps") or 0) < 0:
            by_strategy.setdefault(row["strategy_id"], {"recommendations": []})["recommendations"].append(
                {"parameter": "min_edge_bps", "direction": "increase", "delta": 50, "reason": "Negative expected edge decisions appeared in logs."}
            )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "strategy_summaries": by_strategy,
        "approval_required": True,
    }


def write_weekly_report(report: dict, reports_dir: Path | None = None) -> Path:
    directory = reports_dir or (ROOT / "reports")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"weekly_{datetime.now(timezone.utc).date().isoformat()}.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return path
