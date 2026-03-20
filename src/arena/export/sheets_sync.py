from __future__ import annotations

from dataclasses import asdict

from arena.analytics.calibration import build_calibration_rows
from arena.analytics.cost_tracker import build_cost_rows


def _enrich_executions_with_wins(db, executions: list[dict]) -> list[dict]:
    """Join executions with resolutions to add a 'won' boolean field."""
    resolution_map: dict[tuple[str, str], str] = {}
    with db.connect() as conn:
        for row in conn.execute("SELECT market_id, venue, winning_outcome_id FROM resolutions"):
            resolution_map[(row["market_id"], row["venue"])] = row["winning_outcome_id"]
    for ex in executions:
        key = (ex.get("market_id"), ex.get("venue"))
        winning = resolution_map.get(key)
        if winning is not None:
            ex["won"] = str(ex.get("outcome_id")) == str(winning)
    return executions


def build_dashboard_payloads(db) -> dict[str, list[dict]]:
    snapshots = [dict(row) for row in db.list_daily_snapshots()]
    executions = [dict(row) for row in db.list_recent_executions(200)]
    decisions = [dict(row) for row in db.list_recent_decisions(limit=200)]
    enriched_executions = _enrich_executions_with_wins(db, executions)
    calibration = [asdict(row) for row in build_calibration_rows(decisions, enriched_executions)]
    costs = [asdict(row) for row in build_cost_rows(decisions)]
    return {
        "leaderboard": snapshots,
        "trade_feed": executions,
        "reasoning": decisions,
        "calibration": calibration,
        "costs": costs,
    }
