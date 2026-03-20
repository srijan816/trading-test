from __future__ import annotations

from collections import defaultdict
from datetime import date

from arena.models import CostRow


def build_cost_rows(decision_rows: list[dict], as_of: date | None = None) -> list[CostRow]:
    day = as_of or date.today()
    grouped: dict[str, dict[str, float]] = defaultdict(lambda: {"in": 0, "out": 0, "llm": 0.0, "search": 0, "search_cost": 0.0})
    for row in decision_rows:
        strategy_id = row["strategy_id"]
        grouped[strategy_id]["in"] += row.get("llm_input_tokens") or 0
        grouped[strategy_id]["out"] += row.get("llm_output_tokens") or 0
        grouped[strategy_id]["llm"] += row.get("llm_cost_usd") or 0.0
        grouped[strategy_id]["search"] += row.get("search_queries_count") or 0
        grouped[strategy_id]["search_cost"] += row.get("search_cost_usd") or 0.0
    return [
        CostRow(
            strategy_id=strategy_id,
            as_of=day,
            llm_input_tokens=int(values["in"]),
            llm_output_tokens=int(values["out"]),
            llm_cost_usd=float(values["llm"]),
            search_queries=int(values["search"]),
            search_cost_usd=float(values["search_cost"]),
            total_cost_usd=float(values["llm"] + values["search_cost"]),
        )
        for strategy_id, values in grouped.items()
    ]
