from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import statistics
import tomllib
from typing import Any, Iterator

from arena.data_sources.weather_constants import REFERENCE_BIASES

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DB_PATH = PROJECT_ROOT / "data" / "arena.db"
ARENA_CONFIG_PATH = PROJECT_ROOT / "config" / "arena.toml"
STRATEGY_CONFIG_DIR = PROJECT_ROOT / "config" / "strategies"
LOG_DIR = PROJECT_ROOT / "logs"
SIGMA_CALIBRATION_PATH = PROJECT_ROOT / "data" / "sigma_calibration.json"

TIMESTAMP_COLUMNS = [
    "timestamp",
    "created_at",
    "updated_at",
    "entry_time",
    "resolved_at",
    "fetched_at",
    "computed_at",
    "last_updated_at",
    "snapshot_date",
    "observation_time",
    "forecast_date",
]

@dataclass(slots=True)
class StrategyMeta:
    strategy_id: str
    name: str
    enabled: bool
    cadence_minutes: int | None
    provider: str | None
    model_id: str | None
    starting_balance: float | None


def parse_json(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        if len(text) == 10 and text.count("-") == 2:
            return datetime.fromisoformat(text).replace(tzinfo=UTC)
        dt = datetime.fromisoformat(text)
        return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        return None


def relative_time(value: datetime | None, now: datetime | None = None) -> str:
    if not value:
        return "n/a"
    current = now or datetime.now(UTC)
    delta = current - value
    seconds = int(delta.total_seconds())
    if seconds < 0:
        seconds = abs(seconds)
        suffix = "from now"
    else:
        suffix = "ago"
    if seconds < 60:
        return f"{seconds}s {suffix}"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m {suffix}"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h {suffix}"
    days = hours // 24
    return f"{days}d {suffix}"


def short_question(question: str, limit: int = 72) -> str:
    text = question.strip()
    return text if len(text) <= limit else f"{text[: limit - 1]}…"


def pct_change(value: float | None) -> float | None:
    if value is None:
        return None
    return value * 100.0


def stddev(values: list[float]) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return 0.0
    return statistics.pstdev(values)


CRPS_HISTORY_PATH = PROJECT_ROOT / "data" / "crps_history.jsonl"
BRIER_HISTORY_PATH = PROJECT_ROOT / "data" / "brier_history.jsonl"
CALIBRATION_TRADEABLE_RATIO = 5.0


def ensure_row_factory(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    ensure_row_factory(conn)
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    ensure_row_factory(conn)
    if not table_exists(conn, table_name):
        return set()
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})")}


def normalized_time_expr(column: str) -> str:
    return f"datetime(replace(substr({column}, 1, 19), 'T', ' '))"


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    jsonl_path = Path(path)
    if not jsonl_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with jsonl_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def parse_market_city(question: str | None, fallback: str | None = None) -> str | None:
    if question:
        patterns = [
            r"highest temperature in (?P<city>.+?) be",
            r"temperature in (?P<city>.+?) on",
            r"in (?P<city>.+?) on",
        ]
        lowered = question.lower()
        for pattern in patterns:
            match = re.search(pattern, lowered)
            if match:
                city = match.group("city").strip(" ?.")
                return city.title()
    if fallback:
        return str(fallback).strip().title()
    return None


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    total_seconds = max(int(seconds), 0)
    hours, rem = divmod(total_seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def format_age(value: Any) -> str:
    return relative_time(parse_dt(value))


def normalize_model_name(model_name: str | None) -> str:
    text = str(model_name or "unknown").strip()
    lowered = text.lower()
    if "step-3.5-flash:free" in lowered:
        return "stepfun/step-3.5-flash:free"
    if "minimax" in lowered or "m2.7" in lowered:
        return "minimax/minimax-m2.7"
    return text or "unknown"


def estimate_research_cost(row: sqlite3.Row | dict[str, Any]) -> float:
    model_name = normalize_model_name(row["model_used"] if isinstance(row, sqlite3.Row) else row.get("model_used"))
    duration_ms = safe_float(row["duration_ms"] if isinstance(row, sqlite3.Row) else row.get("duration_ms"), 0.0) or 0.0
    from_cache = bool(int((row["from_cache"] if isinstance(row, sqlite3.Row) else row.get("from_cache")) or 0))
    if from_cache:
        return 0.0
    if model_name == "stepfun/step-3.5-flash:free":
        return 0.0
    if duration_ms <= 0:
        return 0.15 if model_name == "minimax/minimax-m2.7" else 0.05
    return (duration_ms / 60000.0) * 0.15


def summarize_skip_reason(payload: dict[str, Any]) -> str:
    spread_filter = payload.get("spread_filter")
    if isinstance(spread_filter, dict) and spread_filter.get("reason"):
        return str(spread_filter["reason"])
    risk_result = payload.get("risk_result")
    if isinstance(risk_result, dict) and risk_result.get("reason"):
        return str(risk_result["reason"])
    if payload.get("reason"):
        return str(payload["reason"])
    if payload.get("error"):
        return str(payload["error"])
    return "Unknown rejection"


def market_question_map(conn: sqlite3.Connection, market_ids: list[str]) -> dict[str, str]:
    ensure_row_factory(conn)
    unique_ids = [market_id for market_id in dict.fromkeys(market_ids) if market_id]
    if not unique_ids or not table_exists(conn, "markets"):
        return {}
    placeholders = ",".join("?" for _ in unique_ids)
    rows = conn.execute(
        f"SELECT market_id, question FROM markets WHERE market_id IN ({placeholders})",
        unique_ids,
    ).fetchall()
    return {str(row["market_id"]): str(row["question"]) for row in rows}


def build_ratio_svg(series: list[dict[str, Any]], width: int = 360, height: int = 120) -> dict[str, Any]:
    if not series:
        return {
            "width": width,
            "height": height,
            "points": "",
            "min_ratio": 0.0,
            "max_ratio": 5.0,
            "line_one_y": height - 18,
            "line_cutoff_y": 18,
            "start_label": "",
            "end_label": "",
        }
    values = [safe_float(point["ratio"], 0.0) or 0.0 for point in series]
    max_ratio = max(max(values), CALIBRATION_TRADEABLE_RATIO, 1.0)
    min_ratio = 0.0
    usable_width = max(width - 32, 1)
    usable_height = max(height - 28, 1)

    def scale_x(index: int) -> float:
        if len(series) == 1:
            return 16.0
        return 16.0 + (index / (len(series) - 1)) * usable_width

    def scale_y(value: float) -> float:
        pct = 0.0 if max_ratio == min_ratio else (value - min_ratio) / (max_ratio - min_ratio)
        return 10.0 + usable_height - (pct * usable_height)

    point_pairs = [f"{scale_x(idx):.1f},{scale_y(values[idx]):.1f}" for idx in range(len(values))]
    return {
        "width": width,
        "height": height,
        "points": " ".join(point_pairs),
        "min_ratio": min_ratio,
        "max_ratio": max_ratio,
        "line_one_y": scale_y(1.0),
        "line_cutoff_y": scale_y(CALIBRATION_TRADEABLE_RATIO),
        "start_label": series[0]["date"],
        "end_label": series[-1]["date"],
    }


def get_pnl_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    ensure_row_factory(conn)
    zero_summary = {
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "total_pnl": 0.0,
        "today_pnl": 0.0,
        "wins": 0,
        "losses": 0,
        "closed_positions": 0,
        "win_rate": None,
        "open_positions": 0,
    }
    if not table_exists(conn, "positions"):
        return zero_summary
    row = conn.execute(
        f"""
        SELECT
            COALESCE(SUM(CASE WHEN lower(status) IN ('closed', 'settled') THEN realized_pnl ELSE 0 END), 0) AS realized_pnl,
            COALESCE(SUM(CASE WHEN lower(status) = 'open' THEN unrealized_pnl ELSE 0 END), 0) AS unrealized_pnl,
            COALESCE(SUM(
                CASE
                    WHEN lower(status) IN ('closed', 'settled')
                     AND {normalized_time_expr("COALESCE(closed_at, last_updated_at, entry_time)")} >= datetime('now', 'start of day')
                    THEN realized_pnl
                    ELSE 0
                END
            ), 0) AS today_pnl,
            COUNT(CASE WHEN lower(status) IN ('closed', 'settled') AND realized_pnl > 0 THEN 1 END) AS wins,
            COUNT(CASE WHEN lower(status) IN ('closed', 'settled') AND realized_pnl < 0 THEN 1 END) AS losses,
            COUNT(CASE WHEN lower(status) IN ('closed', 'settled') THEN 1 END) AS closed_positions,
            COUNT(CASE WHEN lower(status) = 'open' THEN 1 END) AS open_positions
        FROM positions
        """
    ).fetchone()
    realized_pnl = safe_float(row["realized_pnl"], 0.0) or 0.0
    unrealized_pnl = safe_float(row["unrealized_pnl"], 0.0) or 0.0
    closed_positions = int(row["closed_positions"] or 0)
    wins = int(row["wins"] or 0)
    losses = int(row["losses"] or 0)
    return {
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "total_pnl": realized_pnl + unrealized_pnl,
        "today_pnl": safe_float(row["today_pnl"], 0.0) or 0.0,
        "wins": wins,
        "losses": losses,
        "closed_positions": closed_positions,
        "win_rate": (wins / closed_positions) if closed_positions else None,
        "open_positions": int(row["open_positions"] or 0),
    }


def get_execution_funnel(conn: sqlite3.Connection, hours: int = 24, strategy: str = "all") -> dict[str, Any]:
    ensure_row_factory(conn)
    base = {
        "hours": hours,
        "available": table_exists(conn, "events"),
        "structured_available": False,
        "mode": "not_available",
        "steps": [],
        "strategies": [],
        "selected_strategy": strategy,
        "per_strategy": {},
        "rejection_reasons": [],
        "legacy_summary": {"trade_intents": 0, "execution_skips": 0, "trades_executed": 0},
    }
    if not table_exists(conn, "events"):
        return base

    params: list[Any] = []
    strategy_clause = ""
    if strategy != "all":
        strategy_clause = " AND strategy_id = ?"
        params.append(strategy)

    gate_rows = conn.execute(
        f"""
        SELECT event_type, strategy_id, payload_json, created_at
        FROM events
        WHERE event_type LIKE 'execution_gate_%'
          AND {normalized_time_expr("created_at")} >= datetime('now', ?)
          {strategy_clause}
        ORDER BY created_at DESC
        """,
        (f"-{int(hours)} hours", *params),
    ).fetchall()
    skip_rows = conn.execute(
        f"""
        SELECT strategy_id, payload_json, created_at
        FROM events
        WHERE event_type = 'execution_skip'
          AND {normalized_time_expr("created_at")} >= datetime('now', ?)
          {strategy_clause}
        ORDER BY created_at DESC
        """,
        (f"-{int(hours)} hours", *params),
    ).fetchall()

    execution_rows: list[sqlite3.Row] = []
    if table_exists(conn, "executions"):
        execution_rows = conn.execute(
            f"""
            SELECT strategy_id, status, timestamp
            FROM executions
            WHERE {normalized_time_expr("timestamp")} >= datetime('now', ?)
              {strategy_clause}
            ORDER BY timestamp DESC
            """,
            (f"-{int(hours)} hours", *params),
        ).fetchall()

    decision_rows: list[sqlite3.Row] = []
    if table_exists(conn, "decisions"):
        decision_rows = conn.execute(
            f"""
            SELECT strategy_id, actions_json, timestamp
            FROM decisions
            WHERE {normalized_time_expr("timestamp")} >= datetime('now', ?)
              {strategy_clause}
            ORDER BY timestamp DESC
            """,
            (f"-{int(hours)} hours", *params),
        ).fetchall()

    market_ids: list[str] = []
    for row in skip_rows:
        payload = parse_json(row["payload_json"], {})
        market_ids.append(str(payload.get("market_id") or ""))
    market_map = market_question_map(conn, market_ids)

    rejections: dict[str, dict[str, Any]] = {}
    total_rejections = 0
    for row in skip_rows:
        payload = parse_json(row["payload_json"], {})
        reason = summarize_skip_reason(payload)
        market_id = str(payload.get("market_id") or "")
        label = market_map.get(market_id) or ("(all markets)" if "daily loss limit" in reason.lower() else "Various")
        entry = rejections.setdefault(
            reason,
            {"reason": reason, "count": 0, "example_market": label},
        )
        entry["count"] += 1
        total_rejections += 1

    legacy_summary = {
        "trade_intents": sum(1 for row in decision_rows if parse_json(row["actions_json"], [])),
        "execution_skips": len(skip_rows),
        "trades_executed": sum(1 for row in execution_rows if str(row["status"]).lower() == "filled"),
    }

    step_labels = {
        "execution_gate_market_active": "Market Scanned",
        "execution_gate_risk_approval": "Passed Risk",
        "execution_gate_orderbook": "Orderbook OK",
        "execution_gate_spread_filter": "Passed Spread",
        "execution_gate_kelly_sizing": "Passed Kelly",
    }
    structured_counts: dict[str, dict[str, int]] = {}
    strategies: set[str] = set()
    for row in gate_rows:
        payload = parse_json(row["payload_json"], {})
        strategy_id = str(row["strategy_id"] or payload.get("strategy_id") or "system")
        strategies.add(strategy_id)
        event_type = str(row["event_type"])
        structured_counts.setdefault(strategy_id, {})
        structured_counts[strategy_id].setdefault(event_type, 0)
        passed = payload.get("pass")
        if passed is None and isinstance(payload.get("risk_result"), dict):
            passed = payload["risk_result"].get("approved")
        if passed is None:
            passed = True
        if passed:
            structured_counts[strategy_id][event_type] += 1
        if event_type == "execution_gate_market_active":
            structured_counts[strategy_id].setdefault("_market_total", 0)
            structured_counts[strategy_id]["_market_total"] += 1

    if gate_rows:
        base["structured_available"] = True
        base["mode"] = "structured"
        strategy_names = sorted(strategies)
        base["strategies"] = strategy_names
        for strategy_id in strategy_names:
            counts = structured_counts.get(strategy_id, {})
            executed = sum(
                1
                for row in execution_rows
                if str(row["strategy_id"] or "") == strategy_id and str(row["status"]).lower() == "filled"
            )
            market_scanned = counts.get("_market_total", 0)
            steps = [
                {"key": "market_scanned", "label": "Market Scanned", "count": market_scanned},
                {"key": "passed_risk", "label": "Passed Risk", "count": counts.get("execution_gate_risk_approval", 0)},
                {"key": "orderbook_ok", "label": "Orderbook OK", "count": counts.get("execution_gate_orderbook", 0)},
                {"key": "passed_spread", "label": "Passed Spread", "count": counts.get("execution_gate_spread_filter", 0)},
                {"key": "passed_kelly", "label": "Passed Kelly", "count": counts.get("execution_gate_kelly_sizing", 0)},
                {"key": "trades_executed", "label": "Trades Executed", "count": executed},
            ]
            top_count = max(steps[0]["count"], 1)
            for step in steps:
                step["percent_of_top"] = (step["count"] / top_count) if top_count else None
            base["per_strategy"][strategy_id] = steps
        if strategy != "all" and strategy in base["per_strategy"]:
            base["steps"] = base["per_strategy"][strategy]
        else:
            aggregate_steps: list[dict[str, Any]] = []
            for index, label in enumerate(
                ["Market Scanned", "Passed Risk", "Orderbook OK", "Passed Spread", "Passed Kelly", "Trades Executed"]
            ):
                count = sum(strategy_steps[index]["count"] for strategy_steps in base["per_strategy"].values())
                aggregate_steps.append({"label": label, "count": count})
            top_count = max(aggregate_steps[0]["count"], 1)
            for step in aggregate_steps:
                step["percent_of_top"] = (step["count"] / top_count) if top_count else None
            base["steps"] = aggregate_steps
    else:
        base["mode"] = "legacy"
        base["strategies"] = sorted({str(row["strategy_id"] or "system") for row in skip_rows} | {str(row["strategy_id"] or "system") for row in execution_rows})

    rejection_rows = sorted(rejections.values(), key=lambda item: item["count"], reverse=True)
    for item in rejection_rows:
        item["percent"] = (item["count"] / total_rejections) if total_rejections else None
    base["rejection_reasons"] = rejection_rows
    base["legacy_summary"] = legacy_summary
    return base


def sigma_recommendation_from_ratio(current_sigma: float, ratio: float) -> float:
    if ratio > 10:
        return current_sigma * 3.0
    if ratio > 5:
        return current_sigma * 2.5
    if ratio > 2:
        return current_sigma * 1.5
    if ratio > 1.5:
        return current_sigma * 1.25
    if ratio < 0.8:
        return current_sigma * 0.9
    return current_sigma


def get_city_calibration(
    conn: sqlite3.Connection,
    crps_path: str | Path = CRPS_HISTORY_PATH,
    brier_path: str | Path = BRIER_HISTORY_PATH,
) -> list[dict[str, Any]]:
    ensure_row_factory(conn)
    crps_rows = read_jsonl(crps_path)
    if not crps_rows:
        return []
    brier_rows = read_jsonl(brier_path)
    grouped: dict[str, dict[str, Any]] = {}
    for row in crps_rows:
        city = parse_market_city(None, row.get("city"))
        if not city:
            continue
        group = grouped.setdefault(city, {"crps": [], "ratio": [], "sigma": [], "market_ids": set(), "series": {}})
        crps_value = safe_float(row.get("crps"))
        ratio_value = safe_float(row.get("calibration_ratio"))
        sigma_value = safe_float(row.get("sigma"))
        if crps_value is not None:
            group["crps"].append(crps_value)
        if ratio_value is not None:
            group["ratio"].append(ratio_value)
        if sigma_value is not None:
            group["sigma"].append(sigma_value)
        if row.get("market_id"):
            group["market_ids"].add(str(row["market_id"]))
        timestamp = parse_dt(row.get("timestamp"))
        if timestamp and ratio_value is not None:
            day_key = timestamp.date().isoformat()
            group["series"].setdefault(day_key, []).append(ratio_value)

    brier_by_city: dict[str, list[float]] = {}
    for row in brier_rows:
        city = parse_market_city(None, row.get("city"))
        brier_score = safe_float(row.get("brier_score"))
        if city and brier_score is not None:
            brier_by_city.setdefault(city, []).append(brier_score)

    adjustments: list[sqlite3.Row] = []
    if table_exists(conn, "parameter_adjustments"):
        adjustments = conn.execute(
            """
            SELECT strategy_id, parameter_name, current_value, recommended_value, reason, auto_applied, created_at, city
            FROM parameter_adjustments
            WHERE parameter_name LIKE 'ensemble_sigma%'
            ORDER BY created_at DESC
            """
        ).fetchall()

    results: list[dict[str, Any]] = []
    for city, values in sorted(grouped.items()):
        mean_crps = statistics.fmean(values["crps"]) if values["crps"] else None
        mean_ratio = statistics.fmean(values["ratio"]) if values["ratio"] else None
        current_sigma = values["sigma"][-1] if values["sigma"] else 1.44
        recommended_sigma = None
        matched_adjustment = None
        city_lower = city.lower()
        for adjustment in adjustments:
            haystack = f"{adjustment['strategy_id']} {adjustment['reason']} {adjustment['city']}".lower()
            if city_lower in haystack:
                matched_adjustment = adjustment
                break
        if matched_adjustment is not None:
            current_sigma = safe_float(matched_adjustment["current_value"], current_sigma) or current_sigma
            recommended_sigma = safe_float(matched_adjustment["recommended_value"], current_sigma) or current_sigma
        if recommended_sigma is None:
            recommended_sigma = sigma_recommendation_from_ratio(current_sigma, mean_ratio or 1.0)
        series = [
            {"date": day_key[5:], "ratio": round(statistics.fmean(day_values), 3)}
            for day_key, day_values in sorted(values["series"].items())[-30:]
        ]
        results.append(
            {
                "city": city,
                "markets": len(values["market_ids"]),
                "crps": round(mean_crps, 3) if mean_crps is not None else None,
                "ratio": round(mean_ratio, 3) if mean_ratio is not None else None,
                "brier": round(statistics.fmean(brier_by_city.get(city, [])), 3) if brier_by_city.get(city) else None,
                "sigma_current": round(current_sigma, 3),
                "sigma_recommended": round(recommended_sigma, 3),
                "tradeable": bool((mean_ratio or 0.0) <= CALIBRATION_TRADEABLE_RATIO),
                "tradeable_reason": None if (mean_ratio or 0.0) <= CALIBRATION_TRADEABLE_RATIO else f"ratio > {CALIBRATION_TRADEABLE_RATIO:.0f}x",
                "series": series,
                "chart": build_ratio_svg(series),
            }
        )
    return results


def get_open_limit_orders(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_row_factory(conn)
    if not table_exists(conn, "limit_orders"):
        return []
    market_map = market_question_map(
        conn,
        [str(row["market_id"]) for row in conn.execute(
            "SELECT market_id FROM limit_orders WHERE lower(status) IN ('open', 'pending', 'partial', 'stale')"
        ).fetchall()],
    )
    rows = conn.execute(
        """
        SELECT *
        FROM limit_orders
        WHERE lower(status) IN ('open', 'pending', 'partial', 'stale')
        ORDER BY placed_at DESC
        """
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "order_id": row["order_id"],
                "market_id": row["market_id"],
                "market": market_map.get(str(row["market_id"]), str(row["market_id"])),
                "side": row["side"],
                "price": safe_float(row["limit_price"]),
                "size": safe_float(row["size_dollars"]),
                "status": str(row["status"]).upper(),
                "age": format_age(row["placed_at"]),
                "edge_bps": safe_float(row["edge_bps"]),
                "placed_at": parse_dt(row["placed_at"]),
            }
        )
    return result


def get_order_stats(conn: sqlite3.Connection, hours: int = 24) -> dict[str, Any]:
    ensure_row_factory(conn)
    empty_stats = {
        "active": False,
        "open_orders": 0,
        "open_notional": 0.0,
        "fill_rate": None,
        "avg_time_to_fill_seconds": None,
        "avg_time_to_fill_display": "-",
        "avg_slippage_cents": None,
        "cancelled_expired_rate": None,
    }
    if not table_exists(conn, "limit_orders"):
        return empty_stats
    rows = conn.execute(
        f"""
        SELECT *
        FROM limit_orders
        WHERE {normalized_time_expr("updated_at")} >= datetime('now', ?)
        """,
        (f"-{int(hours)} hours",),
    ).fetchall()
    open_rows = [row for row in rows if str(row["status"]).lower() in {"open", "pending", "partial", "stale"}]
    closed_rows = [row for row in rows if str(row["status"]).lower() in {"filled", "cancelled", "expired", "rejected"}]
    filled_rows = [row for row in rows if str(row["status"]).lower() == "filled"]
    fill_denominator = len(closed_rows)
    slippage_values: list[float] = []
    for row in filled_rows:
        limit_price = safe_float(row["limit_price"])
        fill_price = safe_float(row["fill_price"])
        if limit_price is None or fill_price is None:
            continue
        side = str(row["side"]).lower()
        improvement = (limit_price - fill_price) * 100.0 if "buy" in side else (fill_price - limit_price) * 100.0
        slippage_values.append(improvement)
    fill_durations = []
    for row in filled_rows:
        placed_at = parse_dt(row["placed_at"])
        filled_at = parse_dt(row["filled_at"])
        if placed_at and filled_at:
            fill_durations.append((filled_at - placed_at).total_seconds())
    cancelled_or_expired = sum(1 for row in closed_rows if str(row["status"]).lower() in {"cancelled", "expired"})
    return {
        "active": True,
        "open_orders": len(open_rows),
        "open_notional": sum(safe_float(row["size_dollars"], 0.0) or 0.0 for row in open_rows),
        "fill_rate": (len(filled_rows) / fill_denominator) if fill_denominator else None,
        "avg_time_to_fill_seconds": statistics.fmean(fill_durations) if fill_durations else None,
        "avg_time_to_fill_display": format_duration(statistics.fmean(fill_durations) if fill_durations else None),
        "avg_slippage_cents": statistics.fmean(slippage_values) if slippage_values else None,
        "cancelled_expired_rate": (cancelled_or_expired / fill_denominator) if fill_denominator else None,
    }


def get_order_history(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    ensure_row_factory(conn)
    if not table_exists(conn, "limit_orders"):
        return []
    market_map = market_question_map(
        conn,
        [str(row["market_id"]) for row in conn.execute("SELECT market_id FROM limit_orders ORDER BY updated_at DESC LIMIT ?", (limit,)).fetchall()],
    )
    rows = conn.execute(
        "SELECT * FROM limit_orders ORDER BY updated_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        {
            "order_id": row["order_id"],
            "market": market_map.get(str(row["market_id"]), str(row["market_id"])),
            "side": row["side"],
            "price": safe_float(row["limit_price"]),
            "size": safe_float(row["size_dollars"]),
            "status": str(row["status"]).upper(),
            "placed_at": parse_dt(row["placed_at"]),
            "updated_at": parse_dt(row["updated_at"]),
            "fill_price": safe_float(row["fill_price"]),
            "cancel_reason": row["cancel_reason"],
        }
        for row in rows
    ]


def pick_column(columns: set[str], options: list[str]) -> str | None:
    for option in options:
        if option in columns:
            return option
    return None


def get_discovery_signals(conn: sqlite3.Connection, hours: int = 24) -> list[dict[str, Any]]:
    ensure_row_factory(conn)
    if not table_exists(conn, "discovery_alerts"):
        return []
    columns = table_columns(conn, "discovery_alerts")
    created_col = pick_column(columns, ["created_at", "timestamp", "detected_at"])
    type_col = pick_column(columns, ["signal_type", "type", "alert_type"])
    headline_col = pick_column(columns, ["headline", "title", "summary", "message"])
    relevance_col = pick_column(columns, ["relevance", "relevance_score", "score"])
    direction_col = pick_column(columns, ["direction", "signal_direction", "bias"])
    acted_col = pick_column(columns, ["acted_on", "action_taken", "used_in_decision"])
    market_col = pick_column(columns, ["market_id", "market"])
    if not created_col:
        return []
    rows = conn.execute(
        f"""
        SELECT *
        FROM discovery_alerts
        WHERE {normalized_time_expr(created_col)} >= datetime('now', ?)
        ORDER BY {created_col} DESC
        """,
        (f"-{int(hours)} hours",),
    ).fetchall()
    market_map = market_question_map(conn, [str(row[market_col]) for row in rows if market_col and row[market_col]])
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "time": parse_dt(row[created_col]),
                "type": str(row[type_col] if type_col else "SIGNAL"),
                "market": market_map.get(str(row[market_col]), str(row[market_col])) if market_col and row[market_col] else "-",
                "headline": str(row[headline_col]) if headline_col and row[headline_col] else "Signal",
                "relevance": safe_float(row[relevance_col]) if relevance_col else None,
                "direction": str(row[direction_col]) if direction_col and row[direction_col] else "-",
                "acted_on": bool(int(row[acted_col])) if acted_col and row[acted_col] is not None else None,
            }
        )
    return result


def get_discovery_stats(conn: sqlite3.Connection, hours: int = 24) -> dict[str, Any]:
    ensure_row_factory(conn)
    empty = {
        "active": False,
        "searches": 0,
        "signals_found": 0,
        "signal_rate": None,
        "acted_on": 0,
        "action_rate": None,
        "estimated_cost": 0.0,
        "signal_pnl": 0.0,
        "roi": None,
    }
    if not table_exists(conn, "discovery_alerts"):
        return empty
    signals = get_discovery_signals(conn, hours=hours)
    signal_rows = [row for row in signals if row["type"] != "NO_SIGNAL"]
    acted_rows = [row for row in signal_rows if row["acted_on"]]
    estimated_cost = 0.0
    columns = table_columns(conn, "discovery_alerts")
    cost_col = pick_column(columns, ["cost_usd", "estimated_cost_usd", "research_cost_usd"])
    market_col = pick_column(columns, ["market_id", "market"])
    created_col = pick_column(columns, ["created_at", "timestamp", "detected_at"])
    if cost_col and created_col:
        row = conn.execute(
            f"""
            SELECT COALESCE(SUM({cost_col}), 0) AS total_cost
            FROM discovery_alerts
            WHERE {normalized_time_expr(created_col)} >= datetime('now', ?)
            """,
            (f"-{int(hours)} hours",),
        ).fetchone()
        estimated_cost = safe_float(row["total_cost"], 0.0) or 0.0
    acted_market_ids: set[str] = set()
    type_col = pick_column(columns, ["signal_type", "type", "alert_type"])
    if market_col and created_col:
        rows = conn.execute(
            f"""
            SELECT {market_col} AS market_id
            FROM discovery_alerts
            WHERE {normalized_time_expr(created_col)} >= datetime('now', ?)
              {f"AND lower(COALESCE({type_col}, 'signal')) != 'no_signal'" if type_col else ""}
            """,
            (f"-{int(hours)} hours",),
        ).fetchall()
        acted_market_ids = {str(row["market_id"]) for row in rows if row["market_id"]}
    signal_pnl = 0.0
    if acted_market_ids and table_exists(conn, "positions"):
        placeholders = ",".join("?" for _ in acted_market_ids)
        pnl_row = conn.execute(
            f"""
            SELECT
                COALESCE(SUM(realized_pnl), 0) AS realized_pnl,
                COALESCE(SUM(CASE WHEN lower(status) = 'open' THEN unrealized_pnl ELSE 0 END), 0) AS unrealized_pnl
            FROM positions
            WHERE market_id IN ({placeholders})
            """,
            list(acted_market_ids),
        ).fetchone()
        signal_pnl = (safe_float(pnl_row["realized_pnl"], 0.0) or 0.0) + (safe_float(pnl_row["unrealized_pnl"], 0.0) or 0.0)
    return {
        "active": True,
        "searches": len(signals),
        "signals_found": len(signal_rows),
        "signal_rate": (len(signal_rows) / len(signals)) if signals else None,
        "acted_on": len(acted_rows),
        "action_rate": (len(acted_rows) / len(signal_rows)) if signal_rows else None,
        "estimated_cost": estimated_cost,
        "signal_pnl": signal_pnl,
        "roi": (signal_pnl / estimated_cost) if estimated_cost else None,
    }


def _load_strategy_meta_from_disk() -> dict[str, StrategyMeta]:
    meta: dict[str, StrategyMeta] = {}
    for path in sorted(STRATEGY_CONFIG_DIR.glob("*.toml")):
        with path.open("rb") as handle:
            data = tomllib.load(handle)
        section = data.get("strategy", {})
        strategy_id = str(section.get("id") or path.stem)
        model = section.get("model", {})
        schedule = section.get("schedule", {})
        meta[strategy_id] = StrategyMeta(
            strategy_id=strategy_id,
            name=str(section.get("name") or strategy_id),
            enabled=bool(section.get("enabled", True)),
            cadence_minutes=int(schedule.get("cadence_minutes")) if schedule.get("cadence_minutes") is not None else None,
            provider=model.get("provider"),
            model_id=model.get("model_id"),
            starting_balance=safe_float(section.get("starting_balance")),
        )
    return meta


def get_strategy_performance(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_row_factory(conn)
    strategy_meta = _load_strategy_meta_from_disk()
    with_positions = table_exists(conn, "positions")
    with_decisions = table_exists(conn, "decisions")
    with_executions = table_exists(conn, "executions")
    with_research = table_exists(conn, "research_log")
    with_snapshots = table_exists(conn, "daily_snapshots")
    with_markets = table_exists(conn, "markets")
    with_discovery = table_exists(conn, "discovery_alerts")

    positions = conn.execute("SELECT * FROM positions").fetchall() if with_positions else []
    decisions = conn.execute("SELECT * FROM decisions ORDER BY timestamp DESC").fetchall() if with_decisions else []
    executions = conn.execute("SELECT * FROM executions ORDER BY timestamp DESC").fetchall() if with_executions else []
    research_rows = conn.execute("SELECT * FROM research_log").fetchall() if with_research else []
    snapshots = conn.execute("SELECT * FROM daily_snapshots ORDER BY snapshot_date ASC").fetchall() if with_snapshots else []
    discovery_rows = conn.execute("SELECT * FROM discovery_alerts").fetchall() if with_discovery else []

    market_map: dict[str, sqlite3.Row] = {}
    if with_markets:
        market_ids = list({str(row["market_id"]) for row in positions + executions if row["market_id"]})
        if market_ids:
            placeholders = ",".join("?" for _ in market_ids)
            market_rows = conn.execute(
                f"SELECT market_id, question, category FROM markets WHERE market_id IN ({placeholders})",
                market_ids,
            ).fetchall()
            market_map = {str(row["market_id"]): row for row in market_rows}

    positions_by_strategy: dict[str, list[sqlite3.Row]] = {}
    for row in positions:
        positions_by_strategy.setdefault(str(row["strategy_id"]), []).append(row)
    decisions_by_strategy: dict[str, list[sqlite3.Row]] = {}
    for row in decisions:
        decisions_by_strategy.setdefault(str(row["strategy_id"]), []).append(row)
    executions_by_strategy: dict[str, list[sqlite3.Row]] = {}
    for row in executions:
        executions_by_strategy.setdefault(str(row["strategy_id"]), []).append(row)
    research_by_strategy: dict[str, list[sqlite3.Row]] = {}
    for row in research_rows:
        research_by_strategy.setdefault(str(row["strategy"] or "system"), []).append(row)
    snapshots_by_strategy: dict[str, list[sqlite3.Row]] = {}
    for row in snapshots:
        snapshots_by_strategy.setdefault(str(row["strategy_id"]), []).append(row)

    discovery_counts: dict[str, int] = {}
    if discovery_rows:
        columns = table_columns(conn, "discovery_alerts")
        strategy_col = pick_column(columns, ["strategy_id", "strategy"])
        type_col = pick_column(columns, ["signal_type", "type", "alert_type"])
        for row in discovery_rows:
            strategy_id = str(row[strategy_col] or "system") if strategy_col else "system"
            signal_type = str(row[type_col] or "SIGNAL") if type_col else "SIGNAL"
            if signal_type.upper() != "NO_SIGNAL":
                discovery_counts[strategy_id] = discovery_counts.get(strategy_id, 0) + 1

    strategy_ids = sorted(
        {strategy_id for strategy_id, meta in strategy_meta.items() if meta.enabled}
        | set(positions_by_strategy)
        | set(decisions_by_strategy)
        | set(executions_by_strategy)
    )

    sections: list[dict[str, Any]] = []
    for strategy_id in strategy_ids:
        meta = strategy_meta.get(strategy_id) or StrategyMeta(strategy_id, strategy_id, False, None, None, None, 10000.0)
        strategy_positions = positions_by_strategy.get(strategy_id, [])
        strategy_decisions = decisions_by_strategy.get(strategy_id, [])
        strategy_executions = executions_by_strategy.get(strategy_id, [])
        strategy_research = research_by_strategy.get(strategy_id, [])
        categories = []
        config_path = STRATEGY_CONFIG_DIR / f"{strategy_id}.toml"
        if config_path.exists():
            with config_path.open("rb") as handle:
                config_data = tomllib.load(handle)
            categories = list((((config_data.get("strategy", {}) or {}).get("scope", {}) or {}).get("categories", [])))
        weather_only = bool(categories) and all(str(category).lower() == "weather" for category in categories)
        trade_enabled = meta.enabled and weather_only

        realized_pnl = sum(safe_float(row["realized_pnl"], 0.0) or 0.0 for row in strategy_positions if str(row["status"]).lower() in {"closed", "settled"})
        unrealized_pnl = sum(safe_float(row["unrealized_pnl"], 0.0) or 0.0 for row in strategy_positions if str(row["status"]).lower() == "open")
        historical_pnl = sum(safe_float(row["realized_pnl"], 0.0) or 0.0 for row in strategy_positions if str(row["status"]).lower() not in {"open"})
        total_pnl = historical_pnl + unrealized_pnl
        winning_trades = sum(1 for row in strategy_positions if (safe_float(row["realized_pnl"], 0.0) or 0.0) > 0)
        losing_trades = sum(1 for row in strategy_positions if (safe_float(row["realized_pnl"], 0.0) or 0.0) < 0)
        trade_count = len(strategy_positions) or len(strategy_executions)
        win_rate = (winning_trades / (winning_trades + losing_trades)) if (winning_trades + losing_trades) else None
        avg_edge_values = [safe_float(row["expected_edge_bps"]) for row in strategy_decisions if parse_json(row["actions_json"], [])]
        avg_edge_values = [value for value in avg_edge_values if value is not None]
        durations = []
        for row in strategy_positions:
            start = parse_dt(row["entry_time"])
            end = parse_dt(row["closed_at"]) or parse_dt(row["last_updated_at"])
            if start and end:
                durations.append(max((end - start).total_seconds(), 0))
        best_trade = None
        worst_trade = None
        for row in strategy_positions:
            pnl_value = (safe_float(row["realized_pnl"], 0.0) or 0.0) + ((safe_float(row["unrealized_pnl"], 0.0) or 0.0) if str(row["status"]).lower() == "open" else 0.0)
            trade_info = {
                "pnl": pnl_value,
                "question": market_map.get(str(row["market_id"]))["question"] if str(row["market_id"]) in market_map else str(row["market_id"]),
            }
            if best_trade is None or pnl_value > best_trade["pnl"]:
                best_trade = trade_info
            if worst_trade is None or pnl_value < worst_trade["pnl"]:
                worst_trade = trade_info
        last_trade_at = parse_dt(strategy_executions[0]["timestamp"]) if strategy_executions else None

        daily_returns: list[float] = []
        previous_value = None
        for snapshot in snapshots_by_strategy.get(strategy_id, []):
            portfolio_value = safe_float(snapshot["portfolio_value"])
            if portfolio_value is None:
                continue
            if previous_value and previous_value > 0:
                daily_returns.append((portfolio_value - previous_value) / previous_value)
            previous_value = portfolio_value
        sharpe = None
        if len(daily_returns) >= 2:
            volatility = statistics.pstdev(daily_returns)
            if volatility > 0:
                sharpe = (statistics.fmean(daily_returns) / volatility) * math.sqrt(252)

        trades_7d = 0
        cutoff = datetime.now(UTC) - timedelta(days=7)
        for row in strategy_executions:
            ts = parse_dt(row["timestamp"])
            if ts and ts >= cutoff:
                trades_7d += 1

        status_label = "Active" if trade_enabled else ("Research" if meta.enabled else "Disabled")
        sections.append(
            {
                "strategy_id": strategy_id,
                "name": meta.name,
                "enabled": meta.enabled,
                "status_label": status_label,
                "status_tone": "positive" if trade_enabled else ("warning" if meta.enabled else "neutral"),
                "weather_only": weather_only,
                "maker_orders": table_exists(conn, "limit_orders"),
                "trade_enabled": trade_enabled,
                "trade_enabled_label": "Yes" if trade_enabled else "No",
                "trades": trade_count,
                "trades_7d": trades_7d,
                "wins": winning_trades,
                "losses": losing_trades,
                "win_rate": win_rate,
                "realized_pnl": realized_pnl,
                "unrealized_pnl": unrealized_pnl,
                "total_pnl": total_pnl,
                "sharpe": sharpe,
                "avg_edge_bps": statistics.fmean(avg_edge_values) if avg_edge_values else None,
                "avg_duration_seconds": statistics.fmean(durations) if durations else None,
                "avg_duration_display": format_duration(statistics.fmean(durations) if durations else None),
                "best_trade": best_trade,
                "worst_trade": worst_trade,
                "research_calls": len(strategy_research),
                "signals_found": discovery_counts.get(strategy_id, 0),
                "last_trade_at": last_trade_at,
            }
        )
    return sections


def get_research_costs(conn: sqlite3.Connection, hours: int = 24) -> dict[str, Any]:
    ensure_row_factory(conn)
    empty = {
        "today": {"cost": 0.0, "calls": 0, "avg_cost": 0.0},
        "week": {"cost": 0.0, "calls": 0},
        "month": {"cost": 0.0, "calls": 0},
        "by_model": [],
        "by_strategy": [],
        "researched_trade_pnl": 0.0,
        "roi": None,
    }
    if not table_exists(conn, "research_log"):
        return empty
    rows = conn.execute("SELECT * FROM research_log ORDER BY timestamp DESC").fetchall()
    today_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = datetime.now(UTC) - timedelta(days=7)
    month_start = datetime.now(UTC) - timedelta(days=30)
    model_buckets: dict[str, dict[str, Any]] = {}
    strategy_buckets: dict[str, dict[str, Any]] = {}
    today_cost = today_calls = 0
    week_cost = week_calls = 0
    month_cost = month_calls = 0
    researched_market_pairs: set[tuple[str, str]] = set()
    for row in rows:
        ts = parse_dt(row["timestamp"])
        cost = estimate_research_cost(row)
        model_name = normalize_model_name(row["model_used"])
        strategy_name = str(row["strategy"] or "system")
        bucket = model_buckets.setdefault(model_name, {"model": model_name, "cost": 0.0, "calls": 0})
        bucket["cost"] += cost
        bucket["calls"] += 1
        strategy_bucket = strategy_buckets.setdefault(strategy_name, {"strategy": strategy_name, "cost": 0.0, "calls": 0})
        strategy_bucket["cost"] += cost
        strategy_bucket["calls"] += 1
        if ts and ts >= today_start:
            today_cost += cost
            today_calls += 1
        if ts and ts >= week_start:
            week_cost += cost
            week_calls += 1
        if ts and ts >= month_start:
            month_cost += cost
            month_calls += 1
        if row["market_id"] and row["strategy"] and bool(int(row["used_in_decision"] or 0)):
            researched_market_pairs.add((str(row["strategy"]), str(row["market_id"])))

    researched_trade_pnl = 0.0
    if researched_market_pairs and table_exists(conn, "positions"):
        for strategy_id, market_id in researched_market_pairs:
            pnl_row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(realized_pnl), 0) AS realized_pnl,
                    COALESCE(SUM(CASE WHEN lower(status) = 'open' THEN unrealized_pnl ELSE 0 END), 0) AS unrealized_pnl
                FROM positions
                WHERE strategy_id = ? AND market_id = ?
                """,
                (strategy_id, market_id),
            ).fetchone()
            researched_trade_pnl += (safe_float(pnl_row["realized_pnl"], 0.0) or 0.0) + (safe_float(pnl_row["unrealized_pnl"], 0.0) or 0.0)

    return {
        "today": {"cost": today_cost, "calls": today_calls, "avg_cost": (today_cost / today_calls) if today_calls else 0.0},
        "week": {"cost": week_cost, "calls": week_calls},
        "month": {"cost": month_cost, "calls": month_calls},
        "by_model": sorted(model_buckets.values(), key=lambda item: item["cost"], reverse=True),
        "by_strategy": sorted(strategy_buckets.values(), key=lambda item: item["cost"], reverse=True),
        "researched_trade_pnl": researched_trade_pnl,
        "roi": (researched_trade_pnl / week_cost) if week_cost else None,
    }


def get_recent_activity_feed(conn: sqlite3.Connection, limit: int = 10) -> list[dict[str, Any]]:
    ensure_row_factory(conn)
    items: list[dict[str, Any]] = []
    market_ids: list[str] = []
    decision_rows = conn.execute("SELECT decision_id, strategy_id, timestamp, actions_json, no_action_reason FROM decisions ORDER BY timestamp DESC LIMIT 10").fetchall() if table_exists(conn, "decisions") else []
    execution_rows = conn.execute("SELECT decision_id, strategy_id, timestamp, market_id, status, requested_amount_usd FROM executions ORDER BY timestamp DESC LIMIT 10").fetchall() if table_exists(conn, "executions") else []
    research_rows = conn.execute("SELECT id, strategy, timestamp, market_id, market_question, model_used, from_cache, error FROM research_log ORDER BY timestamp DESC LIMIT 10").fetchall() if table_exists(conn, "research_log") else []
    event_rows = conn.execute("SELECT event_type, strategy_id, payload_json, created_at FROM events ORDER BY created_at DESC LIMIT 10").fetchall() if table_exists(conn, "events") else []
    for row in execution_rows:
        market_ids.append(str(row["market_id"]))
    for row in research_rows:
        market_ids.append(str(row["market_id"] or ""))
    for row in event_rows:
        market_ids.append(str(parse_json(row["payload_json"], {}).get("market_id") or ""))
    market_map = market_question_map(conn, market_ids)

    for row in decision_rows:
        actions = parse_json(row["actions_json"], [])
        if actions:
            action = actions[0]
            items.append(
                {
                    "timestamp": parse_dt(row["timestamp"]),
                    "headline": f"{row['strategy_id']} proposed {action.get('action_type', 'trade')} {str(action.get('outcome_label', '')).upper()}",
                    "detail": short_question(market_map.get(str(action.get("market_id")), str(action.get("market_id"))), 96),
                    "status_class": "positive",
                }
            )
        else:
            items.append(
                {
                    "timestamp": parse_dt(row["timestamp"]),
                    "headline": f"{row['strategy_id']} held",
                    "detail": short_question(str(row["no_action_reason"] or "No action"), 96),
                    "status_class": "neutral",
                }
            )
    for row in execution_rows:
        items.append(
            {
                "timestamp": parse_dt(row["timestamp"]),
                "headline": f"{row['strategy_id']} execution {str(row['status']).upper()}",
                "detail": short_question(market_map.get(str(row["market_id"]), str(row["market_id"])), 96),
                "status_class": "positive" if str(row["status"]).lower() == "filled" else "warning",
            }
        )
    for row in research_rows:
        items.append(
            {
                "timestamp": parse_dt(row["timestamp"]),
                "headline": f"{row['strategy'] or 'system'} research",
                "detail": short_question(str(row["market_question"] or market_map.get(str(row["market_id"]), row["market_id"] or "research")), 96),
                "status_class": "negative" if row["error"] else ("warning" if bool(int(row["from_cache"] or 0)) else "positive"),
            }
        )
    for row in event_rows:
        payload = parse_json(row["payload_json"], {})
        items.append(
            {
                "timestamp": parse_dt(row["created_at"]),
                "headline": f"{row['strategy_id'] or 'system'} {row['event_type']}",
                "detail": short_question(str(payload.get("reason") or payload.get("error") or payload.get("market_id") or payload)[:120], 96),
                "status_class": "negative" if "error" in str(row["event_type"]).lower() else "warning",
            }
        )
    items.sort(key=lambda item: item["timestamp"] or datetime.fromtimestamp(0, tz=UTC), reverse=True)
    return items[:limit]


class DashboardQueries:
    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self.strategy_meta = self._load_strategy_meta()
        self.active_strategies = [meta for meta in self.strategy_meta.values() if meta.enabled]
        self.arena_config = self._load_arena_config()
        self.sigma_reference = self._load_sigma_reference()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _load_arena_config(self) -> dict[str, Any]:
        if not ARENA_CONFIG_PATH.exists():
            return {}
        with ARENA_CONFIG_PATH.open("rb") as fh:
            return tomllib.load(fh)

    def _load_strategy_meta(self) -> dict[str, StrategyMeta]:
        meta: dict[str, StrategyMeta] = {}
        for path in sorted(STRATEGY_CONFIG_DIR.glob("*.toml")):
            with path.open("rb") as fh:
                data = tomllib.load(fh)
            section = data.get("strategy", {})
            strategy_id = str(section.get("id") or path.stem)
            model = section.get("model", {})
            schedule = section.get("schedule", {})
            meta[strategy_id] = StrategyMeta(
                strategy_id=strategy_id,
                name=str(section.get("name") or strategy_id),
                enabled=bool(section.get("enabled", True)),
                cadence_minutes=int(schedule.get("cadence_minutes")) if schedule.get("cadence_minutes") is not None else None,
                provider=model.get("provider"),
                model_id=model.get("model_id"),
                starting_balance=safe_float(section.get("starting_balance")),
            )
        return meta

    def _load_sigma_reference(self) -> dict[str, Any]:
        if not SIGMA_CALIBRATION_PATH.exists():
            return {}
        return json.loads(SIGMA_CALIBRATION_PATH.read_text(encoding="utf-8"))

    def _market_row_map(self, conn: sqlite3.Connection, market_ids: list[str]) -> dict[str, sqlite3.Row]:
        unique = [market_id for market_id in dict.fromkeys(market_ids) if market_id]
        if not unique:
            return {}
        placeholders = ",".join("?" for _ in unique)
        rows = conn.execute(
            f"SELECT * FROM markets WHERE market_id IN ({placeholders})",
            unique,
        ).fetchall()
        return {str(row["market_id"]): row for row in rows}

    def _market_prices(self, market_row: sqlite3.Row | None) -> dict[str, Any]:
        if not market_row:
            return {"yes_ask": None, "no_ask": None, "yes_mid": None, "no_mid": None, "outcomes": []}
        outcomes = parse_json(market_row["outcomes_json"], [])
        yes = next((item for item in outcomes if str(item.get("label", "")).lower() == "yes"), {})
        no = next((item for item in outcomes if str(item.get("label", "")).lower() == "no"), {})
        return {
            "yes_ask": safe_float(yes.get("best_ask")),
            "no_ask": safe_float(no.get("best_ask")),
            "yes_mid": safe_float(yes.get("mid_price")),
            "no_mid": safe_float(no.get("mid_price")),
            "outcomes": outcomes,
        }

    def _pid_info(self) -> dict[str, Any]:
        pid_file = LOG_DIR / "burnin.pid"
        pid: int | None = None
        created_at: datetime | None = None
        alive = False
        if pid_file.exists():
            try:
                pid = int(pid_file.read_text(encoding="utf-8").strip())
                created_at = datetime.fromtimestamp(pid_file.stat().st_mtime, tz=UTC)
                os.kill(pid, 0)
                alive = True
            except Exception:
                alive = False
        return {
            "pid": pid,
            "alive": alive,
            "created_at": created_at,
            "uptime": relative_time(created_at) if created_at else "n/a",
        }

    def _latest_market_scan(self, conn: sqlite3.Connection) -> datetime | None:
        row = conn.execute("SELECT MAX(fetched_at) AS latest FROM markets").fetchone()
        return parse_dt(row["latest"]) if row else None

    def _latest_decision_time(self, conn: sqlite3.Connection) -> datetime | None:
        row = conn.execute("SELECT MAX(timestamp) AS latest FROM decisions").fetchone()
        return parse_dt(row["latest"]) if row else None

    def _first_decision_time(self, conn: sqlite3.Connection) -> datetime | None:
        row = conn.execute("SELECT MIN(timestamp) AS earliest FROM decisions").fetchone()
        return parse_dt(row["earliest"]) if row else None

    def _latest_resolution_time(self, conn: sqlite3.Connection) -> datetime | None:
        row = conn.execute("SELECT MAX(resolved_at) AS latest FROM resolutions").fetchone()
        return parse_dt(row["latest"]) if row else None

    def _latest_paper_reset(self, conn: sqlite3.Connection) -> datetime | None:
        row = conn.execute(
            "SELECT MAX(created_at) AS latest FROM events WHERE event_type = 'paper_reset'"
        ).fetchone()
        return parse_dt(row["latest"]) if row else None

    def get_topbar(self) -> dict[str, Any]:
        with self.connect() as conn:
            market_count = conn.execute("SELECT COUNT(*) AS n FROM markets").fetchone()["n"]
            first_decision = self._first_decision_time(conn)
            last_scan = self._latest_market_scan(conn)
            last_reset = self._latest_paper_reset(conn)
        pid_info = self._pid_info()
        now = datetime.now(UTC)
        burnin_day = 0
        if first_decision:
            burnin_day = (now.date() - first_decision.date()).days + 1
        mode = str(self.arena_config.get("execution", {}).get("mode", "paper"))
        return {
            "arena_name": str(self.arena_config.get("arena", {}).get("name", "Arena")).upper(),
            "mode": mode,
            "active_strategy_count": len(self.active_strategies),
            "market_count": market_count,
            "burnin_day": burnin_day,
            "pid_info": pid_info,
            "last_scan_at": last_scan,
            "last_scan_rel": relative_time(last_scan),
            "last_reset_at": last_reset,
            "last_reset_rel": relative_time(last_reset) if last_reset else "n/a",
            "live_status": "Live" if pid_info["alive"] else "Dead",
        }

    def get_overview(self) -> dict[str, Any]:
        with self.connect() as conn:
            pnl_summary = get_pnl_summary(conn)
            execution_funnel = get_execution_funnel(conn)
            strategy_rows = get_strategy_performance(conn)
            calibration_rows = get_city_calibration(conn, CRPS_HISTORY_PATH, BRIER_HISTORY_PATH)
            order_stats = get_order_stats(conn)
            discovery_stats = get_discovery_stats(conn)
            research_costs = get_research_costs(conn)
            activity = get_recent_activity_feed(conn, limit=12)
        return {
            "pnl_summary": pnl_summary,
            "execution_funnel": execution_funnel,
            "strategy_rows": strategy_rows,
            "calibration_rows": calibration_rows[:6],
            "order_stats": order_stats,
            "discovery_stats": discovery_stats,
            "research_costs": research_costs,
            "activity": activity,
        }

    def get_research_overview(self) -> dict[str, Any]:
        cutoff = (datetime.now(UTC) - timedelta(days=1)).replace(tzinfo=None).isoformat()
        with self.connect() as conn:
            latest_reset = self._latest_paper_reset(conn)
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_calls,
                    SUM(CASE WHEN from_cache = 1 THEN 1 ELSE 0 END) AS cache_hits,
                    SUM(CASE WHEN error IS NOT NULL AND trim(error) != '' THEN 1 ELSE 0 END) AS errors,
                    SUM(CASE WHEN used_in_decision = 1 THEN 1 ELSE 0 END) AS used_in_decision,
                    SUM(CASE WHEN reasoning_trace IS NOT NULL AND trim(reasoning_trace) != '' THEN 1 ELSE 0 END) AS reasoning_traces,
                    AVG(CASE WHEN duration_ms IS NOT NULL THEN duration_ms END) AS avg_duration_ms,
                    MAX(timestamp) AS last_research_at
                FROM research_log
                WHERE timestamp >= ?
                """,
                (cutoff,),
            ).fetchone()
        total_calls = int(row["total_calls"] or 0) if row else 0
        cache_hits = int(row["cache_hits"] or 0) if row else 0
        errors = int(row["errors"] or 0) if row else 0
        used_in_decision = int(row["used_in_decision"] or 0) if row else 0
        reasoning_traces = int(row["reasoning_traces"] or 0) if row else 0
        return {
            "total_calls": total_calls,
            "live_calls": max(total_calls - cache_hits - errors, 0),
            "cache_hits": cache_hits,
            "errors": errors,
            "used_in_decision": used_in_decision,
            "reasoning_traces": reasoning_traces,
            "avg_duration_ms": round(float(row["avg_duration_ms"]), 1) if row and row["avg_duration_ms"] is not None else None,
            "last_research_at": parse_dt(row["last_research_at"]) if row and row["last_research_at"] else None,
            "latest_reset_at": latest_reset,
        }

    def get_portfolio_cards(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM portfolios ORDER BY strategy_id"
            ).fetchall()
        cards = []
        for row in rows:
            meta = self.strategy_meta.get(row["strategy_id"])
            if not meta or not meta.enabled:
                continue
            total_trades = int(row["total_trades"])
            winning_trades = int(row["winning_trades"])
            win_rate = (winning_trades / total_trades) if total_trades else None
            cards.append(
                {
                    "strategy_id": row["strategy_id"],
                    "name": meta.name,
                    "cash": safe_float(row["cash"], 0.0) or 0.0,
                    "total_value": safe_float(row["total_value"], 0.0) or 0.0,
                    "realized_pnl": safe_float(row["realized_pnl"], 0.0) or 0.0,
                    "unrealized_pnl": safe_float(row["unrealized_pnl"], 0.0) or 0.0,
                    "win_rate": win_rate,
                    "total_trades": total_trades,
                    "updated_at": parse_dt(row["updated_at"]),
                }
            )
        return cards

    def get_recent_activity(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            latest_reset = self._latest_paper_reset(conn)
            decisions = conn.execute(
                "SELECT * FROM decisions ORDER BY timestamp DESC LIMIT 20"
            ).fetchall()
            executions = conn.execute(
                "SELECT * FROM executions ORDER BY timestamp DESC LIMIT 20"
            ).fetchall()
            research_rows = conn.execute(
                "SELECT * FROM research_log ORDER BY timestamp DESC LIMIT 20"
            ).fetchall()
            event_rows = conn.execute(
                """
                SELECT * FROM events
                WHERE event_type IN ('execution_skip', 'llm_decision_error', 'scan_error', 'search_warning')
                ORDER BY created_at DESC
                LIMIT 20
                """
            ).fetchall()
            market_ids: list[str] = []
            for row in decisions:
                for action in parse_json(row["actions_json"], []):
                    market_ids.append(str(action.get("market_id")))
            for row in executions:
                market_ids.append(str(row["market_id"]))
            for row in research_rows:
                market_ids.append(str(row["market_id"]))
            for row in event_rows:
                payload = parse_json(row["payload_json"], {})
                market_ids.append(str(payload.get("market_id", "")))
            market_map = self._market_row_map(conn, market_ids)

        items: list[dict[str, Any]] = []
        for row in decisions:
            timestamp = parse_dt(row["timestamp"])
            reset_boundary = "post-reset" if latest_reset and timestamp and timestamp >= latest_reset else ("pre-reset" if latest_reset and timestamp else None)
            actions = parse_json(row["actions_json"], [])
            if actions:
                action = actions[0]
                market = market_map.get(str(action.get("market_id")))
                kelly_match = re.search(r"Kelly:\s*([0-9.]+)", str(action.get("reasoning_summary", "")))
                items.append(
                    {
                        "kind": "decision_trade",
                        "status_class": "positive",
                        "timestamp": timestamp,
                        "strategy_id": row["strategy_id"],
                        "headline": f"{row['strategy_id']} -> BUY {str(action.get('outcome_label', '')).upper()} \"{short_question(market['question'] if market else str(action.get('market_id')))}\" @ ${safe_float(action.get('limit_price'), 0.0):.3f}",
                        "detail": f"prob={safe_float(row['predicted_probability'], 0.0):.3f}, edge={row['expected_edge_bps']}bps, kelly=${safe_float(action.get('amount_usd'), 0.0):.2f}" + (f" ({kelly_match.group(1)})" if kelly_match else ""),
                        "reset_boundary": reset_boundary,
                    }
                )
            else:
                reason = str(row["no_action_reason"] or "No action recorded.")
                status = "neutral"
                kind = "decision_no_action"
                if "error" in reason.lower() or str(row["decision_id"]).startswith("decision_error"):
                    status = "negative"
                    kind = "decision_error"
                elif any(term in reason.lower() for term in ("skip", "stale", "normalization", "warning", "truncated")):
                    status = "warning"
                    kind = "decision_skip"
                items.append(
                    {
                        "kind": kind,
                        "status_class": status,
                        "timestamp": timestamp,
                        "strategy_id": row["strategy_id"],
                        "headline": f"{row['strategy_id']} -> NO ACTION",
                        "detail": reason,
                        "reset_boundary": reset_boundary,
                    }
                )
        for row in executions:
            timestamp = parse_dt(row["timestamp"])
            reset_boundary = "post-reset" if latest_reset and timestamp and timestamp >= latest_reset else ("pre-reset" if latest_reset and timestamp else None)
            market = market_map.get(str(row["market_id"]))
            items.append(
                {
                    "kind": "execution",
                    "status_class": "positive" if str(row["status"]).lower() == "filled" else "warning",
                    "timestamp": timestamp,
                    "strategy_id": row["strategy_id"],
                    "headline": f"{row['strategy_id']} -> EXECUTED {row['action_type']} \"{short_question(market['question'] if market else row['market_id'])}\" @ ${safe_float(row['avg_fill_price'], 0.0):.3f}",
                    "detail": f"qty={safe_float(row['filled_quantity'], 0.0):.2f}, total=${safe_float(row['total_cost'], 0.0):.2f}, status={row['status']}",
                    "reset_boundary": reset_boundary,
                }
            )
        for row in research_rows:
            timestamp = parse_dt(row["timestamp"])
            reset_boundary = "post-reset" if latest_reset and timestamp and timestamp >= latest_reset else ("pre-reset" if latest_reset and timestamp else None)
            market = market_map.get(str(row["market_id"]))
            error_text = str(row["error"] or "").strip()
            from_cache = bool(int(row["from_cache"] or 0))
            status = "negative" if error_text else ("warning" if from_cache else "positive")
            trace_available = bool(str(row["reasoning_trace"] or "").strip())
            headline_target = short_question(market["question"] if market else str(row["market_question"] or row["market_id"] or "research"))
            items.append(
                {
                    "kind": "research",
                    "status_class": status,
                    "timestamp": timestamp,
                    "strategy_id": row["strategy"] or "system",
                    "headline": f"{row['strategy'] or 'system'} -> RESEARCH \"{headline_target}\"",
                    "detail": (
                        f"{str(row['endpoint'] or 'unknown')} · {str(row['mode'] or 'n/a')} · "
                        f"{'cache' if from_cache else 'live'} · "
                        f"sources={int(row['sources_count'] or 0)} · "
                        f"trace={'yes' if trace_available else 'no'}"
                        + (f" · error={error_text}" if error_text else "")
                    ),
                    "reset_boundary": reset_boundary,
                }
            )
        for row in event_rows:
            payload = parse_json(row["payload_json"], {})
            timestamp = parse_dt(row["created_at"])
            reset_boundary = "post-reset" if latest_reset and timestamp and timestamp >= latest_reset else ("pre-reset" if latest_reset and timestamp else None)
            kind = "event"
            status = "warning"
            if "error" in str(row["event_type"]).lower() or "fail" in str(row["event_type"]).lower():
                status = "negative"
            market = market_map.get(str(payload.get("market_id", "")))
            detail = payload.get("error") or payload.get("warning") or json.dumps(payload, sort_keys=True)[:180]
            items.append(
                {
                    "kind": kind,
                    "status_class": status,
                    "timestamp": timestamp,
                    "strategy_id": row["strategy_id"] or "system",
                    "headline": f"{row['strategy_id'] or 'system'} -> {str(row['event_type']).upper()}",
                    "detail": f"{short_question(market['question']) if market else ''} {detail}".strip(),
                    "reset_boundary": reset_boundary,
                }
            )
        items.sort(key=lambda item: item["timestamp"] or datetime.fromtimestamp(0, tz=UTC), reverse=True)
        return items[:limit]

    def get_edge_distribution(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM decisions WHERE actions_json != '[]' ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()
            market_ids = []
            for row in rows:
                actions = parse_json(row["actions_json"], [])
                if actions:
                    market_ids.append(str(actions[0].get("market_id")))
            market_map = self._market_row_map(conn, market_ids)
        result = []
        for row in rows:
            actions = parse_json(row["actions_json"], [])
            if not actions:
                continue
            action = actions[0]
            market = market_map.get(str(action.get("market_id")))
            result.append(
                {
                    "strategy_id": row["strategy_id"],
                    "market_id": action.get("market_id"),
                    "question": market["question"] if market else str(action.get("market_id")),
                    "edge_bps": int(row["expected_edge_bps"] or 0),
                    "probability": safe_float(row["predicted_probability"]),
                    "amount_usd": safe_float(action.get("amount_usd"), 0.0) or 0.0,
                    "timestamp": parse_dt(row["timestamp"]),
                }
            )
        return result

    def get_weather_monitor(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            forecast_rows = conn.execute(
                """
                SELECT fh.*
                FROM forecast_history fh
                JOIN (
                    SELECT location, source, MAX(created_at) AS latest
                    FROM forecast_history
                    GROUP BY location, source
                ) latest
                  ON latest.location = fh.location
                 AND latest.source = fh.source
                 AND latest.latest = fh.created_at
                ORDER BY fh.location, fh.source
                """
            ).fetchall()
            obs_rows = conn.execute(
                """
                SELECT so.*
                FROM station_observations so
                JOIN (
                    SELECT location, MAX(observation_time) AS latest
                    FROM station_observations
                    GROUP BY location
                ) latest
                  ON latest.location = so.location
                 AND latest.latest = so.observation_time
                ORDER BY so.location
                """
            ).fetchall()
        obs_map = {str(row["location"]): row for row in obs_rows}
        grouped: dict[str, list[sqlite3.Row]] = {}
        for row in forecast_rows:
            grouped.setdefault(str(row["location"]), []).append(row)
        results = []
        for location, rows in grouped.items():
            highs = [safe_float(row["predicted_high_c"]) for row in rows if safe_float(row["predicted_high_c"]) is not None]
            sigma = stddev([value for value in highs if value is not None])
            obs = obs_map.get(location)
            results.append(
                {
                    "location": location,
                    "ensemble_high_c": statistics.mean(highs) if highs else None,
                    "sigma_c": sigma,
                    "sources": len(highs),
                    "source_names": ", ".join(sorted({str(row["source"]) for row in rows})),
                    "trend": obs["trending"] if obs else None,
                    "last_obs_c": safe_float(obs["temperature_c"]) if obs else None,
                    "obs_time": parse_dt(obs["observation_time"]) if obs else None,
                }
            )
        return sorted(results, key=lambda item: item["location"])

    def get_positions_page(self) -> dict[str, Any]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM positions WHERE lower(status) = 'open' ORDER BY entry_time ASC"
            ).fetchall()
            market_map = self._market_row_map(conn, [str(row["market_id"]) for row in rows])
        positions = []
        total_exposure = 0.0
        total_unrealized = 0.0
        for row in rows:
            market = market_map.get(str(row["market_id"]))
            prices = self._market_prices(market)
            current_price = safe_float(row["current_price"])
            if current_price is None:
                label = str(row["outcome_label"]).lower()
                current_price = prices["yes_ask"] if label == "yes" else prices["no_ask"]
            entry_price = safe_float(row["avg_entry_price"], 0.0) or 0.0
            quantity = safe_float(row["quantity"], 0.0) or 0.0
            size_usd = entry_price * quantity
            unrealized = safe_float(row["unrealized_pnl"], 0.0) or 0.0
            total_exposure += size_usd
            total_unrealized += unrealized
            positions.append(
                {
                    "strategy_id": row["strategy_id"],
                    "market_id": row["market_id"],
                    "question": market["question"] if market else str(row["market_id"]),
                    "side": row["side"],
                    "outcome_label": row["outcome_label"],
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "size_usd": size_usd,
                    "unrealized_pnl": unrealized,
                    "opened_at": parse_dt(row["entry_time"]),
                    "resolves_at": parse_dt(market["end_time"]) if market else None,
                    "venue": market["venue"] if market else row["venue"],
                    "status_class": "positive" if unrealized >= 0 else "negative",
                }
            )
        positions.sort(key=lambda item: item["resolves_at"] or datetime.max.replace(tzinfo=UTC))
        return {
            "positions": positions,
            "summary": {
                "count": len(positions),
                "total_exposure": total_exposure,
                "net_unrealized_pnl": total_unrealized,
            },
        }

    def get_decision_log(self, strategy: str = "all", action_filter: str = "all", date_range: str = "today") -> dict[str, Any]:
        now = datetime.now(UTC)
        params: list[Any] = []
        query = "SELECT * FROM decisions WHERE 1=1"
        if strategy != "all":
            query += " AND strategy_id = ?"
            params.append(strategy)
        if date_range == "today":
            cutoff = now - timedelta(days=1)
            query += " AND timestamp >= ?"
            params.append(cutoff.isoformat())
        elif date_range == "last3":
            cutoff = now - timedelta(days=3)
            query += " AND timestamp >= ?"
            params.append(cutoff.isoformat())
        elif date_range == "last7":
            cutoff = now - timedelta(days=7)
            query += " AND timestamp >= ?"
            params.append(cutoff.isoformat())
        query += " ORDER BY timestamp DESC LIMIT 250"
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
            market_ids = []
            for row in rows:
                actions = parse_json(row["actions_json"], [])
                if actions:
                    market_ids.append(str(actions[0].get("market_id")))
            market_map = self._market_row_map(conn, market_ids)
        decisions = []
        for row in rows:
            actions = parse_json(row["actions_json"], [])
            has_actions = bool(actions)
            no_action_reason = str(row["no_action_reason"] or "")
            status = "trade" if has_actions else "no_action"
            if "error" in no_action_reason.lower() or str(row["decision_id"]).startswith("decision_error"):
                status = "error"
            elif any(term in no_action_reason.lower() for term in ("skip", "stale", "truncated", "normalization")):
                status = "skip"
            if action_filter == "trades" and not has_actions:
                continue
            if action_filter == "no-action" and has_actions:
                continue
            if action_filter == "errors" and status != "error":
                continue
            action = actions[0] if actions else {}
            market = market_map.get(str(action.get("market_id"))) if action else None
            decisions.append(
                {
                    "decision_id": row["decision_id"],
                    "timestamp": parse_dt(row["timestamp"]),
                    "strategy_id": row["strategy_id"],
                    "status": status,
                    "action_label": f"{action.get('action_type', 'NO ACTION')} {action.get('outcome_label', '')}".strip() if has_actions else "NO ACTION",
                    "market_question": market["question"] if market else (short_question(no_action_reason, 90) if not has_actions else str(action.get("market_id"))),
                    "predicted_probability": safe_float(row["predicted_probability"]),
                    "expected_edge_bps": safe_float(row["expected_edge_bps"]),
                    "amount_usd": safe_float(action.get("amount_usd")) if has_actions else None,
                    "decision_row": row,
                }
            )
        return {
            "filters": {
                "strategy": strategy,
                "action_filter": action_filter,
                "date_range": date_range,
            },
            "decisions": decisions,
            "strategy_options": ["all"] + [meta.strategy_id for meta in self.active_strategies],
        }

    def get_research_pipeline(
        self,
        strategy: str = "all",
        endpoint: str = "all",
        date_range: str = "today",
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        params: list[Any] = []
        query = "SELECT * FROM research_log WHERE 1=1"
        latest_reset: datetime | None = None
        with self.connect() as conn:
            latest_reset = self._latest_paper_reset(conn)
        if strategy != "all":
            query += " AND strategy = ?"
            params.append(strategy)
        if endpoint != "all":
            query += " AND endpoint = ?"
            params.append(endpoint)
        if date_range == "today":
            cutoff = now - timedelta(days=1)
            query += " AND timestamp >= ?"
            params.append(cutoff.replace(tzinfo=None).isoformat())
        elif date_range == "last3":
            cutoff = now - timedelta(days=3)
            query += " AND timestamp >= ?"
            params.append(cutoff.replace(tzinfo=None).isoformat())
        elif date_range == "last7":
            cutoff = now - timedelta(days=7)
            query += " AND timestamp >= ?"
            params.append(cutoff.replace(tzinfo=None).isoformat())
        query += " ORDER BY timestamp DESC LIMIT 300"
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()

        entries = []
        durations = []
        source_counts = []
        endpoint_values: set[str] = set()
        model_counter: dict[str, int] = {}
        for row in rows:
            duration_ms = int(row["duration_ms"]) if row["duration_ms"] is not None else None
            if duration_ms is not None:
                durations.append(duration_ms)
            sources_count = int(row["sources_count"] or 0)
            source_counts.append(sources_count)
            endpoint_value = str(row["endpoint"] or "unknown")
            endpoint_values.add(endpoint_value)
            model_used = str(row["model_used"] or "unknown")
            model_counter[model_used] = model_counter.get(model_used, 0) + 1
            error_text = str(row["error"] or "").strip()
            status = "error" if error_text else ("cache" if int(row["from_cache"] or 0) else "live")
            entries.append(
                {
                    "id": int(row["id"]),
                    "timestamp": parse_dt(row["timestamp"]),
                    "reset_boundary": (
                        "post-reset"
                        if latest_reset and parse_dt(row["timestamp"]) and parse_dt(row["timestamp"]) >= latest_reset
                        else ("pre-reset" if latest_reset and parse_dt(row["timestamp"]) else None)
                    ),
                    "strategy": row["strategy"] or "system",
                    "market_id": row["market_id"],
                    "market_question": str(row["market_question"] or row["market_id"] or "unknown market"),
                    "query_sent": str(row["query_sent"] or ""),
                    "endpoint": endpoint_value,
                    "mode": row["mode"] or "n/a",
                    "model_used": model_used,
                    "duration_ms": duration_ms,
                    "report_length": int(row["report_length"] or 0),
                    "sources_count": sources_count,
                    "report_summary": str(row["report_summary"] or ""),
                    "has_reasoning_trace": bool(str(row["reasoning_trace"] or "").strip()),
                    "probability": safe_float(row["probability"]),
                    "confidence": row["confidence"],
                    "from_cache": bool(int(row["from_cache"] or 0)),
                    "used_in_decision": bool(int(row["used_in_decision"] or 0)),
                    "error": error_text or None,
                    "status": status,
                }
            )

        summary = {
            "total_calls": len(entries),
            "cache_hits": sum(1 for entry in entries if entry["from_cache"]),
            "fresh_calls": sum(1 for entry in entries if not entry["from_cache"] and not entry["error"]),
            "errors": sum(1 for entry in entries if entry["error"]),
            "used_in_decision": sum(1 for entry in entries if entry["used_in_decision"]),
            "avg_duration_ms": round(statistics.mean(durations), 1) if durations else None,
            "avg_sources": round(statistics.mean(source_counts), 1) if source_counts else None,
            "top_models": sorted(
                ({"model": model, "count": count} for model, count in model_counter.items()),
                key=lambda item: item["count"],
                reverse=True,
            )[:5],
        }
        return {
            "filters": {
                "strategy": strategy,
                "endpoint": endpoint,
                "date_range": date_range,
            },
            "entries": entries,
            "summary": summary,
            "latest_reset_at": latest_reset,
            "strategy_options": ["all"] + [meta.strategy_id for meta in self.active_strategies],
            "endpoint_options": ["all"] + sorted(endpoint_values or {"/api/v1/research", "/api/v1/market-research"}),
            "costs": self.get_research_costs_page(),
        }

    def get_research_detail(self, research_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            latest_reset = self._latest_paper_reset(conn)
            row = conn.execute("SELECT * FROM research_log WHERE id = ?", (research_id,)).fetchone()
        if not row:
            return None
        sources = parse_json(row["sources_json"], [])
        edge_assessment = parse_json(row["edge_assessment"], {})
        return {
            "id": int(row["id"]),
            "timestamp": parse_dt(row["timestamp"]),
            "reset_boundary": (
                "post-reset"
                if latest_reset and parse_dt(row["timestamp"]) and parse_dt(row["timestamp"]) >= latest_reset
                else ("pre-reset" if latest_reset and parse_dt(row["timestamp"]) else None)
            ),
            "strategy": row["strategy"] or "system",
            "market_id": row["market_id"],
            "market_question": row["market_question"],
            "query_sent": row["query_sent"],
            "endpoint": row["endpoint"],
            "mode": row["mode"],
            "model_used": row["model_used"],
            "duration_ms": int(row["duration_ms"]) if row["duration_ms"] is not None else None,
            "report_length": int(row["report_length"] or 0),
            "sources_count": int(row["sources_count"] or 0),
            "sources": sources,
            "report_summary": str(row["report_summary"] or ""),
            "full_report": str(row["full_report"] or ""),
            "reasoning_trace": str(row["reasoning_trace"] or "").strip(),
            "probability": safe_float(row["probability"]),
            "confidence": row["confidence"],
            "edge_assessment": edge_assessment,
            "from_cache": bool(int(row["from_cache"] or 0)),
            "used_in_decision": bool(int(row["used_in_decision"] or 0)),
            "error": str(row["error"] or "").strip() or None,
        }

    def get_decision_detail(self, decision_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM decisions WHERE decision_id = ?", (decision_id,)).fetchone()
            if not row:
                return None
            actions = parse_json(row["actions_json"], [])
            evidence = parse_json(row["evidence_items_json"], [])
            searches = parse_json(row["web_searches_used_json"], [])
            markets_considered = parse_json(row["markets_considered_json"], [])
            action_market_ids = [str(action.get("market_id")) for action in actions]
            market_map = self._market_row_map(conn, action_market_ids + [str(item) for item in markets_considered])
            executions = conn.execute(
                "SELECT * FROM executions WHERE decision_id = ? ORDER BY timestamp ASC",
                (decision_id,),
            ).fetchall()

        primary_action = actions[0] if actions else {}
        primary_market = market_map.get(str(primary_action.get("market_id"))) if primary_action else None
        if not primary_market and markets_considered:
            primary_market = market_map.get(str(markets_considered[0]))
        prices = self._market_prices(primary_market)
        evidence_blocks = []
        ensemble_data: dict[str, Any] = {}
        for item in evidence:
            content = str(item.get("content", ""))
            source = str(item.get("source", "evidence"))
            evidence_blocks.append(
                {
                    "source": source,
                    "content": content,
                    "retrieved_at": parse_dt(item.get("retrieved_at")),
                }
            )
            match = re.search(
                r"(?P<location>.+?) ensemble forecast: (?P<high>[-0-9.]+)C ±(?P<sigma>[-0-9.]+)C from (?P<sources>\d+) sources \((?P<source_names>[^)]+)\), bias correction (?P<bias>[-+0-9.]+)C",
                content,
            )
            if match:
                location = match.group("location")
                ensemble_data = {
                    "location": location,
                    "ensemble_method": "rmse_weighted",
                    "ensemble_high_c": safe_float(match.group("high")),
                    "ensemble_sigma_c": safe_float(match.group("sigma")),
                    "sources_used": int(match.group("sources")),
                    "source_names": [name.strip() for name in match.group("source_names").split(",")],
                    "bias_correction_total_c": safe_float(match.group("bias")),
                    "bias_corrections_applied": REFERENCE_BIASES.get(location, {}),
                    "sigma_multiplier": (self.sigma_reference.get("per_location", {}).get(location, {}) or {}).get(
                        "sigma_mult",
                        self.sigma_reference.get("best_sigma_multiplier"),
                    ),
                }
        predicted_probability = safe_float(row["predicted_probability"])
        action_side = str(primary_action.get("action_type", "NO_ACTION")).upper()
        outcome_label = str(primary_action.get("outcome_label", "")).upper()
        side_for_edge = "YES" if outcome_label == "YES" else "NO"
        market_price = prices["yes_ask"] if side_for_edge == "YES" else prices["no_ask"]
        raw_edge_bps = None
        if predicted_probability is not None and market_price is not None:
            comparable_prob = predicted_probability if side_for_edge == "YES" else 1.0 - predicted_probability
            raw_edge_bps = int((comparable_prob - market_price) * 10000)
        meta = self.strategy_meta.get(str(row["strategy_id"]))
        bankroll = meta.starting_balance if meta and meta.starting_balance is not None else 10000.0
        fee_rate = safe_float(self.arena_config.get("position_sizing", {}).get("fee_rate"), 0.02) or 0.02
        position_sizing: dict[str, Any] = {}
        if predicted_probability is not None and market_price not in (None, 0.0, 1.0):
            payout_ratio = (1.0 - market_price) / market_price if market_price else None
            if payout_ratio:
                kelly_full = (predicted_probability * payout_ratio - (1 - predicted_probability)) / payout_ratio
                quarter_kelly = kelly_full * float(self.arena_config.get("position_sizing", {}).get("kelly_fraction", 0.25))
                raw_amount = bankroll * quarter_kelly
                max_position = float(self.arena_config.get("position_sizing", {}).get("max_position_usd", 25.0))
                position_sizing = {
                    "kelly_full": kelly_full,
                    "quarter_kelly": quarter_kelly,
                    "bankroll": bankroll,
                    "raw_amount": raw_amount,
                    "capped_at": min(raw_amount, max_position) if raw_amount is not None else None,
                    "final_amount": safe_float(primary_action.get("amount_usd")) if primary_action else None,
                }
        exec_items = []
        for execution in executions:
            exec_items.append(
                {
                    "timestamp": parse_dt(execution["timestamp"]),
                    "fill_price": safe_float(execution["avg_fill_price"]),
                    "slippage_applied": safe_float(execution["slippage_applied"]),
                    "fees_applied": safe_float(execution["fees_applied"]),
                    "total_cost": safe_float(execution["total_cost"]),
                    "status": execution["status"],
                    "requested_amount_usd": safe_float(execution["requested_amount_usd"]),
                    "filled_quantity": safe_float(execution["filled_quantity"]),
                }
            )
        risk_cfg = self.arena_config.get("risk", {})
        with self.connect() as conn:
            decision_time = parse_dt(row["timestamp"])
            day_floor = decision_time.strftime("%Y-%m-%d") if decision_time else None
            trades_today = 0
            daily_spend = 0.0
            if day_floor:
                trades_today = conn.execute(
                    "SELECT COUNT(*) AS n FROM executions WHERE strategy_id = ? AND substr(timestamp, 1, 10) = ?",
                    (row["strategy_id"], day_floor),
                ).fetchone()["n"]
                spend_row = conn.execute(
                    "SELECT COALESCE(SUM(total_cost), 0) AS spend FROM executions WHERE strategy_id = ? AND substr(timestamp, 1, 10) = ?",
                    (row["strategy_id"], day_floor),
                ).fetchone()
                daily_spend = safe_float(spend_row["spend"], 0.0) or 0.0
            open_positions = conn.execute(
                "SELECT COUNT(*) AS n FROM positions WHERE strategy_id = ? AND lower(status) = 'open'",
                (row["strategy_id"],),
            ).fetchone()["n"]
            total_exposure_row = conn.execute(
                "SELECT COALESCE(SUM(quantity * avg_entry_price), 0) AS exposure FROM positions WHERE strategy_id = ? AND lower(status) = 'open'",
                (row["strategy_id"],),
            ).fetchone()
            total_exposure = safe_float(total_exposure_row["exposure"], 0.0) or 0.0
        detail = {
            "decision_id": row["decision_id"],
            "strategy_id": row["strategy_id"],
            "strategy_name": meta.name if meta else row["strategy_id"],
            "timestamp": parse_dt(row["timestamp"]),
            "market": {
                "market_id": primary_market["market_id"] if primary_market else primary_action.get("market_id"),
                "question": primary_market["question"] if primary_market else None,
                "yes_ask": prices["yes_ask"],
                "no_ask": prices["no_ask"],
                "category": primary_market["category"] if primary_market else None,
                "venue": primary_market["venue"] if primary_market else primary_action.get("venue"),
                "end_time": parse_dt(primary_market["end_time"]) if primary_market else None,
            },
            "probability": {
                "predicted_probability": predicted_probability,
                "ensemble": ensemble_data,
                "intraday": bool(ensemble_data and primary_market and parse_dt(primary_market["end_time"]) and parse_dt(primary_market["end_time"]).date() == parse_dt(row["timestamp"]).date()) if parse_dt(row["timestamp"]) else False,
            },
            "edge": {
                "raw_edge_bps": raw_edge_bps,
                "raw_edge_pct": raw_edge_bps / 100.0 if raw_edge_bps is not None else None,
                "capped_edge_bps": safe_float(row["expected_edge_bps"]),
                "side": f"BUY {side_for_edge}" if primary_action else "NO ACTION",
                "fee_rate": fee_rate,
                "edge_after_fees_pct": ((raw_edge_bps / 100.0) - fee_rate * 100.0) if raw_edge_bps is not None else None,
            },
            "position_sizing": position_sizing,
            "risk_check": {
                "daily_trades": trades_today,
                "max_daily_trades": int(risk_cfg.get("max_daily_trades", 0) or 0),
                "daily_spend": daily_spend,
                "max_daily_loss_usd": safe_float(risk_cfg.get("max_daily_loss_usd"), 0.0) or 0.0,
                "open_positions": open_positions,
                "max_open_positions": int(risk_cfg.get("max_open_positions", 0) or 0),
                "per_market_exposure": safe_float(primary_action.get("amount_usd"), 0.0) or 0.0,
                "max_exposure_per_market_usd": safe_float(risk_cfg.get("max_exposure_per_market_usd"), 0.0) or 0.0,
                "total_exposure": total_exposure,
                "max_total_exposure_usd": safe_float(risk_cfg.get("max_total_exposure_usd"), 0.0) or 0.0,
                "result": "APPROVED" if executions else ("NO ACTION" if not primary_action else "PENDING/REJECTED"),
            },
            "evidence": evidence_blocks,
            "thinking": str(row["thinking"] or "").strip(),
            "searches": searches,
            "executions": exec_items,
            "no_action_reason": row["no_action_reason"],
            "llm_model_used": row["llm_model_used"],
        }
        return detail

    def get_strategy_performance(self) -> dict[str, Any]:
        with self.connect() as conn:
            sections = get_strategy_performance(conn)
        comparison_rows = [
            {
                "label": "Enabled",
                "values": [
                    {"text": item["status_label"], "tone": item["status_tone"]}
                    for item in sections
                ],
            },
            {
                "label": "Trade Enabled",
                "values": [
                    {"text": item["trade_enabled_label"], "tone": "positive" if item["trade_enabled"] else "warning"}
                    for item in sections
                ],
            },
            {
                "label": "Trades (7d)",
                "values": [{"text": str(item["trades_7d"]), "tone": None} for item in sections],
            },
            {
                "label": "Win Rate",
                "values": [{"text": f"{item['win_rate'] * 100:.0f}%" if item["win_rate"] is not None else "-", "tone": None} for item in sections],
            },
            {
                "label": "Total P&L",
                "values": [{"text": f"${item['total_pnl']:,.2f}", "tone": "positive" if item["total_pnl"] > 0 else ("negative" if item["total_pnl"] < 0 else None)} for item in sections],
            },
            {
                "label": "Sharpe",
                "values": [{"text": f"{item['sharpe']:.2f}" if item["sharpe"] is not None else "-", "tone": None} for item in sections],
            },
            {
                "label": "Avg Edge (bps)",
                "values": [{"text": f"{item['avg_edge_bps']:.0f}" if item["avg_edge_bps"] is not None else "-", "tone": None} for item in sections],
            },
        ]
        return {"sections": sections, "comparison_rows": comparison_rows}

    def get_forecast_accuracy(self) -> dict[str, Any]:
        with self.connect() as conn:
            actual_rows = conn.execute(
                """
                SELECT location, source,
                       COUNT(*) AS forecasts,
                       SUM(CASE WHEN actual_high_c IS NOT NULL THEN 1 ELSE 0 END) AS with_actuals,
                       AVG(CASE WHEN actual_high_c IS NOT NULL THEN predicted_high_c - actual_high_c END) AS bias_c,
                       AVG(CASE WHEN actual_high_c IS NOT NULL THEN (predicted_high_c - actual_high_c) * (predicted_high_c - actual_high_c) END) AS mse
                FROM forecast_history
                GROUP BY location, source
                ORDER BY location, source
                """
            ).fetchall()
            obs_rows = conn.execute(
                """
                SELECT location,
                       COUNT(*) AS observations_24h,
                       MAX(observation_time) AS latest_observation_time
                FROM station_observations
                WHERE created_at >= datetime('now', '-1 day')
                GROUP BY location
                ORDER BY location
                """
            ).fetchall()
            latest_obs = conn.execute(
                """
                SELECT so.*
                FROM station_observations so
                JOIN (
                    SELECT location, MAX(observation_time) AS latest
                    FROM station_observations
                    GROUP BY location
                ) latest
                  ON latest.location = so.location
                 AND latest.latest = so.observation_time
                ORDER BY so.location
                """
            ).fetchall()
            recent_forecasts = conn.execute(
                """
                SELECT fh.*
                FROM forecast_history fh
                JOIN (
                    SELECT location, source, MAX(created_at) AS latest
                    FROM forecast_history
                    GROUP BY location, source
                ) latest
                  ON latest.location = fh.location
                 AND latest.source = fh.source
                 AND latest.latest = fh.created_at
                ORDER BY fh.location, fh.source
                """
            ).fetchall()
            ensemble_rows = conn.execute(
                """
                SELECT location, target_date, source, predicted_high_c, actual_high_c
                FROM forecast_history
                WHERE actual_high_c IS NOT NULL
                ORDER BY location, target_date, source
                """
            ).fetchall()
        accuracy_rows = []
        has_actuals = False
        for row in actual_rows:
            with_actuals = int(row["with_actuals"] or 0)
            if with_actuals > 0:
                has_actuals = True
            mse = safe_float(row["mse"])
            accuracy_rows.append(
                {
                    "location": row["location"],
                    "source": row["source"],
                    "forecasts": int(row["forecasts"] or 0),
                    "with_actuals": with_actuals,
                    "bias_c": safe_float(row["bias_c"]),
                    "rmse": math.sqrt(mse) if mse is not None else None,
                }
            )
        grouped: dict[str, dict[str, list[float] | str]] = {}
        best_source_by_location: dict[str, tuple[str, float]] = {}
        for row in ensemble_rows:
            location = str(row["location"])
            actual_high = safe_float(row["actual_high_c"])
            predicted_high = safe_float(row["predicted_high_c"])
            if actual_high is None or predicted_high is None:
                continue
            grouped.setdefault(location, {}).setdefault(str(row["target_date"]), [])
            grouped[location][str(row["target_date"])].append(predicted_high - actual_high)
        for row in accuracy_rows:
            if row["with_actuals"] <= 0 or row["rmse"] is None:
                continue
            current = best_source_by_location.get(row["location"])
            if current is None or row["rmse"] < current[1]:
                best_source_by_location[row["location"]] = (row["source"], row["rmse"])
        ensemble_perf = []
        for location, by_date in grouped.items():
            errors = [statistics.mean(errs) for errs in by_date.values() if errs]
            mse = statistics.mean([err * err for err in errors]) if errors else None
            ensemble_perf.append(
                {
                    "location": location,
                    "ensemble_rmse": math.sqrt(mse) if mse is not None else None,
                    "best_single_source": best_source_by_location.get(location, (None, None))[0],
                    "sigma_used": (self.sigma_reference.get("per_location", {}).get(location, {}) or {}).get(
                        "sigma_mult",
                        self.sigma_reference.get("best_sigma_multiplier"),
                    ),
                }
            )
        latest_obs_map = {str(row["location"]): row for row in latest_obs}
        obs_coverage = []
        for row in obs_rows:
            latest = latest_obs_map.get(str(row["location"]))
            obs_coverage.append(
                {
                    "location": row["location"],
                    "observations_24h": int(row["observations_24h"] or 0),
                    "latest_temp_c": safe_float(latest["temperature_c"]) if latest else None,
                    "max_today_c": safe_float(latest["temperature_c"]) if latest else None,
                    "trend": latest["trending"] if latest else None,
                    "latest_time": parse_dt(latest["observation_time"]) if latest else None,
                }
            )
        disagreement_map: dict[str, dict[str, float | None]] = {}
        for row in recent_forecasts:
            location = str(row["location"])
            disagreement_map.setdefault(location, {})[str(row["source"])] = safe_float(row["predicted_high_c"])
        disagreement_rows = []
        for location, source_values in disagreement_map.items():
            vals = [value for value in source_values.values() if value is not None]
            disagreement_rows.append(
                {
                    "location": location,
                    "open_meteo": source_values.get("open_meteo"),
                    "ecmwf": source_values.get("ecmwf") or source_values.get("ecmwf_ifs025"),
                    "gfs": source_values.get("gfs") or source_values.get("gfs_seamless"),
                    "hko": source_values.get("hko"),
                    "spread": (max(vals) - min(vals)) if len(vals) >= 2 else None,
                }
            )
        return {
            "has_actuals": has_actuals,
            "accuracy_rows": accuracy_rows,
            "ensemble_rows": sorted(ensemble_perf, key=lambda item: item["location"]),
            "observation_rows": sorted(obs_coverage, key=lambda item: item["location"]),
            "disagreement_rows": sorted(disagreement_rows, key=lambda item: item["location"]),
        }

    def get_calibration(self) -> dict[str, Any]:
        with self.connect() as conn:
            city_rows = get_city_calibration(conn, CRPS_HISTORY_PATH, BRIER_HISTORY_PATH)
            adjustment_rows = conn.execute(
                "SELECT * FROM parameter_adjustments ORDER BY created_at DESC LIMIT 20"
            ).fetchall() if table_exists(conn, "parameter_adjustments") else []
        return {
            "city_rows": city_rows,
            "adjustment_rows": adjustment_rows,
            "data_available": bool(city_rows),
        }

    def get_execution_funnel_page(self, strategy: str = "all", hours: int = 24) -> dict[str, Any]:
        with self.connect() as conn:
            return get_execution_funnel(conn, hours=hours, strategy=strategy)

    def get_orders_page(self) -> dict[str, Any]:
        with self.connect() as conn:
            return {
                "available": table_exists(conn, "limit_orders"),
                "stats": get_order_stats(conn),
                "open_orders": get_open_limit_orders(conn),
                "history": get_order_history(conn),
            }

    def get_discovery_page(self) -> dict[str, Any]:
        with self.connect() as conn:
            return {
                "available": table_exists(conn, "discovery_alerts"),
                "stats": get_discovery_stats(conn),
                "signals": get_discovery_signals(conn),
            }

    def get_research_costs_page(self) -> dict[str, Any]:
        with self.connect() as conn:
            return get_research_costs(conn)

    def get_system_health(self) -> dict[str, Any]:
        with self.connect() as conn:
            last_decision = self._latest_decision_time(conn)
            last_scan = self._latest_market_scan(conn)
            last_settlement = self._latest_resolution_time(conn)
            error_rows = conn.execute(
                """
                SELECT * FROM events
                WHERE lower(event_type) LIKE '%error%' OR lower(event_type) LIKE '%fail%'
                ORDER BY created_at DESC
                LIMIT 20
                """
            ).fetchall()
            table_names = [str(row["name"]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
            db_stats = []
            for table in table_names:
                count = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
                cols = [str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")]
                last_updated = None
                for column in TIMESTAMP_COLUMNS:
                    if column in cols:
                        row = conn.execute(f"SELECT MAX({column}) AS latest FROM {table}").fetchone()
                        last_updated = parse_dt(row["latest"]) if row else None
                        if last_updated:
                            break
                db_stats.append({"table": table, "rows": count, "last_updated": last_updated})
            response_events = conn.execute(
                "SELECT * FROM events WHERE event_type = 'llm_response_served' ORDER BY created_at DESC"
            ).fetchall()
            llm_news_rows = conn.execute(
                "SELECT * FROM decisions WHERE strategy_id = 'llm_news_trader' ORDER BY timestamp DESC"
            ).fetchall()
        pid_info = self._pid_info()
        last_log_line = ""
        burnin_log = LOG_DIR / "burnin.log"
        if burnin_log.exists():
            lines = burnin_log.read_text(encoding="utf-8", errors="ignore").splitlines()
            last_log_line = lines[-1] if lines else ""
            log_tail = lines[-50:]
        else:
            log_tail = []
        provider_times = {
            "MiniMax direct": None,
            "Gemini 3 Flash": None,
            "NVIDIA NIM": None,
            "Nexus Research": None,
        }
        for row in response_events:
            payload = parse_json(row["payload_json"], {})
            served_by = str(payload.get("served_by", ""))
            created_at = parse_dt(row["created_at"])
            if provider_times["MiniMax direct"] is None and "minimax" in served_by.lower():
                provider_times["MiniMax direct"] = created_at
            if provider_times["Gemini 3 Flash"] is None and "gemini" in served_by.lower():
                provider_times["Gemini 3 Flash"] = created_at
            if provider_times["NVIDIA NIM"] is None and "nvidia" in served_by.lower():
                provider_times["NVIDIA NIM"] = created_at
        for row in llm_news_rows:
            evidence = parse_json(row["evidence_items_json"], [])
            if any(str(item.get("source", "")).lower() not in {"market data"} for item in evidence):
                provider_times["Nexus Research"] = parse_dt(row["timestamp"])
                break
        return {
            "process": {
                "pid_info": pid_info,
                "last_log_line": last_log_line,
                "last_decision_at": last_decision,
                "last_scan_at": last_scan,
                "last_settlement_at": last_settlement,
            },
            "errors": [
                {
                    "time": parse_dt(row["created_at"]),
                    "strategy_id": row["strategy_id"],
                    "event_type": row["event_type"],
                    "payload": parse_json(row["payload_json"], {}),
                }
                for row in error_rows
            ],
            "db_stats": db_stats,
            "provider_status": provider_times,
            "log_tail": log_tail,
        }
