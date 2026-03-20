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
        return {
            "portfolio_cards": self.get_portfolio_cards(),
            "research_summary": self.get_research_overview(),
            "activity": self.get_recent_activity(),
            "edge_rows": self.get_edge_distribution(),
            "weather_rows": self.get_weather_monitor(),
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
            portfolios = {
                str(row["strategy_id"]): row for row in conn.execute("SELECT * FROM portfolios").fetchall()
            }
            decisions = conn.execute("SELECT * FROM decisions ORDER BY timestamp DESC").fetchall()
            executions = conn.execute("SELECT * FROM executions ORDER BY timestamp DESC").fetchall()
            resolutions = {str(row["market_id"]): row for row in conn.execute("SELECT * FROM resolutions").fetchall()}
            market_map = self._market_row_map(conn, [str(row["market_id"]) for row in executions])
            snapshots = conn.execute("SELECT * FROM daily_snapshots ORDER BY snapshot_date ASC").fetchall()
        decisions_by_strategy: dict[str, list[sqlite3.Row]] = {}
        for row in decisions:
            decisions_by_strategy.setdefault(str(row["strategy_id"]), []).append(row)
        executions_by_strategy: dict[str, list[sqlite3.Row]] = {}
        for row in executions:
            executions_by_strategy.setdefault(str(row["strategy_id"]), []).append(row)
        snapshots_by_strategy: dict[str, list[sqlite3.Row]] = {}
        for row in snapshots:
            snapshots_by_strategy.setdefault(str(row["strategy_id"]), []).append(row)

        sections = []
        for meta in self.active_strategies:
            portfolio = portfolios.get(meta.strategy_id)
            strategy_decisions = decisions_by_strategy.get(meta.strategy_id, [])
            strategy_execs = executions_by_strategy.get(meta.strategy_id, [])
            avg_edge = statistics.mean([int(row["expected_edge_bps"] or 0) for row in strategy_decisions if row["expected_edge_bps"] is not None]) if strategy_decisions else None
            avg_position_size = statistics.mean([safe_float(row["requested_amount_usd"], 0.0) or 0.0 for row in strategy_execs]) if strategy_execs else None
            win_rate = None
            total_trades = int(portfolio["total_trades"]) if portfolio else 0
            if portfolio and total_trades:
                win_rate = int(portfolio["winning_trades"]) / total_trades
            trade_rows = []
            for execution in strategy_execs[:20]:
                market = market_map.get(str(execution["market_id"]))
                resolution = resolutions.get(str(execution["market_id"]))
                exit_label = resolution["winning_outcome_label"] if resolution else "Open"
                pnl = None
                if resolution:
                    won = exit_label.lower() == str(market["question"]).lower()
                duration = None
                exec_time = parse_dt(execution["timestamp"])
                resolved_time = parse_dt(resolution["resolved_at"]) if resolution else None
                if exec_time and resolved_time:
                    duration = relative_time(exec_time, now=resolved_time)
                trade_rows.append(
                    {
                        "timestamp": exec_time,
                        "question": market["question"] if market else execution["market_id"],
                        "side": execution["action_type"],
                        "entry": safe_float(execution["avg_fill_price"]),
                        "exit": None,
                        "pnl": pnl,
                        "edge_bps": next((safe_float(dec["expected_edge_bps"]) for dec in strategy_decisions if dec["decision_id"] == execution["decision_id"]), None),
                        "duration": duration or "Open",
                    }
                )
            daily_series = []
            snap_rows = snapshots_by_strategy.get(meta.strategy_id, [])
            if snap_rows:
                values = []
                last_value = None
                for snap in snap_rows:
                    portfolio_value = safe_float(snap["portfolio_value"], 0.0) or 0.0
                    daily_pnl = portfolio_value - last_value if last_value is not None else portfolio_value - (meta.starting_balance or 10000.0)
                    values.append(daily_pnl)
                    daily_series.append({"date": snap["snapshot_date"], "pnl": daily_pnl})
                    last_value = portfolio_value
                sharpe = None
                if len(values) >= 2 and statistics.pstdev(values) > 0:
                    sharpe = statistics.mean(values) / statistics.pstdev(values)
            else:
                sharpe = None
            sections.append(
                {
                    "strategy_id": meta.strategy_id,
                    "name": meta.name,
                    "status": "active" if meta.enabled else "disabled",
                    "metrics": {
                        "total_trades": total_trades,
                        "win_rate": win_rate,
                        "avg_edge_bps": avg_edge,
                        "total_pnl": (safe_float(portfolio["realized_pnl"], 0.0) + safe_float(portfolio["unrealized_pnl"], 0.0)) if portfolio else 0.0,
                        "avg_position_size": avg_position_size,
                        "max_drawdown": safe_float(portfolio["max_drawdown"]) if portfolio else None,
                        "sharpe_estimate": sharpe,
                    },
                    "trades": trade_rows,
                    "daily_series": daily_series,
                }
            )
        return {"sections": sections}

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
            strategy_health = conn.execute(
                "SELECT * FROM strategy_health ORDER BY computed_at DESC"
            ).fetchall()
            decision_scores = conn.execute(
                "SELECT * FROM decision_scores ORDER BY created_at DESC"
            ).fetchall()
            adjustments = conn.execute(
                "SELECT * FROM parameter_adjustments WHERE auto_applied = 0 ORDER BY created_at DESC"
            ).fetchall()
        bins: dict[str, dict[str, float]] = {}
        for row in decision_scores:
            prob = safe_float(row["predicted_probability"])
            actual = safe_float(row["actual_outcome"])
            if prob is None or actual is None:
                continue
            bucket_start = math.floor(prob * 10) / 10
            bucket_end = bucket_start + 0.1
            label = f"{bucket_start:.1f}-{min(bucket_end, 1.0):.1f}"
            entry = bins.setdefault(label, {"pred_sum": 0.0, "actual_sum": 0.0, "count": 0})
            entry["pred_sum"] += prob
            entry["actual_sum"] += actual
            entry["count"] += 1
        calibration_curve = []
        for label, bucket in sorted(bins.items()):
            count = int(bucket["count"])
            mean_pred = bucket["pred_sum"] / count
            mean_actual = bucket["actual_sum"] / count
            delta = abs(mean_pred - mean_actual)
            status = "good" if delta < 0.05 else "warn" if delta < 0.10 else "bad"
            calibration_curve.append(
                {
                    "label": label,
                    "mean_pred": mean_pred,
                    "mean_actual": mean_actual,
                    "count": count,
                    "status": status,
                }
            )
        return {
            "strategy_health": strategy_health,
            "calibration_curve": calibration_curve,
            "adjustments": adjustments,
            "sigma_reference": self.sigma_reference,
        }

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
