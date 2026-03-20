from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from arena.config import load_app_config  # noqa: E402
from arena.env import load_local_env  # noqa: E402


REPORT_DIR = ROOT / "data"
CRPS_PATH = REPORT_DIR / "crps_history.jsonl"


@dataclass(slots=True)
class ReportContext:
    cutoff: datetime
    city_filter: str | None
    db_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily burn-in report for calibration and execution quality.")
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days (default: 7).")
    parser.add_argument("--city", type=str, default=None, help="Optional city filter.")
    return parser.parse_args()


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        try:
            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def load_crps_entries(ctx: ReportContext) -> list[dict[str, Any]]:
    if not CRPS_PATH.exists():
        return []
    entries: list[dict[str, Any]] = []
    for raw_line in CRPS_PATH.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip():
            continue
        try:
            entry = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        ts = parse_dt(str(entry.get("timestamp") or ""))
        if ts is None or ts < ctx.cutoff:
            continue
        if ctx.city_filter and str(entry.get("city", "")).lower() != ctx.city_filter.lower():
            continue
        entries.append(entry)
    return entries


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def load_market_questions(conn: sqlite3.Connection) -> dict[tuple[str, str], str]:
    rows = conn.execute("SELECT market_id, venue, question FROM markets").fetchall()
    return {(str(row["market_id"]), str(row["venue"])): str(row["question"] or "") for row in rows}


def load_event_rows(conn: sqlite3.Connection, event_type: str, cutoff: datetime) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT created_at, event_type, strategy_id, payload_json "
        "FROM events WHERE event_type = ? AND created_at >= ? ORDER BY created_at DESC",
        (event_type, cutoff.strftime("%Y-%m-%d %H:%M:%S")),
    ).fetchall()


def load_spread_filter_events(ctx: ReportContext) -> list[dict[str, Any]]:
    with connect_db(ctx.db_path) as conn:
        market_questions = load_market_questions(conn)
        rows = load_event_rows(conn, "spread_filter_check", ctx.cutoff)

    events: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except json.JSONDecodeError:
            continue
        market_id = str(payload.get("market_id", ""))
        venue = str(payload.get("venue", ""))
        question = market_questions.get((market_id, venue), "")
        if ctx.city_filter and ctx.city_filter.lower() not in question.lower():
            continue
        spread = payload.get("spread_filter") or {}
        if not isinstance(spread, dict):
            continue
        events.append(
            {
                "created_at": str(row["created_at"]),
                "decision_id": payload.get("decision_id"),
                "strategy_id": payload.get("strategy_id"),
                "market_id": market_id,
                "venue": venue,
                "question": question,
                "spread_filter": spread,
            }
        )
    return events


def load_cross_platform_events(ctx: ReportContext) -> list[dict[str, Any]]:
    with connect_db(ctx.db_path) as conn:
        rows = load_event_rows(conn, "cross_platform_price_comparison", ctx.cutoff)

    events: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except json.JSONDecodeError:
            continue
        comparison = payload.get("comparison") or {}
        if not isinstance(comparison, dict):
            continue
        city = str(comparison.get("city") or "")
        if ctx.city_filter and city.lower() != ctx.city_filter.lower():
            continue
        events.append(
            {
                "created_at": str(row["created_at"]),
                "comparison": comparison,
            }
        )
    return events


def classify_rejection(reason: str) -> str:
    lowered = reason.lower()
    if "spread" in lowered:
        return "wide spread"
    if "net edge" in lowered:
        return "low edge"
    if "volume" in lowered:
        return "low volume"
    return "other"


def format_float(value: float | None, digits: int = 3, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}{suffix}"


def format_pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "n/a"
    return f"{(100.0 * numerator / denominator):.1f}%"


def build_crps_section(entries: list[dict[str, Any]], days: int) -> list[str]:
    lines = [f"SECTION 1: CRPS CALIBRATION BY CITY (last {days} days)"]
    if not entries:
        lines.append("  No resolved markets with CRPS history in this window yet.")
        return lines

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        grouped[str(entry.get("city") or "unknown")].append(entry)

    for city in sorted(grouped):
        city_entries = grouped[city]
        mean_crps = statistics.mean(float(item["crps"]) for item in city_entries)
        mean_ratio = statistics.mean(float(item["calibration_ratio"]) for item in city_entries)
        mean_grad_sigma = statistics.mean(float(item["grad_sigma"]) for item in city_entries)
        worst_thresholds = [float(item["worst_threshold"]) for item in city_entries if item.get("worst_threshold") is not None]
        threshold_low = min(worst_thresholds) if worst_thresholds else None
        threshold_high = max(worst_thresholds) if worst_thresholds else None

        suggestion = suggest_sigma_adjustment(city_entries)
        direction = suggestion.get("direction", suggestion.get("status", "n/a"))
        threshold_range = (
            f"{threshold_low:.1f}F to {threshold_high:.1f}F"
            if threshold_low is not None and threshold_high is not None
            else "n/a"
        )

        lines.append(
            f"  {city}: resolved={len(city_entries)} "
            f"mean_crps={mean_crps:.4f} "
            f"mean_ratio={mean_ratio:.3f} "
            f"mean_grad_sigma={mean_grad_sigma:.4f} "
            f"sigma={direction} "
            f"worst_thresholds={threshold_range}"
        )
    return lines


def suggest_sigma_adjustment(entries: list[dict[str, Any]], last_n: int = 20) -> dict[str, Any]:
    if len(entries) < 5:
        return {"status": "insufficient_data", "count": len(entries)}

    recent = entries[-last_n:]
    avg_grad_sigma = statistics.mean(float(item["grad_sigma"]) for item in recent)
    avg_grad_mu = statistics.mean(float(item["grad_mu"]) for item in recent)
    avg_crps = statistics.mean(float(item["crps"]) for item in recent)
    avg_ratio = statistics.mean(float(item["calibration_ratio"]) for item in recent)

    if avg_grad_sigma > 0.01:
        multiplier = 0.98
        direction = "decrease"
    elif avg_grad_sigma < -0.01:
        multiplier = 1.02
        direction = "increase"
    else:
        multiplier = 1.0
        direction = "hold"

    return {
        "status": "ready",
        "sample_size": len(recent),
        "avg_crps": round(avg_crps, 4),
        "avg_grad_sigma": round(avg_grad_sigma, 4),
        "avg_grad_mu": round(avg_grad_mu, 4),
        "avg_calibration_ratio": round(avg_ratio, 3),
        "direction": direction,
        "suggested_sigma_multiplier": multiplier,
    }


def build_spread_section(events: list[dict[str, Any]], days: int) -> list[str]:
    lines = [f"SECTION 2: SPREAD FILTER SUMMARY (last {days} days)"]
    if not events:
        lines.append("  No spread-filter audit events in this window yet.")
        return lines

    total = len(events)
    passed = [event for event in events if bool(event["spread_filter"].get("pass"))]
    rejected = [event for event in events if not bool(event["spread_filter"].get("pass"))]

    breakdown = Counter(
        classify_rejection(str(event["spread_filter"].get("reason") or ""))
        for event in rejected
    )

    def avg_metric(items: list[dict[str, Any]], key: str) -> float | None:
        values = [
            float(item["spread_filter"][key])
            for item in items
            if item["spread_filter"].get(key) is not None
        ]
        return statistics.mean(values) if values else None

    lines.append(f"  Total trade attempts: {total}")
    lines.append(f"  Passed: {len(passed)} ({format_pct(len(passed), total)})")
    lines.append(f"  Rejected: {len(rejected)} ({format_pct(len(rejected), total)})")
    if rejected:
        parts = [f"{label}={count}" for label, count in sorted(breakdown.items())]
        lines.append(f"  Rejection breakdown: {', '.join(parts)}")
    else:
        lines.append("  Rejection breakdown: none")

    lines.append(
        "  Avg spread: "
        f"passed={format_float(avg_metric(passed, 'spread_cents'), 1, 'c')} "
        f"rejected={format_float(avg_metric(rejected, 'spread_cents'), 1, 'c')}"
    )
    lines.append(
        "  Avg estimated edge on passed trades: "
        f"{format_float(avg_metric(passed, 'estimated_edge_cents'), 1, 'c')}"
    )
    return lines


def build_cross_platform_section(events: list[dict[str, Any]], days: int) -> list[str]:
    lines = [f"SECTION 3: CROSS-PLATFORM COMPARISON (last {days} days, if Kalshi data exists)"]
    if not events:
        lines.append("  No cross-platform comparison events in this window yet.")
        return lines

    yes_diffs: list[float] = []
    arb_count = 0
    for event in events:
        comparison = event["comparison"]
        kalshi_yes = comparison.get("kalshi_yes_ask")
        polymarket_yes = comparison.get("polymarket_yes_ask")
        kalshi_no = comparison.get("kalshi_no_ask")
        polymarket_no = comparison.get("polymarket_no_ask")
        if kalshi_yes is not None and polymarket_yes is not None:
            yes_diff = float(kalshi_yes) - float(polymarket_yes)
            yes_diffs.append(yes_diff)
        else:
            yes_diff = 0.0
        no_diff_abs = 0.0
        if kalshi_no is not None and polymarket_no is not None:
            no_diff_abs = abs(float(kalshi_no) - float(polymarket_no))
        if abs(yes_diff) > 0.03 or no_diff_abs > 0.03:
            arb_count += 1

    avg_yes_diff = statistics.mean(yes_diffs) if yes_diffs else None
    lines.append(f"  Markets with both Polymarket and Kalshi prices: {len(events)}")
    lines.append(
        "  Average YES price difference (Kalshi - Polymarket): "
        f"{format_float(avg_yes_diff * 100 if avg_yes_diff is not None else None, 2, 'c')}"
    )
    lines.append(f"  Potential arbitrage opportunities (>3c divergence): {arb_count}")
    return lines


def render_report(days: int, city_filter: str | None, crps_entries: list[dict[str, Any]], spread_events: list[dict[str, Any]], cross_events: list[dict[str, Any]]) -> str:
    today = date.today().isoformat()
    lines = [
        "ARENA BURN-IN REPORT",
        "====================",
        f"Generated: {today}",
        f"Lookback: last {days} day(s)",
        f"City filter: {city_filter or 'all'}",
        "",
    ]
    lines.extend(build_crps_section(crps_entries, days))
    lines.append("")
    lines.extend(build_spread_section(spread_events, days))
    lines.append("")
    lines.extend(build_cross_platform_section(cross_events, days))
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    load_local_env()
    app_config = load_app_config()

    cutoff = datetime.now(UTC) - timedelta(days=max(args.days, 1))
    ctx = ReportContext(
        cutoff=cutoff,
        city_filter=args.city.strip() if args.city else None,
        db_path=app_config.db_path,
    )

    crps_entries = load_crps_entries(ctx)
    spread_events = load_spread_filter_events(ctx)
    cross_events = load_cross_platform_events(ctx)

    report = render_report(args.days, ctx.city_filter, crps_entries, spread_events, cross_events)
    print(report)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORT_DIR / f"burn_in_report_{date.today().isoformat()}.txt"
    out_path.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
