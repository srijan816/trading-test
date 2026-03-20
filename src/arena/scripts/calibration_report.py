"""Calibration report: prints strategy health, calibration curve, forecast bias, and pending adjustments."""
from __future__ import annotations

import statistics
import sys
from datetime import date
from pathlib import Path

from arena.config import load_app_config
from arena.db import ArenaDB


def _safe_rows(conn, query, params=()):
    try:
        return list(conn.execute(query, params))
    except Exception:
        return []


def print_report(db: ArenaDB) -> None:
    print(f"ARENA CALIBRATION REPORT")
    print(f"========================")
    print(f"Generated: {date.today()}")
    print()

    with db.connect() as conn:
        health_rows = _safe_rows(
            conn,
            "SELECT * FROM strategy_health ORDER BY computed_at DESC",
        )
        score_rows = _safe_rows(
            conn,
            "SELECT * FROM decision_scores ORDER BY created_at DESC LIMIT 200",
        )
        bias_rows = _safe_rows(
            conn,
            "SELECT source, COUNT(*) as cnt, "
            "AVG(error_high_c) as avg_bias, "
            "SUM(CASE WHEN actual_high_c IS NOT NULL THEN 1 ELSE 0 END) as resolved "
            "FROM forecast_history GROUP BY source",
        )
        adj_rows = _safe_rows(
            conn,
            "SELECT * FROM parameter_adjustments WHERE auto_applied = 0 ORDER BY created_at DESC",
        )

    # Strategy Health
    print("STRATEGY HEALTH")
    if not health_rows:
        print("  No data yet — markets need to resolve for scoring to begin.")
    else:
        seen: set[str] = set()
        print(f"{'Strategy':<20} {'Trades':>7} {'Brier':>7} {'Cal.Err':>8} {'Temp Err':>10} {'Overconfident':>14}")
        print("-" * 68)
        for row in health_rows:
            sid = row["strategy_id"]
            if sid in seen:
                continue
            seen.add(sid)
            temp_err = f"{row['mean_forecast_error_c']:+.1f}C" if row["mean_forecast_error_c"] is not None else "N/A"
            overconf = f"{row['overconfidence_rate']:.0%}" if row["overconfidence_rate"] is not None else "N/A"
            print(
                f"{sid:<20} {row['sample_size']:>7} {row['rolling_brier']:>7.3f} "
                f"{row['calibration_error']:>8.3f} {temp_err:>10} {overconf:>14}"
            )
    print()

    # Calibration Curve
    if score_rows:
        strategies = set(r["strategy_id"] for r in score_rows)
        for strategy_id in sorted(strategies):
            strat_scores = [r for r in score_rows if r["strategy_id"] == strategy_id]
            print(f"CALIBRATION CURVE ({strategy_id})")
            buckets = {"0.0-0.2": ([], []), "0.2-0.4": ([], []), "0.4-0.6": ([], []), "0.6-0.8": ([], []), "0.8-1.0": ([], [])}
            for r in strat_scores:
                p = float(r["predicted_probability"])
                if p < 0.2:
                    key = "0.0-0.2"
                elif p < 0.4:
                    key = "0.2-0.4"
                elif p < 0.6:
                    key = "0.4-0.6"
                elif p < 0.8:
                    key = "0.6-0.8"
                else:
                    key = "0.8-1.0"
                buckets[key][0].append(p)
                buckets[key][1].append(float(r["actual_outcome"]))
            print(f"{'Predicted':<12} {'Actual':>8} {'Count':>7} {'Status':>20}")
            for bname, (preds, acts) in buckets.items():
                if not preds:
                    print(f"{bname:<12} {'N/A':>8} {0:>7}")
                    continue
                mean_act = statistics.mean(acts)
                mean_pred = statistics.mean(preds)
                deviation = abs(mean_pred - mean_act)
                if deviation < 0.1:
                    status = "calibrated"
                elif deviation < 0.15:
                    status = "~ slightly off"
                else:
                    status = "! miscalibrated"
                print(f"{bname:<12} {mean_act:>8.2f} {len(preds):>7} {status:>20}")
            print()
    else:
        print("CALIBRATION CURVE")
        print("  No scored decisions yet.")
        print()

    # Forecast Bias by Source
    print("FORECAST BIAS BY SOURCE")
    if not bias_rows:
        print("  No forecast history yet.")
    else:
        print(f"{'Source':<15} {'Samples':>8} {'Bias (C)':>10} {'Reliable?':>12}")
        print("-" * 47)
        for row in bias_rows:
            cnt = row["resolved"] or 0
            if cnt >= 5 and row["avg_bias"] is not None:
                bias_str = f"{row['avg_bias']:+.2f}"
                reliable = "yes"
            else:
                bias_str = "N/A"
                reliable = f"no ({cnt} pts)"
            print(f"{row['source']:<15} {row['cnt']:>8} {bias_str:>10} {reliable:>12}")
    print()

    # Pending Adjustments
    print("PENDING ADJUSTMENTS")
    if not adj_rows:
        print("  None")
    else:
        for row in adj_rows:
            print(f"  - {row['parameter_name']}: {row['current_value']} -> {row['recommended_value']} ({row['reason']})")
    print()

    # Recent Decision Accuracy
    print("RECENT DECISION ACCURACY (last 10)")
    recent = score_rows[:10] if score_rows else []
    if not recent:
        print("  No scored decisions yet.")
    else:
        print(f"{'Decision':<25} {'Market':<20} {'Predicted':>10} {'Actual':>8} {'Brier':>7}")
        print("-" * 72)
        for r in recent:
            dec_id = str(r["decision_id"])[:22] + "..."
            mkt_id = str(r["market_id"])[:17] + "..."
            print(f"{dec_id:<25} {mkt_id:<20} {r['predicted_probability']:>10.3f} {r['actual_outcome']:>8.1f} {r['brier_score']:>7.3f}")
    print()


def main():
    from arena.env import load_local_env
    load_local_env()
    app_config = load_app_config()
    db = ArenaDB(app_config.db_path)
    db.initialize()
    print_report(db)


if __name__ == "__main__":
    main()
