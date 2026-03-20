from __future__ import annotations

import statistics
from collections import defaultdict

from arena.db import ArenaDB
from arena.models import CalibrationRow


def build_calibration_rows(decision_rows: list[dict], execution_rows: list[dict]) -> list[CalibrationRow]:
    exec_by_decision: dict[str, list[dict]] = defaultdict(list)
    for row in execution_rows:
        if row.get("status") in ("filled", "partial"):
            exec_by_decision[row["decision_id"]].append(row)

    buckets: dict[tuple[str, str], list[float]] = defaultdict(list)
    wins: dict[tuple[str, str], int] = defaultdict(int)
    totals: dict[tuple[str, str], int] = defaultdict(int)

    for row in decision_rows:
        confidence = row.get("confidence")
        if confidence is None:
            continue
        bucket = confidence_bucket(confidence)
        key = (row["strategy_id"], bucket)
        buckets[key].append(float(confidence))
        execs = exec_by_decision.get(row["decision_id"], [])
        for ex in execs:
            won = ex.get("won")
            if won is not None:
                totals[key] += 1
                if won:
                    wins[key] += 1

    output: list[CalibrationRow] = []
    for (strategy_id, bucket), confidences in buckets.items():
        key = (strategy_id, bucket)
        total = totals.get(key, 0)
        win_rate = wins.get(key, 0) / total if total > 0 else 0.0
        output.append(
            CalibrationRow(
                strategy_id=strategy_id,
                bucket=bucket,
                predictions=len(confidences),
                win_rate=round(win_rate, 4),
                avg_confidence=sum(confidences) / len(confidences),
            )
        )
    return output


def confidence_bucket(value: float) -> str:
    if value < 0.5:
        return "0-50%"
    if value < 0.7:
        return "50-70%"
    if value < 0.85:
        return "70-85%"
    return "85-100%"


def get_strategy_health(db: ArenaDB, strategy_id: str | None = None) -> list[dict]:
    query = "SELECT * FROM strategy_health"
    params: list = []
    if strategy_id:
        query += " WHERE strategy_id = ?"
        params.append(strategy_id)
    query += " ORDER BY computed_at DESC"
    try:
        with db.connect() as conn:
            return [dict(row) for row in conn.execute(query, params)]
    except Exception:
        return []


def get_decision_scores(db: ArenaDB, strategy_id: str | None = None, limit: int = 50) -> list[dict]:
    query = "SELECT * FROM decision_scores"
    params: list = []
    if strategy_id:
        query += " WHERE strategy_id = ?"
        params.append(strategy_id)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    try:
        with db.connect() as conn:
            return [dict(row) for row in conn.execute(query, params)]
    except Exception:
        return []


def get_calibration_summary(db: ArenaDB, strategy_id: str) -> dict:
    scores = get_decision_scores(db, strategy_id, limit=50)
    if not scores:
        return {
            "strategy_id": strategy_id,
            "sample_size": 0,
            "rolling_brier": None,
            "calibration_error": None,
            "mean_forecast_error_c": None,
            "overconfidence_rate": None,
        }
    brier_scores = [float(s["brier_score"]) for s in scores]
    forecast_errors = [float(s["forecast_error_c"]) for s in scores if s["forecast_error_c"] is not None]
    predictions = [float(s["predicted_probability"]) for s in scores]
    actuals = [float(s["actual_outcome"]) for s in scores]

    overconfident = 0
    overconfident_total = 0
    for p, a in zip(predictions, actuals):
        if p > 0.7 or p < 0.3:
            overconfident_total += 1
            if (p > 0.7 and a == 0.0) or (p < 0.3 and a == 1.0):
                overconfident += 1

    return {
        "strategy_id": strategy_id,
        "sample_size": len(scores),
        "rolling_brier": round(statistics.mean(brier_scores), 4),
        "calibration_error": _calibration_error(predictions, actuals),
        "mean_forecast_error_c": round(statistics.mean(forecast_errors), 3) if forecast_errors else None,
        "overconfidence_rate": round(overconfident / overconfident_total, 4) if overconfident_total else None,
    }


def _calibration_error(predictions: list[float], actuals: list[float]) -> float:
    buckets: dict[str, tuple[list[float], list[float]]] = {
        "0-0.2": ([], []), "0.2-0.4": ([], []),
        "0.4-0.6": ([], []), "0.6-0.8": ([], []), "0.8-1.0": ([], []),
    }
    for p, a in zip(predictions, actuals):
        if p < 0.2:
            key = "0-0.2"
        elif p < 0.4:
            key = "0.2-0.4"
        elif p < 0.6:
            key = "0.4-0.6"
        elif p < 0.8:
            key = "0.6-0.8"
        else:
            key = "0.8-1.0"
        buckets[key][0].append(p)
        buckets[key][1].append(a)
    max_dev = 0.0
    for preds, acts in buckets.values():
        if preds:
            max_dev = max(max_dev, abs(statistics.mean(preds) - statistics.mean(acts)))
    return round(max_dev, 4)
