from __future__ import annotations

import json
import logging
import math
import statistics
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import httpx

from arena.env import load_local_env

logger = logging.getLogger(__name__)

HISTORICAL_FORECAST_API = "https://historical-forecast-api.open-meteo.com/v1/forecast"
PREVIOUS_RUNS_API = "https://previous-runs-api.open-meteo.com/v1/forecast"
START_DATE = "2026-01-18"
END_DATE = "2026-03-18"
REQUEST_DELAY_SECONDS = 0.5
TIMEOUT = 30.0
MIN_WORKABLE_LOCATIONS = 3
SIGMA_MULTIPLIERS = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.7, 2.0, 2.5, 3.0]
THRESHOLD_OFFSETS = [(-5.0 + 0.5 * step) for step in range(21)]
SOURCE_ORDER = ["best_match", "ecmwf_ifs025", "gfs_seamless"]
METHOD_ORDER = ["A", "B", "C"]
MODEL_PARAMS = {
    "best_match": {},
    "ecmwf_ifs025": {"models": "ecmwf_ifs025"},
    "gfs_seamless": {"models": "gfs_seamless"},
}
LOCATIONS = [
    {"name": "Hong Kong", "lat": 22.30, "lon": 114.17},
    {"name": "Chicago", "lat": 41.88, "lon": -87.63},
    {"name": "London", "lat": 51.51, "lon": -0.13},
    {"name": "Tokyo", "lat": 35.68, "lon": 139.69},
    {"name": "Seoul", "lat": 37.57, "lon": 126.98},
    {"name": "Lucknow", "lat": 26.85, "lon": 80.95},
]


@dataclass(frozen=True)
class ErrorStats:
    mean_error: float
    rmse: float
    std_dev: float
    p05: float
    p95: float
    skewness: float
    count: int


def gaussian_cdf(x: float, mean: float, sigma: float) -> float:
    if sigma <= 0:
        return 1.0 if x >= mean else 0.0
    z = (x - mean) / (sigma * math.sqrt(2.0))
    return 0.5 * (1.0 + math.erf(z))


def clamp_probability(value: float) -> float:
    return max(0.001, min(0.999, value))


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return ordered[lower]
    frac = rank - lower
    return ordered[lower] * (1.0 - frac) + ordered[upper] * frac


def safe_mean(values: list[float]) -> float:
    return statistics.mean(values) if values else float("nan")


def safe_stdev(values: list[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def compute_error_stats(errors: list[float]) -> ErrorStats:
    if not errors:
        return ErrorStats(float("nan"), float("nan"), float("nan"), float("nan"), float("nan"), float("nan"), 0)
    mean_error = safe_mean(errors)
    rmse = math.sqrt(safe_mean([error * error for error in errors]))
    std_dev = safe_stdev(errors)
    if len(errors) > 1 and std_dev > 0:
        centered = [error - mean_error for error in errors]
        skewness = safe_mean([(value / std_dev) ** 3 for value in centered])
    else:
        skewness = 0.0
    return ErrorStats(
        mean_error=mean_error,
        rmse=rmse,
        std_dev=std_dev,
        p05=percentile(errors, 0.05),
        p95=percentile(errors, 0.95),
        skewness=skewness,
        count=len(errors),
    )


def is_suspicious_series(stats: ErrorStats) -> bool:
    return stats.count >= 20 and math.isfinite(stats.rmse) and stats.rmse <= 0.05


def format_float(value: float, digits: int = 2) -> str:
    if value is None or not math.isfinite(value):
        return "N/A"
    return f"{value:.{digits}f}"


def format_temp(value: float) -> str:
    if value is None or not math.isfinite(value):
        return "N/A"
    return f"{value:.1f}C"


def estimate_calibration_label(max_deviation: float) -> str:
    if max_deviation < 0.05:
        return "EXCELLENT"
    if max_deviation < 0.10:
        return "GOOD"
    if max_deviation < 0.15:
        return "FAIR"
    return "POOR"


def build_histogram(errors: list[float]) -> list[tuple[str, int, str]]:
    if not errors:
        return []
    lower_bound = math.floor(min(errors))
    upper_bound = math.ceil(max(errors))
    if lower_bound == upper_bound:
        upper_bound += 1
    bins: list[tuple[str, int, str]] = []
    counts: list[int] = []
    for start in range(lower_bound, upper_bound):
        end = start + 1
        if end == upper_bound:
            count = sum(1 for error in errors if start <= error <= end)
        else:
            count = sum(1 for error in errors if start <= error < end)
        counts.append(count)
    max_count = max(counts) if counts else 0
    width = 24
    for index, start in enumerate(range(lower_bound, upper_bound)):
        end = start + 1
        count = counts[index]
        bar_length = int(round((count / max_count) * width)) if max_count else 0
        bins.append((f"{start:>2} to {end:>2}", count, "#" * bar_length))
    return bins


def get_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def sleep_between_calls() -> None:
    time.sleep(REQUEST_DELAY_SECONDS)


def fetch_json(client: httpx.Client, url: str, params: dict) -> dict:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
            sleep_between_calls()
            return payload
        except Exception as exc:  # pragma: no cover - live API failures are runtime concerns
            last_error = exc
            logger.warning(
                "Request failed (%s/%s) for %s params=%s: %s",
                attempt + 1,
                3,
                url,
                params,
                exc,
            )
            sleep_between_calls()
    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def parse_actuals(payload: dict) -> dict[str, dict[str, float]]:
    daily = payload.get("daily", {})
    dates = daily.get("time", [])
    highs = daily.get("temperature_2m_max", [])
    lows = daily.get("temperature_2m_min", [])
    records: dict[str, dict[str, float]] = {}
    for index, day in enumerate(dates):
        high = highs[index] if index < len(highs) else None
        low = lows[index] if index < len(lows) else None
        if high is None or low is None:
            continue
        records[day] = {"high_c": float(high), "low_c": float(low)}
    return records


def parse_daily_highs(payload: dict) -> dict[str, float]:
    daily = payload.get("daily", {})
    dates = daily.get("time", [])
    highs = daily.get("temperature_2m_max", [])
    records: dict[str, float] = {}
    for index, day in enumerate(dates):
        high = highs[index] if index < len(highs) else None
        if high is None:
            continue
        records[day] = float(high)
    return records


def parse_hourly_highs(payload: dict) -> dict[str, float]:
    hourly = payload.get("hourly", {})
    timestamps = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    grouped: dict[str, list[float]] = {}
    for index, timestamp in enumerate(timestamps):
        value = temps[index] if index < len(temps) else None
        if value is None:
            continue
        day = str(timestamp).split("T", 1)[0]
        grouped.setdefault(day, []).append(float(value))
    return {day: max(values) for day, values in grouped.items() if values}


def merge_forecast_series(day1: dict[str, float], day0: dict[str, float]) -> tuple[dict[str, dict[str, float | str]], dict[str, int]]:
    merged: dict[str, dict[str, float | str]] = {}
    counters = {"day1": 0, "day0_fallback": 0}
    for day in sorted(set(day0) | set(day1)):
        if day in day1:
            merged[day] = {"high_c": day1[day], "lead": "day1"}
            counters["day1"] += 1
        elif day in day0:
            merged[day] = {"high_c": day0[day], "lead": "day0_fallback"}
            counters["day0_fallback"] += 1
    return merged, counters


def collect_errors(
    forecast_series: dict[str, dict[str, float | str]],
    actual_series: dict[str, dict[str, float]],
) -> tuple[list[float], list[str]]:
    errors: list[float] = []
    days: list[str] = []
    for day, forecast in sorted(forecast_series.items()):
        actual = actual_series.get(day)
        if not actual:
            continue
        errors.append(float(forecast["high_c"]) - float(actual["high_c"]))
        days.append(day)
    return errors, days


def summarize_location_actuals(location_name: str, records: dict[str, dict[str, float]]) -> None:
    highs = [record["high_c"] for record in records.values()]
    if not highs:
        print(f"{location_name}: 0 days, no actual highs available")
        return
    print(
        f"{location_name}: {len(records)} days, high range {min(highs):.1f} to {max(highs):.1f}C"
    )


def make_probability_records(
    samples: list[dict],
    ensemble_rmse: float,
    source_rmse_lookup: dict[str, float],
) -> dict[str, list[dict[str, float]]]:
    records_by_method = {method: [] for method in METHOD_ORDER}
    usable_ensemble_rmse = max(ensemble_rmse, 0.1)
    for sample in samples:
        actual_high = sample["actual_high"]
        ensemble_mean = sample["ensemble_mean"]
        ensemble_std = sample["ensemble_std"]
        available_sources = sample["source_forecasts"]
        weighted_components: list[tuple[float, float]] = []
        for source_name, forecast_value in available_sources.items():
            source_rmse = source_rmse_lookup.get(source_name)
            if source_rmse is None or not math.isfinite(source_rmse) or source_rmse <= 0:
                continue
            weight = 1.0 / (source_rmse ** 2)
            weighted_components.append((weight, forecast_value))
        weighted_mean = ensemble_mean
        weighted_sigma_base = usable_ensemble_rmse
        if weighted_components:
            total_weight = sum(weight for weight, _ in weighted_components)
            weighted_mean = sum(weight * forecast for weight, forecast in weighted_components) / total_weight
            weighted_sigma_base = max(1.0 / math.sqrt(total_weight), 0.1)

        for threshold_offset in THRESHOLD_OFFSETS:
            threshold = actual_high + threshold_offset
            actual_outcome = 1.0 if actual_high >= threshold else 0.0
            for sigma_multiplier in SIGMA_MULTIPLIERS:
                fixed_sigma = usable_ensemble_rmse * sigma_multiplier
                dynamic_sigma = max(ensemble_std, 1.0) * sigma_multiplier
                weighted_sigma = weighted_sigma_base * sigma_multiplier
                probabilities = {
                    "A": 1.0 - gaussian_cdf(threshold, ensemble_mean, fixed_sigma),
                    "B": 1.0 - gaussian_cdf(threshold, ensemble_mean, dynamic_sigma),
                    "C": 1.0 - gaussian_cdf(threshold, weighted_mean, weighted_sigma),
                }
                for method, predicted_probability in probabilities.items():
                    records_by_method[method].append(
                        {
                            "sigma_multiplier": sigma_multiplier,
                            "predicted_probability": predicted_probability,
                            "actual_outcome": actual_outcome,
                        }
                    )
    return records_by_method


def score_probability_records(records_by_method: dict[str, list[dict[str, float]]]) -> dict[str, list[dict[str, float]]]:
    scored: dict[str, list[dict[str, float]]] = {}
    for method, records in records_by_method.items():
        buckets: dict[float, list[dict[str, float]]] = {}
        for record in records:
            buckets.setdefault(record["sigma_multiplier"], []).append(record)
        method_rows: list[dict[str, float]] = []
        for sigma_multiplier in SIGMA_MULTIPLIERS:
            sigma_records = buckets.get(sigma_multiplier, [])
            if not sigma_records:
                continue
            brier = safe_mean(
                [
                    (record["predicted_probability"] - record["actual_outcome"]) ** 2
                    for record in sigma_records
                ]
            )
            log_loss = safe_mean(
                [
                    -(
                        record["actual_outcome"] * math.log(clamp_probability(record["predicted_probability"]))
                        + (1.0 - record["actual_outcome"]) * math.log(clamp_probability(1.0 - record["predicted_probability"]))
                    )
                    for record in sigma_records
                ]
            )
            method_rows.append(
                {
                    "sigma_multiplier": sigma_multiplier,
                    "brier": brier,
                    "log_loss": log_loss,
                    "count": float(len(sigma_records)),
                }
            )
        scored[method] = method_rows
    return scored


def best_method_row(scored: dict[str, list[dict[str, float]]]) -> dict[str, float | str]:
    candidates: list[dict[str, float | str]] = []
    for method, rows in scored.items():
        for row in rows:
            candidate = dict(row)
            candidate["method"] = method
            candidates.append(candidate)
    return min(candidates, key=lambda row: (row["brier"], row["log_loss"])) if candidates else {}


def print_sigma_tables(scored: dict[str, list[dict[str, float]]]) -> None:
    titles = {
        "A": "SIGMA CALIBRATION — METHOD A (Fixed RMSE-based sigma)",
        "B": "SIGMA CALIBRATION — METHOD B (Source disagreement sigma)",
        "C": "SIGMA CALIBRATION — METHOD C (RMSE-weighted ensemble sigma)",
    }
    overall_best = best_method_row(scored)
    for method in METHOD_ORDER:
        print()
        print(titles[method])
        print("=" * len(titles[method]))
        print(f"{'σ mult':<8} {'Brier':<8} {'LogLoss':<8} {'Best?':<8}")
        print(f"{'-' * 8} {'-' * 8} {'-' * 8} {'-' * 8}")
        for row in scored.get(method, []):
            marker = ""
            if overall_best and method == overall_best["method"] and row["sigma_multiplier"] == overall_best["sigma_multiplier"]:
                marker = "<-- best"
            elif method == "B" and math.isclose(row["sigma_multiplier"], 1.0):
                marker = "<-- baseline"
            print(
                f"{row['sigma_multiplier']:<8.2f} {row['brier']:<8.3f} {row['log_loss']:<8.3f} {marker:<8}"
            )
    if overall_best:
        print()
        print(
            f"BEST OVERALL: Method {overall_best['method']} with σ multiplier {overall_best['sigma_multiplier']:.2f} "
            f"(Brier: {overall_best['brier']:.3f})"
        )


def compute_calibration_curve(records: list[dict[str, float]]) -> tuple[list[dict[str, float]], float, str]:
    bins: list[list[dict[str, float]]] = [[] for _ in range(10)]
    for record in records:
        probability = record["predicted_probability"]
        index = min(int(probability * 10), 9)
        bins[index].append(record)
    rows: list[dict[str, float]] = []
    max_deviation = 0.0
    for index, bucket in enumerate(bins):
        lower = index / 10
        upper = lower + 0.1
        if bucket:
            mean_predicted = safe_mean([item["predicted_probability"] for item in bucket])
            mean_actual = safe_mean([item["actual_outcome"] for item in bucket])
            deviation = abs(mean_predicted - mean_actual)
        else:
            mean_predicted = float("nan")
            mean_actual = float("nan")
            deviation = float("nan")
        if math.isfinite(deviation):
            max_deviation = max(max_deviation, deviation)
        rows.append(
            {
                "lower": lower,
                "upper": upper,
                "mean_predicted": mean_predicted,
                "mean_actual": mean_actual,
                "count": float(len(bucket)),
                "deviation": deviation,
            }
        )
    return rows, max_deviation, estimate_calibration_label(max_deviation)


def print_calibration_curve(best_method: str, best_sigma_multiplier: float, records: list[dict[str, float]]) -> tuple[list[dict[str, float]], float, str]:
    rows, max_deviation, label = compute_calibration_curve(records)
    print()
    print(f"CALIBRATION CURVE (Best: Method {best_method}, σ×{best_sigma_multiplier:.2f})")
    print("=" * len(f"CALIBRATION CURVE (Best: Method {best_method}, σ×{best_sigma_multiplier:.2f})"))
    print(f"{'Predicted':<12} {'Mean Pred':<10} {'Actual':<8} {'Count':<8} {'Deviation':<10}")
    for row in rows:
        label_range = f"{row['lower']:.1f}-{row['upper']:.1f}"
        print(
            f"{label_range:<12} {format_float(row['mean_predicted']):<10} {format_float(row['mean_actual']):<8} "
            f"{int(row['count']):<8} {format_float(row['deviation']):<10}"
        )
    deviation_row = max(
        (row for row in rows if math.isfinite(row["deviation"])),
        key=lambda row: row["deviation"],
        default=None,
    )
    if deviation_row:
        print(
            f"Max deviation: {deviation_row['deviation']:.2f} "
            f"(in bin {deviation_row['lower']:.1f}-{deviation_row['upper']:.1f})"
        )
    else:
        print("Max deviation: N/A")
    print(f"Overall calibration: {label}")
    return rows, max_deviation, label


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def recommendations_text(
    best_global: dict[str, float | str],
    per_location_best: dict[str, dict[str, float | str]],
    ensemble_global_rmse: float,
    best_source_name: str,
    suspicious_notes: list[str],
) -> list[str]:
    best_method = str(best_global["method"])
    best_sigma_multiplier = float(best_global["sigma_multiplier"])
    if per_location_best:
        per_location_methods = {str(result["method"]) for result in per_location_best.values()}
        per_location_sigmas = [float(result["sigma_multiplier"]) for result in per_location_best.values()]
    else:
        per_location_methods = {best_method}
        per_location_sigmas = [best_sigma_multiplier]
    if len(per_location_methods) == 1 and max(per_location_sigmas) - min(per_location_sigmas) < 0.35:
        location_recommendation = "Global calibration looks sufficient; per-location tuning adds limited incremental value."
    else:
        location_recommendation = "Per-location tuning changes the best fit materially, so keeping location overrides is justified."
    system_approach = (
        "better"
        if best_method == "B"
        else "worse"
    )
    notes = [
        f"Use Method {best_method} in `algo_forecast.py` for sigma estimation.",
        f"Default the sigma multiplier to {best_sigma_multiplier:.2f}; this was the best global fit by Brier score.",
        location_recommendation,
        f"The most accurate non-suspicious single source over this sample was `{best_source_name}` by global RMSE.",
        (
            "The current source-disagreement sigma approach (Method B) "
            f"was {system_approach} than the best alternative on this calibration set."
        ),
        f"Reference ensemble RMSE for Method A comparisons was {ensemble_global_rmse:.2f}C.",
    ]
    if suspicious_notes:
        notes.append("Quasi-actual series were detected and excluded from the ensemble calibration path: " + "; ".join(suspicious_notes))
    return notes


def write_markdown_report(
    repo_root: Path,
    actuals: dict[str, dict[str, dict[str, float]]],
    source_stats_by_location: dict[str, dict[str, ErrorStats]],
    source_global_stats: dict[str, ErrorStats],
    ensemble_stats_by_location: dict[str, ErrorStats],
    ensemble_global_stats: ErrorStats,
    ensemble_vs_best_model: dict[str, dict[str, float | str]],
    global_scored: dict[str, list[dict[str, float]]],
    best_global: dict[str, float | str],
    per_location_best: dict[str, dict[str, float | str]],
    calibration_rows: list[dict[str, float]],
    calibration_label: str,
    histogram_rows: list[tuple[str, int, str]],
    recommendations: list[str],
    suspicious_notes: list[str],
) -> Path:
    report_path = repo_root / "docs" / "SIGMA_CALIBRATION.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    actual_coverage = sum(len(records) for records in actuals.values())
    global_table_rows = []
    for source in SOURCE_ORDER + ["ensemble"]:
        stats = source_global_stats[source] if source != "ensemble" else ensemble_global_stats
        global_table_rows.append(
            [
                source,
                str(stats.count),
                format_float(stats.mean_error),
                format_float(stats.rmse),
                format_float(stats.std_dev),
                format_float(stats.p05),
                format_float(stats.p95),
                format_float(stats.skewness),
            ]
        )
    per_location_rows = []
    for location_name in [location["name"] for location in LOCATIONS if location["name"] in per_location_best]:
        best = per_location_best[location_name]
        highs = [record["high_c"] for record in actuals[location_name].values()]
        per_location_rows.append(
            [
                location_name,
                str(best["method"]),
                f"{float(best['sigma_multiplier']):.2f}",
                format_float(float(best["brier"]), 3),
                format_temp(ensemble_stats_by_location[location_name].rmse),
                f"{min(highs):.1f} to {max(highs):.1f}C",
            ]
        )
    sigma_section_rows = []
    for method in METHOD_ORDER:
        for row in global_scored[method]:
            marker = "best" if method == best_global["method"] and row["sigma_multiplier"] == best_global["sigma_multiplier"] else ""
            sigma_section_rows.append(
                [
                    method,
                    f"{row['sigma_multiplier']:.2f}",
                    format_float(row["brier"], 3),
                    format_float(row["log_loss"], 3),
                    marker,
                ]
            )
    calibration_curve_rows = [
        [
            f"{row['lower']:.1f}-{row['upper']:.1f}",
            format_float(row["mean_predicted"], 3),
            format_float(row["mean_actual"], 3),
            str(int(row["count"])),
            format_float(row["deviation"], 3),
        ]
        for row in calibration_rows
    ]
    ensemble_vs_best_rows = []
    for location_name, comparison in ensemble_vs_best_model.items():
        ensemble_vs_best_rows.append(
            [
                location_name,
                format_float(float(comparison["ensemble_rmse"])),
                format_float(float(comparison["best_model_rmse"])),
                str(comparison["best_model"]),
            ]
        )
    histogram_block = "\n".join(
        [f"{label:>10} | {count:>4} {bar}" for label, count, bar in histogram_rows]
    )
    report_lines = [
        "# Sigma Calibration",
        "",
        f"- Date of analysis: {date.today().isoformat()}",
        f"- Data sources: Historical Forecast API + Previous Runs API (`best_match`, `ecmwf_ifs025`, `gfs_seamless`)",
        f"- Data range: {START_DATE} to {END_DATE}",
        f"- Location-days analyzed: {actual_coverage}",
        "",
        "## Data Quality Caveats",
        "",
    ]
    if suspicious_notes:
        report_lines.extend([f"- {note}" for note in suspicious_notes])
    else:
        report_lines.append("- No suspicious quasi-actual forecast series were detected.")
    report_lines.extend([
        "",
        "## Per-Source RMSE and Bias",
        "",
        markdown_table(
            ["Source", "Samples", "Bias (C)", "RMSE (C)", "Std (C)", "P05", "P95", "Skew"],
            global_table_rows,
        ),
        "",
        "## Ensemble vs Single Source",
        "",
        markdown_table(
            ["Location", "Ensemble RMSE", "Best Single RMSE", "Best Model"],
            ensemble_vs_best_rows,
        ),
        "",
        "## Global Sigma Calibration",
        "",
        markdown_table(
            ["Method", "σ Mult", "Brier", "LogLoss", "Best"],
            sigma_section_rows,
        ),
        "",
        (
            f"Best overall calibration was Method {best_global['method']} with σ×{float(best_global['sigma_multiplier']):.2f} "
            f"(Brier {float(best_global['brier']):.3f})."
        ),
        "",
        "## Per-Location Results",
        "",
        markdown_table(
            ["Location", "Best Method", "Best σ×", "Brier", "RMSE", "Temp Range"],
            per_location_rows,
        ),
        "",
        "## Calibration Curve",
        "",
        markdown_table(
            ["Predicted Bin", "Mean Pred", "Mean Actual", "Count", "Deviation"],
            calibration_curve_rows,
        ),
        "",
        f"Overall calibration quality: **{calibration_label}**",
        "",
        "## Error Distribution",
        "",
        "```text",
        histogram_block,
        "```",
        "",
        "## Recommendations",
        "",
    ])
    report_lines.extend([f"{index}. {line}" for index, line in enumerate(recommendations, start=1)])
    report_lines.extend(["", "## Per-Location Source Detail", ""])
    for location_name in [location["name"] for location in LOCATIONS if location["name"] in actuals]:
        report_lines.extend(
            [
                f"### {location_name}",
                "",
                markdown_table(
                    ["Source", "Count", "Bias (C)", "RMSE (C)", "Std (C)", "Suspicious?"],
                    [
                        [
                            source,
                            str(source_stats_by_location[source][location_name].count),
                            format_float(source_stats_by_location[source][location_name].mean_error),
                            format_float(source_stats_by_location[source][location_name].rmse),
                            format_float(source_stats_by_location[source][location_name].std_dev),
                            "yes" if is_suspicious_series(source_stats_by_location[source][location_name]) else "no",
                        ]
                        for source in SOURCE_ORDER
                    ],
                ),
                "",
            ]
        )
    report = "\n".join(report_lines)
    report_path.write_text(report, encoding="utf-8")
    return report_path


def write_json_output(
    repo_root: Path,
    best_global: dict[str, float | str],
    per_location_best: dict[str, dict[str, float | str]],
    source_global_stats: dict[str, ErrorStats],
    source_stats_by_location: dict[str, dict[str, ErrorStats]],
    ensemble_global_stats: ErrorStats,
    ensemble_stats_by_location: dict[str, ErrorStats],
    suspicious_notes: list[str],
) -> Path:
    output_path = repo_root / "data" / "sigma_calibration.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "calibration_date": date.today().isoformat(),
        "data_range": f"{START_DATE} to {END_DATE}",
        "best_method": str(best_global["method"]),
        "best_sigma_multiplier": round(float(best_global["sigma_multiplier"]), 2),
        "best_brier": round(float(best_global["brier"]), 3),
        "warnings": suspicious_notes,
        "per_location": {
            location_name: {
                "method": str(result["method"]),
                "sigma_mult": round(float(result["sigma_multiplier"]), 2),
                "brier": round(float(result["brier"]), 3),
            }
            for location_name, result in per_location_best.items()
        },
        "source_rmse": {
            source: {
                "global": round(source_global_stats[source].rmse, 3),
                "per_location": {
                    location_name: round(source_stats_by_location[source][location_name].rmse, 3)
                    for location_name in source_stats_by_location[source]
                },
            }
            for source in SOURCE_ORDER
        }
        | {
            "ensemble": {
                "global": round(ensemble_global_stats.rmse, 3),
                "per_location": {
                    location_name: round(ensemble_stats_by_location[location_name].rmse, 3)
                    for location_name in ensemble_stats_by_location
                },
            }
        },
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path


def print_error_distribution(title: str, stats: ErrorStats) -> None:
    print(
        f"{title}: bias {stats.mean_error:+.2f}C, std {stats.std_dev:.2f}C, RMSE {stats.rmse:.2f}C, "
        f"p05 {stats.p05:.2f}C, p95 {stats.p95:.2f}C, skew {stats.skewness:+.2f}"
    )


def integration_instructions(best_global: dict[str, float | str]) -> str:
    method = str(best_global["method"])
    sigma_multiplier = float(best_global["sigma_multiplier"])
    method_lines = {
        "A": [
            "sigma = ENSEMBLE_RMSE * sigma_mult",
        ],
        "B": [
            "sigma = max(source_std, 1.0) * sigma_mult",
        ],
        "C": [
            "weights = [1 / (rmse ** 2) for rmse in source_rmses]",
            "weighted_mean = sum(w * f for w, f in zip(weights, forecasts)) / sum(weights)",
            "sigma = (1 / math.sqrt(sum(weights))) * sigma_mult",
            "mean = weighted_mean",
        ],
    }
    body = [
        "INTEGRATION INSTRUCTIONS",
        "========================",
        "In algo_forecast.py, replace the current sigma logic with:",
        "",
        "from pathlib import Path",
        "import json",
        "import math",
        "",
        'CALIBRATION = json.loads(Path("data/sigma_calibration.json").read_text())',
        "",
        "# In probability estimation:",
        'if location in CALIBRATION["per_location"]:',
        '    sigma_mult = CALIBRATION["per_location"][location]["sigma_mult"]',
        "else:",
        '    sigma_mult = CALIBRATION["best_sigma_multiplier"]',
        "",
        f"# Best global calibration from this run: Method {method}, sigma multiplier {sigma_multiplier:.2f}",
        "",
    ]
    body.extend(method_lines[method])
    body.extend(
        [
            "",
            "# Preserve the existing Gaussian CDF contract logic:",
            "# at_or_above => 1 - self._cdf(contract['threshold'] - 0.5, mean, sigma)",
            "# at_or_below => self._cdf(contract['threshold'] + 0.5, mean, sigma)",
            "# between / exact => same current threshold-window logic",
        ]
    )
    return "\n".join(body)


def main() -> None:
    load_local_env()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    repo_root = get_repo_root()

    actuals: dict[str, dict[str, dict[str, float]]] = {}
    forecasts: dict[str, dict[str, dict[str, dict[str, float | str]]]] = {source: {} for source in SOURCE_ORDER}
    source_stats_by_location: dict[str, dict[str, ErrorStats]] = {source: {} for source in SOURCE_ORDER}
    source_global_errors: dict[str, list[float]] = {source: [] for source in SOURCE_ORDER}
    usable_source_global_errors: dict[str, list[float]] = {source: [] for source in SOURCE_ORDER}
    suspicious_source_locations: set[tuple[str, str]] = set()
    suspicious_notes: list[str] = []

    with httpx.Client(timeout=TIMEOUT) as client:
        print("STEP 1 — FETCH ACTUALS")
        print("======================")
        for location in LOCATIONS:
            params = {
                "latitude": location["lat"],
                "longitude": location["lon"],
                "daily": "temperature_2m_max,temperature_2m_min",
                "start_date": START_DATE,
                "end_date": END_DATE,
                "timezone": "auto",
            }
            try:
                payload = fetch_json(client, HISTORICAL_FORECAST_API, params)
                records = parse_actuals(payload)
                actuals[location["name"]] = records
                summarize_location_actuals(location["name"], records)
            except Exception as exc:
                logger.error("Failed to fetch actuals for %s: %s", location["name"], exc)

        if len(actuals) < MIN_WORKABLE_LOCATIONS:
            raise SystemExit(f"Only {len(actuals)} locations fetched successfully; need at least {MIN_WORKABLE_LOCATIONS}.")

        print()
        print("STEP 2 — FETCH SOURCE FORECASTS")
        print("===============================")
        for source in SOURCE_ORDER:
            for location in LOCATIONS:
                location_name = location["name"]
                if location_name not in actuals:
                    continue
                base_params = {
                    "latitude": location["lat"],
                    "longitude": location["lon"],
                    "timezone": "auto",
                    "start_date": START_DATE,
                    "end_date": END_DATE,
                } | MODEL_PARAMS[source]
                try:
                    day0_payload = fetch_json(
                        client,
                        PREVIOUS_RUNS_API,
                        base_params | {"daily": "temperature_2m_max"},
                    )
                    day1_payload = fetch_json(
                        client,
                        PREVIOUS_RUNS_API,
                        base_params | {"hourly": "temperature_2m", "hourly_previous_day": 1},
                    )
                    day0_highs = parse_daily_highs(day0_payload)
                    day1_highs = parse_hourly_highs(day1_payload)
                    merged, counters = merge_forecast_series(day1_highs, day0_highs)
                    forecasts[source][location_name] = merged
                    errors, _ = collect_errors(merged, actuals[location_name])
                    stats = compute_error_stats(errors)
                    source_stats_by_location[source][location_name] = stats
                    source_global_errors[source].extend(errors)
                    note = ""
                    if is_suspicious_series(stats):
                        suspicious_source_locations.add((source, location_name))
                        note = " [SUSPICIOUS quasi-actual match excluded from calibration ensemble]"
                        suspicious_notes.append(
                            f"{source} for {location_name} matched actuals almost perfectly (RMSE {stats.rmse:.2f}C) and was excluded from ensemble calibration."
                        )
                    else:
                        usable_source_global_errors[source].extend(errors)
                    print(
                        f"{source} for {location_name}: mean error {stats.mean_error:+.2f}C, "
                        f"RMSE {stats.rmse:.2f}C, std {stats.std_dev:.2f}C over {stats.count} days "
                        f"(day1={counters['day1']}, fallback={counters['day0_fallback']}){note}"
                    )
                except Exception as exc:
                    logger.error("Failed to fetch %s for %s: %s", source, location_name, exc)

    source_global_stats = {
        source: compute_error_stats(source_global_errors[source])
        for source in SOURCE_ORDER
    }
    usable_source_global_stats = {
        source: compute_error_stats(usable_source_global_errors[source])
        for source in SOURCE_ORDER
    }

    print()
    print("STEP 3 — ENSEMBLE FORECAST ERRORS")
    print("=================================")
    if suspicious_notes:
        print("Excluding suspicious quasi-actual source/location pairs from ensemble calibration:")
        for note in suspicious_notes:
            print(f"- {note}")
        print()
    ensemble_samples: list[dict] = []
    ensemble_errors_by_location: dict[str, list[float]] = {}
    ensemble_stats_by_location: dict[str, ErrorStats] = {}
    ensemble_global_errors: list[float] = []
    ensemble_vs_best_model: dict[str, dict[str, float | str]] = {}
    for location in LOCATIONS:
        location_name = location["name"]
        if location_name not in actuals:
            continue
        errors: list[float] = []
        per_day_samples: list[dict] = []
        for day, actual in sorted(actuals[location_name].items()):
            source_forecasts = {
                source: float(forecasts[source][location_name][day]["high_c"])
                for source in SOURCE_ORDER
                if location_name in forecasts[source]
                and day in forecasts[source][location_name]
                and (source, location_name) not in suspicious_source_locations
            }
            if not source_forecasts:
                continue
            source_values = list(source_forecasts.values())
            ensemble_mean = safe_mean(source_values)
            ensemble_std = max(safe_stdev(source_values), 0.5)
            ensemble_error = ensemble_mean - float(actual["high_c"])
            errors.append(ensemble_error)
            per_day_samples.append(
                {
                    "location": location_name,
                    "date": day,
                    "actual_high": float(actual["high_c"]),
                    "ensemble_mean": ensemble_mean,
                    "ensemble_std": ensemble_std,
                    "source_forecasts": source_forecasts,
                }
            )
        stats = compute_error_stats(errors)
        ensemble_errors_by_location[location_name] = errors
        ensemble_stats_by_location[location_name] = stats
        ensemble_global_errors.extend(errors)
        ensemble_samples.extend(per_day_samples)
        best_model, best_rmse = min(
            (
                (source, source_stats_by_location[source][location_name].rmse)
                for source in SOURCE_ORDER
                if location_name in source_stats_by_location[source]
                and (source, location_name) not in suspicious_source_locations
            ),
            key=lambda item: item[1],
        )
        ensemble_vs_best_model[location_name] = {
            "ensemble_rmse": stats.rmse,
            "best_model_rmse": best_rmse,
            "best_model": best_model,
        }
        print(
            f"Ensemble for {location_name}: mean error {stats.mean_error:+.2f}C, "
            f"RMSE {stats.rmse:.2f}C, std {stats.std_dev:.2f}C"
        )
        print(
            f"Ensemble vs best single model: ensemble RMSE {stats.rmse:.2f}C vs "
            f"best single {best_rmse:.2f}C ({best_model})"
        )

    ensemble_global_stats = compute_error_stats(ensemble_global_errors)
    if not ensemble_samples:
        raise SystemExit("No usable forecast-vs-actual samples were available for calibration.")

    print()
    print("STEP 4 — GLOBAL SIGMA CALIBRATION")
    print("=================================")
    global_probability_records = make_probability_records(
        ensemble_samples,
        ensemble_global_stats.rmse,
        {source: source_global_stats[source].rmse for source in SOURCE_ORDER},
    )
    global_scored = score_probability_records(global_probability_records)
    print_sigma_tables(global_scored)
    best_global = best_method_row(global_scored)

    print()
    print("STEP 5 — PER-LOCATION OPTIMAL SIGMA")
    print("===================================")
    per_location_best: dict[str, dict[str, float | str]] = {}
    print(f"{'Location':<12} {'Best Method':<11} {'Best σ×':<8} {'Brier':<8} {'RMSE':<8} {'Temp Range':<14}")
    for location in LOCATIONS:
        location_name = location["name"]
        if location_name not in ensemble_stats_by_location:
            continue
        location_samples = [sample for sample in ensemble_samples if sample["location"] == location_name]
        scored = score_probability_records(
            make_probability_records(
                location_samples,
                ensemble_stats_by_location[location_name].rmse,
                {
                    source: source_stats_by_location[source][location_name].rmse
                    for source in SOURCE_ORDER
                    if location_name in source_stats_by_location[source]
                },
            )
        )
        best_row = best_method_row(scored)
        per_location_best[location_name] = best_row
        highs = [record["high_c"] for record in actuals[location_name].values()]
        print(
            f"{location_name:<12} {str(best_row['method']):<11} {float(best_row['sigma_multiplier']):<8.2f} "
            f"{float(best_row['brier']):<8.3f} {ensemble_stats_by_location[location_name].rmse:<8.2f} "
            f"{min(highs):.1f}-{max(highs):.1f}C"
        )

    print()
    print("STEP 6 — CALIBRATION CURVE")
    print("==========================")
    best_method = str(best_global["method"])
    best_sigma_multiplier = float(best_global["sigma_multiplier"])
    best_records = [
        record
        for record in global_probability_records[best_method]
        if math.isclose(record["sigma_multiplier"], best_sigma_multiplier)
    ]
    calibration_rows, _, calibration_label = print_calibration_curve(best_method, best_sigma_multiplier, best_records)

    print()
    print("STEP 7 — FORECAST ERROR DISTRIBUTION")
    print("====================================")
    for source in SOURCE_ORDER:
        print_error_distribution(source, source_global_stats[source])
    print_error_distribution("ensemble", ensemble_global_stats)
    print()
    print("Error (C)  |  Count")
    histogram_rows = build_histogram(ensemble_global_errors)
    for label, count, bar in histogram_rows:
        print(f"{label:<10} |  {count:<4} {bar}")

    recommendations = recommendations_text(
        best_global,
        per_location_best,
        ensemble_global_stats.rmse,
        min(
            (
                (source, stats.rmse)
                for source, stats in usable_source_global_stats.items()
                if stats.count
            ),
            key=lambda item: item[1],
        )[0],
        suspicious_notes,
    )
    report_path = write_markdown_report(
        repo_root=repo_root,
        actuals=actuals,
        source_stats_by_location=source_stats_by_location,
        source_global_stats=source_global_stats,
        ensemble_stats_by_location=ensemble_stats_by_location,
        ensemble_global_stats=ensemble_global_stats,
        ensemble_vs_best_model=ensemble_vs_best_model,
        global_scored=global_scored,
        best_global=best_global,
        per_location_best=per_location_best,
        calibration_rows=calibration_rows,
        calibration_label=calibration_label,
        histogram_rows=histogram_rows,
        recommendations=recommendations,
        suspicious_notes=suspicious_notes,
    )
    json_path = write_json_output(
        repo_root=repo_root,
        best_global=best_global,
        per_location_best=per_location_best,
        source_global_stats=source_global_stats,
        source_stats_by_location=source_stats_by_location,
        ensemble_global_stats=ensemble_global_stats,
        ensemble_stats_by_location=ensemble_stats_by_location,
        suspicious_notes=suspicious_notes,
    )

    print()
    print("STEP 8 — OUTPUTS WRITTEN")
    print("========================")
    print(f"Report: {report_path}")
    print(f"JSON:   {json_path}")
    print()
    print(integration_instructions(best_global))


if __name__ == "__main__":
    main()
