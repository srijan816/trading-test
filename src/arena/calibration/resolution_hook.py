from __future__ import annotations

import json
import logging
import re
import statistics
from datetime import datetime, timezone

from arena.adapters.weather_openmeteo import CITY_COORDS
from arena.calibration.crps_tracker import CRPSTracker
from arena.data_sources.station_observations import get_daily_observed_temperature_details
from arena.db import ArenaDB

logger = logging.getLogger(__name__)


def _score_decision(predicted_prob: float, actual_outcome: float) -> float:
    return (predicted_prob - actual_outcome) ** 2


def _compute_calibration_error(predictions: list[float], actuals: list[float]) -> float:
    if not predictions:
        return 0.0
    buckets: dict[str, tuple[list[float], list[float]]] = {
        "0-0.2": ([], []),
        "0.2-0.4": ([], []),
        "0.4-0.6": ([], []),
        "0.6-0.8": ([], []),
        "0.8-1.0": ([], []),
    }
    for pred, act in zip(predictions, actuals):
        if pred < 0.2:
            key = "0-0.2"
        elif pred < 0.4:
            key = "0.2-0.4"
        elif pred < 0.6:
            key = "0.4-0.6"
        elif pred < 0.8:
            key = "0.6-0.8"
        else:
            key = "0.8-1.0"
        buckets[key][0].append(pred)
        buckets[key][1].append(act)

    max_deviation = 0.0
    for preds, acts in buckets.values():
        if not preds:
            continue
        mean_pred = statistics.mean(preds)
        mean_act = statistics.mean(acts)
        max_deviation = max(max_deviation, abs(mean_pred - mean_act))
    return round(max_deviation, 4)


def _c_to_f(value_c: float) -> float:
    return (float(value_c) * 9.0 / 5.0) + 32.0


def _delta_c_to_f(value_c: float) -> float:
    return float(value_c) * 9.0 / 5.0


def _f_to_c(value_f: float) -> float:
    return (float(value_f) - 32.0) * 5.0 / 9.0


def _parse_weather_market_question(question: str) -> dict[str, object] | None:
    normalized = re.sub(r"\s+", " ", str(question or "")).strip().rstrip("?")
    if not normalized:
        return None

    city = None
    city_patterns = (
        r"highest temperature in ([A-Za-z .'-]+?) be",
        r"lowest temperature in ([A-Za-z .'-]+?) be",
        r"will ([A-Za-z .'-]+?) high\b",
        r"will ([A-Za-z .'-]+?) low\b",
        r"will ([A-Za-z .'-]+?) hit\b",
        r"for ([A-Za-z .'-]+?) on\b",
        r"in ([A-Za-z .'-]+?) on\b",
    )
    for pattern in city_patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            break
    if not city:
        return None

    date_match = re.search(r"\bon ([A-Z][a-z]+ \d{1,2})(?:,? (\d{4}))?\b", normalized)
    if date_match:
        month_day, year_text = date_match.groups()
        year = int(year_text) if year_text else datetime.now(timezone.utc).year
        try:
            target_date = datetime.strptime(f"{month_day} {year}", "%B %d %Y").date().isoformat()
        except ValueError:
            return None
    else:
        target_date = datetime.now(timezone.utc).date().isoformat()

    unit = "f" if re.search(r"°?\s*F\b", normalized, re.IGNORECASE) else "c"
    threshold = None
    threshold_c = None
    shape = None
    metric = "low" if re.search(r"\blow(?:est)? temperature\b", normalized, re.IGNORECASE) else "high"
    if re.search(r"\brain\b", normalized, re.IGNORECASE):
        metric = "rain"

    between = re.search(r"between (\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*°?\s*([CF])", normalized, re.IGNORECASE)
    if between:
        lower = float(between.group(1))
        upper = float(between.group(2))
        return {
            "city": city.lower(),
            "target_date": target_date,
            "unit": between.group(3).lower(),
            "shape": "between",
            "metric": metric,
            "lower": lower,
            "upper": upper,
        }

    threshold_patterns = (
        (r"(\d+(?:\.\d+)?)\s*°?\s*([CF])\s*or higher", "at_or_above"),
        (r"(\d+(?:\.\d+)?)\s*°?\s*([CF])\s*or below", "at_or_below"),
        (r"high exceed (\d+(?:\.\d+)?)\s*°?\s*([CF])", "at_or_above"),
        (r"hit (\d+(?:\.\d+)?)\s*([CF])", "at_or_above"),
        (r"be (\d+(?:\.\d+)?)\s*°?\s*([CF])", "exact"),
    )
    for pattern, candidate_shape in threshold_patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match:
            threshold = float(match.group(1))
            unit = match.group(2).lower()
            shape = candidate_shape
            break

    if threshold is not None:
        threshold_c = threshold if unit == "c" else _f_to_c(threshold)

    parsed = {
        "city": city.lower(),
        "target_date": target_date,
        "unit": unit,
        "shape": shape or "unknown",
        "metric": metric,
        "threshold": threshold,
        "threshold_c": round(threshold_c, 3) if threshold_c is not None else None,
    }
    return parsed


def _extract_gaussian_inputs(evidence_items: list[dict]) -> dict[str, object]:
    mu_high_c: float | None = None
    mu_low_c: float | None = None
    sigma_high_c: float | None = None
    sigma_low_c: float | None = None
    source_names: list[str] = []

    for item in evidence_items:
        if not isinstance(item, dict):
            continue
        content = str(item.get("content", ""))
        lowered = content.lower()

        if "ensemble forecast:" in lowered:
            match = re.search(
                r"ensemble forecast:\s*(-?\d+\.?\d*)C\s*±\s*(\d+\.?\d*)C(?:.*\(([^)]*)\))?",
                content,
                re.IGNORECASE,
            )
            if match:
                mu_high_c = float(match.group(1))
                sigma_high_c = float(match.group(2))
                if match.group(3):
                    source_names = [part.strip() for part in match.group(3).split(",") if part.strip()]
                continue

        detailed_match = re.search(
            r"high=(?P<high>-?\d+\.?\d*)C\s+low=(?P<low>-?\d+\.?\d*|None)C\s+high_sigma=(?P<high_sigma>\d+\.?\d*)C\s+low_sigma=(?P<low_sigma>\d+\.?\d*|None)C",
            content,
            re.IGNORECASE,
        )
        if detailed_match:
            mu_high_c = float(detailed_match.group("high"))
            low_token = detailed_match.group("low")
            if low_token.lower() != "none":
                mu_low_c = float(low_token)
            sigma_high_c = float(detailed_match.group("high_sigma"))
            low_sigma_token = detailed_match.group("low_sigma")
            if low_sigma_token.lower() != "none":
                sigma_low_c = float(low_sigma_token)
            continue

        if mu_high_c is None:
            forecast_match = re.search(r"high forecast\s*(-?\d+\.?\d*)\s*°?C", content, re.IGNORECASE)
            if forecast_match:
                mu_high_c = float(forecast_match.group(1))

        if mu_low_c is None:
            forecast_low_match = re.search(r"low forecast\s*(-?\d+\.?\d*)\s*°?C", content, re.IGNORECASE)
            if forecast_low_match:
                mu_low_c = float(forecast_low_match.group(1))

        if sigma_high_c is None:
            sigma_match = re.search(r"(\d+\.?\d*)\s*°?C sigma", content, re.IGNORECASE)
            if sigma_match:
                sigma_high_c = float(sigma_match.group(1))

        if not source_names:
            source_match = re.search(r"ensemble \(([^)]*)\)", content, re.IGNORECASE)
            if source_match:
                source_names = [part.strip() for part in source_match.group(1).split(",") if part.strip()]
        if not source_names:
            model_match = re.search(r"(\d+)-model ensemble \(([^)]*)\)", content, re.IGNORECASE)
            if model_match:
                source_names = [part.strip().lower().replace("-", "_") for part in model_match.group(2).split(",") if part.strip()]

    return {
        "mu_high_c": mu_high_c,
        "mu_low_c": mu_low_c,
        "sigma_high_c": sigma_high_c,
        "sigma_low_c": sigma_low_c,
        "source_names": source_names,
    }


def _compute_market_probability(parsed_weather: dict[str, object], mu_c: float, sigma_c: float) -> float | None:
    if sigma_c <= 0:
        return None

    distribution = statistics.NormalDist(mu=mu_c, sigma=sigma_c)
    shape = str(parsed_weather.get("shape") or "unknown")
    threshold_c = parsed_weather.get("threshold_c")

    if shape == "between":
        lower = parsed_weather.get("lower")
        upper = parsed_weather.get("upper")
        unit = str(parsed_weather.get("unit") or "c").lower()
        if lower is None or upper is None:
            return None
        if unit == "f":
            lower = _f_to_c(float(lower))
            upper = _f_to_c(float(upper))
        probability = distribution.cdf(float(upper)) - distribution.cdf(float(lower))
    elif threshold_c is None:
        return None
    elif shape == "at_or_above":
        probability = 1.0 - distribution.cdf(float(threshold_c))
    elif shape == "at_or_below":
        probability = distribution.cdf(float(threshold_c))
    elif shape == "exact":
        probability = distribution.cdf(float(threshold_c) + 0.5) - distribution.cdf(float(threshold_c) - 0.5)
    else:
        return None

    return max(0.0, min(1.0, float(probability)))


def compute_sigma_adjustment(current_sigma: float, crps_ratio: float, n_observations: int) -> float:
    """Compute a city-level sigma recommendation in degrees Celsius."""
    sigma = max(float(current_sigma), 0.5)
    ratio = float(crps_ratio)
    observations = int(n_observations)

    if observations < 3:
        adjusted = sigma
    else:
        target_ratio = 1.25
        raw_multiplier = max(0.7, min(4.0, ratio / target_ratio if target_ratio > 0 else 1.0))
        confidence_weight = min(1.0, max(0.35, observations / 8.0))
        adjusted = sigma * (1.0 + ((raw_multiplier - 1.0) * confidence_weight))

    return round(min(max(adjusted, 0.5), 10.0), 3)


def _write_parameter_adjustment(
    db: ArenaDB,
    *,
    strategy_id: str,
    parameter_name: str,
    current_value: float,
    recommended_value: float,
    reason: str,
    city: str | None = None,
) -> int | None:
    logger.info(
        "Writing parameter adjustment: city=%s, param=%s, old=%.3f, new=%.3f, reason=%s",
        city,
        parameter_name,
        current_value,
        recommended_value,
        reason,
    )
    try:
        with db.connect() as conn:
            cursor = conn.execute(
                "INSERT INTO parameter_adjustments "
                "(strategy_id, city, parameter_name, current_value, recommended_value, reason) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (strategy_id, city, parameter_name, current_value, recommended_value, reason),
            )
        logger.info("Parameter adjustment written successfully, id=%s", cursor.lastrowid)
        return int(cursor.lastrowid)
    except Exception as exc:
        logger.error("FAILED to write parameter adjustment: %s", exc, exc_info=True)
        return None


def _write_sigma_adjustment_for_city(
    db: ArenaDB,
    tracker: CRPSTracker,
    *,
    city: str,
    metric: str,
    current_sigma_c: float,
) -> int | None:
    summary = tracker.get_calibration_summary(city=city, last_n_days=30, metric=metric)
    n_records = int(summary.get("n_records", 0) or 0)
    calibration_ratio = float(summary.get("calibration_ratio", 0.0) or 0.0)
    recommended_sigma_c = compute_sigma_adjustment(current_sigma_c, calibration_ratio, n_records)

    if n_records < 3 or abs(recommended_sigma_c - float(current_sigma_c)) < 0.01:
        return None

    latest_row = None
    with db.connect() as conn:
        latest_row = conn.execute(
            """
            SELECT recommended_value, created_at
            FROM parameter_adjustments
            WHERE lower(city) = lower(?) AND parameter_name = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (city, f"ensemble_sigma_{metric}"),
        ).fetchone()

    if latest_row and abs(float(latest_row["recommended_value"]) - recommended_sigma_c) < 0.01:
        return None

    reason = (
        f"CRPS calibration ratio {calibration_ratio:.2f}x across {n_records} resolved markets "
        f"for {city} {metric} - recommend sigma {current_sigma_c:.2f}C -> {recommended_sigma_c:.2f}C"
    )
    return _write_parameter_adjustment(
        db,
        strategy_id="weather_ensemble",
        city=city,
        parameter_name=f"ensemble_sigma_{metric}",
        current_value=current_sigma_c,
        recommended_value=recommended_sigma_c,
        reason=reason,
    )


def _load_source_forecasts(
    db: ArenaDB,
    location: str | None,
    target_date: str | None,
    source_names: list[str] | None = None,
    metric: str = "high",
) -> dict[str, float]:
    if not location or not target_date:
        return {}

    with db.connect() as conn:
        rows = list(
            conn.execute(
                "SELECT source, predicted_high_c, predicted_low_c FROM forecast_history "
                "WHERE lower(location) = lower(?) AND target_date = ?",
                (location, target_date),
            )
        )

    column_name = "predicted_low_c" if metric == "low" else "predicted_high_c"
    source_values = {
        str(row["source"]): round(_c_to_f(float(row[column_name])), 2)
        for row in rows
        if row[column_name] is not None
    }
    if source_names:
        filtered = {name: source_values[name] for name in source_names if name in source_values}
        if filtered:
            return filtered
    return source_values


def _load_market_context(db: ArenaDB, market_id: str, venue: str) -> dict[str, object] | None:
    market_row = db.get_market(market_id, venue)
    if not market_row:
        return None
    outcomes_json = market_row["outcomes_json"]
    outcomes = json.loads(outcomes_json) if isinstance(outcomes_json, str) else (outcomes_json or [])
    yes_outcome_id = None
    no_outcome_id = None
    for outcome in outcomes:
        label = str(outcome.get("label", "")).strip().lower()
        if label == "yes":
            yes_outcome_id = str(outcome.get("outcome_id"))
        elif label == "no":
            no_outcome_id = str(outcome.get("outcome_id"))
    return {
        "question": str(market_row["question"]),
        "category": str(market_row["category"]),
        "yes_outcome_id": yes_outcome_id,
        "no_outcome_id": no_outcome_id,
    }


def _load_latest_decision_context(db: ArenaDB, market_id: str) -> dict[str, object] | None:
    with db.connect() as conn:
        row = conn.execute(
            """
            SELECT decision_id, timestamp, evidence_items_json
            FROM decisions
            WHERE markets_considered_json LIKE ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (f'%{market_id}%',),
        ).fetchone()
    if not row:
        return None
    evidence_json = row["evidence_items_json"]
    evidence = json.loads(evidence_json) if isinstance(evidence_json, str) else (evidence_json or [])
    gaussian_inputs = _extract_gaussian_inputs(evidence)
    return {
        "decision_id": str(row["decision_id"]),
        "timestamp": str(row["timestamp"]),
        "evidence": evidence,
        "mu_high_c": gaussian_inputs.get("mu_high_c"),
        "mu_low_c": gaussian_inputs.get("mu_low_c"),
        "sigma_high_c": gaussian_inputs.get("sigma_high_c"),
        "sigma_low_c": gaussian_inputs.get("sigma_low_c"),
        "source_names": gaussian_inputs.get("source_names") or [],
    }


def _infer_predicted_side(
    predicted_prob: float,
    evidence: list[dict],
    actual_market_outcome: float | None,
) -> str:
    for item in evidence:
        if not isinstance(item, dict):
            continue
        content = str(item.get("content", ""))
        match = re.search(
            r"Predicted YES=(?P<predicted>[-0-9.]+), buy YES ask=(?P<yes>[-0-9.]+), buy NO ask=(?P<no>[-0-9.]+)",
            content,
            re.IGNORECASE,
        )
        if match:
            predicted_yes = float(match.group("predicted"))
            yes_ask = float(match.group("yes"))
            no_ask = float(match.group("no"))
            yes_edge = predicted_yes - yes_ask
            no_edge = (1.0 - predicted_yes) - no_ask
            return "yes" if yes_edge >= no_edge else "no"

    if actual_market_outcome is not None and predicted_prob in {0.0, 1.0}:
        return "yes" if predicted_prob == actual_market_outcome else "no"
    return "no" if predicted_prob < 0.5 else "yes"


async def on_market_resolved(
    db: ArenaDB,
    market_id: str,
    venue: str,
    winning_outcome_id: str,
    resolution_data: dict | None = None,
) -> None:
    resolution_data = resolution_data or {}
    crps_tracker = CRPSTracker()
    market_context = _load_market_context(db, market_id, venue)
    if not market_context:
        logger.warning("Skipping resolution hook enrichment for %s/%s: market row not found", market_id, venue)
        return

    parsed_weather = None
    if str(market_context.get("category")) == "weather":
        parsed_weather = _parse_weather_market_question(str(market_context.get("question", "")))
        if not parsed_weather:
            logger.warning("Could not parse weather market question for %s: %s", market_id, market_context.get("question"))
    actual_market_outcome = resolution_data.get("actual_outcome")
    yes_outcome_id = market_context.get("yes_outcome_id")
    if actual_market_outcome is None and yes_outcome_id is not None:
        actual_market_outcome = 1.0 if str(winning_outcome_id) == str(yes_outcome_id) else 0.0
        resolution_data["actual_outcome"] = actual_market_outcome

    # A) Backfill forecast actuals
    location = resolution_data.get("location") or (parsed_weather or {}).get("city")
    target_date = resolution_data.get("target_date") or (parsed_weather or {}).get("target_date")
    actual_high = resolution_data.get("actual_high_c")
    actual_low = resolution_data.get("actual_low_c")
    threshold_c = resolution_data.get("threshold_c") or (parsed_weather or {}).get("threshold_c")
    observation_details: dict[str, object] = {
        "source": resolution_data.get("observation_source"),
        "timestamp": resolution_data.get("observation_timestamp"),
        "secondary_source": resolution_data.get("observation_secondary_source"),
        "secondary_high_c": resolution_data.get("observation_secondary_high_c"),
        "disagreement_c": resolution_data.get("observation_disagreement_c"),
    }
    resolution_data["location"] = location
    resolution_data["target_date"] = target_date
    if threshold_c is not None:
        resolution_data["threshold_c"] = threshold_c

    if location and target_date and actual_high is None:
        coords = CITY_COORDS.get(str(location).lower())
        if coords is None:
            logger.warning("Skipping CRPS lookup for %s: no coordinates for %s", market_id, location)
        else:
            observed = await get_daily_observed_temperature_details(
                db,
                latitude=coords[0],
                longitude=coords[1],
                location_name=str(location),
                target_date=str(target_date),
            )
            actual_high = observed.get("actual_high_c")
            actual_low = observed.get("actual_low_c")
            if actual_high is not None:
                resolution_data["actual_high_c"] = actual_high
                resolution_data["actual_low_c"] = actual_low
                resolution_data["observation_source"] = observed.get("observation_source")
                resolution_data["observation_timestamp"] = observed.get("observation_timestamp")
                resolution_data["observation_secondary_source"] = observed.get("observation_secondary_source")
                resolution_data["observation_secondary_high_c"] = observed.get("observation_secondary_high_c")
                resolution_data["observation_disagreement_c"] = observed.get("observation_disagreement_c")
                observation_details = {
                    "source": observed.get("observation_source"),
                    "timestamp": observed.get("observation_timestamp"),
                    "secondary_source": observed.get("observation_secondary_source"),
                    "secondary_high_c": observed.get("observation_secondary_high_c"),
                    "disagreement_c": observed.get("observation_disagreement_c"),
                }
            else:
                logger.warning(
                    "Skipping CRPS for %s: no observed high available for %s on %s",
                    market_id,
                    location,
                    target_date,
                )

    if location and target_date and actual_high is not None:
        from arena.data_sources.weather_bias import backfill_actuals
        updated = await backfill_actuals(db, location, target_date, actual_high, actual_low or 0.0)
        logger.info(f"Backfilled {updated} forecast history rows for {location} {target_date}")

    latest_decision_context = _load_latest_decision_context(db, market_id)

    # B) Score every decision on this market
    with db.connect() as conn:
        decision_rows = list(conn.execute(
            "SELECT decision_id, strategy_id, predicted_probability, actions_json, evidence_items_json "
            "FROM decisions WHERE markets_considered_json LIKE ?",
            (f'%{market_id}%',),
        ))

    for row in decision_rows:
        predicted_prob = row["predicted_probability"]
        if predicted_prob is None:
            continue

        evidence_json = row["evidence_items_json"]
        evidence = json.loads(evidence_json) if isinstance(evidence_json, str) else (evidence_json or [])

        actions_json = row["actions_json"]
        actions = json.loads(actions_json) if isinstance(actions_json, str) else (actions_json or [])
        relevant_action = None
        for action in actions:
            if isinstance(action, dict) and str(action.get("market_id")) == str(market_id):
                relevant_action = action
                break

        if relevant_action:
            traded_outcome = str(relevant_action.get("outcome_id", ""))
            actual_outcome = 1.0 if traded_outcome == str(winning_outcome_id) else 0.0
        else:
            inferred_side = _infer_predicted_side(float(predicted_prob), evidence, actual_market_outcome)
            if actual_market_outcome is None:
                logger.warning("Skipping decision score for %s/%s: unresolved actual outcome", row["decision_id"], market_id)
                continue
            actual_outcome = float(actual_market_outcome) if inferred_side == "yes" else 1.0 - float(actual_market_outcome)

        brier = _score_decision(predicted_prob, actual_outcome)

        forecast_error_c = None
        if actual_high is not None:
            for ev in evidence:
                if isinstance(ev, dict) and "forecast" in str(ev.get("source", "")).lower():
                    content = str(ev.get("content", ""))
                    match = re.search(r"(\d+\.?\d*)C", content)
                    if match:
                        forecast_temp = float(match.group(1))
                        forecast_error_c = round(forecast_temp - actual_high, 2)
                        break

        with db.connect() as conn:
            conn.execute(
                "INSERT INTO decision_scores "
                "(decision_id, market_id, strategy_id, predicted_probability, actual_outcome, brier_score, forecast_error_c) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (row["decision_id"], market_id, row["strategy_id"], predicted_prob, actual_outcome, round(brier, 6), forecast_error_c),
            )

    forecast_metric = str((parsed_weather or {}).get("metric") or "high").lower()
    observed_temp_c = float(actual_low) if forecast_metric == "low" and actual_low is not None else actual_high
    if observed_temp_c is not None and latest_decision_context:
        forecast_mu_c = (
            latest_decision_context.get("mu_low_c") if forecast_metric == "low" else latest_decision_context.get("mu_high_c")
        )
        forecast_sigma_c = (
            latest_decision_context.get("sigma_low_c")
            if forecast_metric == "low"
            else latest_decision_context.get("sigma_high_c")
        )
        source_names = latest_decision_context.get("source_names") or []

        if forecast_mu_c is not None and forecast_sigma_c is not None and forecast_sigma_c > 0:
            city = str(location or resolution_data.get("location") or "unknown")
            source_forecasts = _load_source_forecasts(db, city, target_date, source_names, metric=forecast_metric)
            crps_tracker.record(
                market_id=market_id,
                observation=_c_to_f(float(observed_temp_c)),
                mu=_c_to_f(float(forecast_mu_c)),
                sigma=_delta_c_to_f(float(forecast_sigma_c)),
                city=city,
                target_date=target_date or "",
                metric=forecast_metric,
                sources=source_forecasts,
                observed_high_c=float(actual_high),
                observed_low_c=float(actual_low) if actual_low is not None else None,
                observation_source=str(observation_details.get("source") or "unknown"),
                observation_timestamp=str(observation_details.get("timestamp") or ""),
                observation_secondary_source=str(observation_details.get("secondary_source") or ""),
                observation_secondary_high_c=(
                    float(observation_details["secondary_high_c"])
                    if observation_details.get("secondary_high_c") is not None else None
                ),
                observation_secondary_low_c=None,
                observation_disagreement_c=(
                    float(observation_details["disagreement_c"])
                    if observation_details.get("disagreement_c") is not None else None
                ),
            )

            if parsed_weather and actual_market_outcome is not None:
                forecast_probability = _compute_market_probability(parsed_weather, float(forecast_mu_c), float(forecast_sigma_c))
                if forecast_probability is not None:
                    crps_tracker.record_brier(
                        city=city,
                        target_date=target_date or "",
                        market_id=market_id,
                        question=str(market_context.get("question") or ""),
                        forecast_prob=forecast_probability,
                        actual_outcome=float(actual_market_outcome),
                        metric=forecast_metric,
                        observed_high_c=float(actual_high),
                        observed_low_c=float(actual_low) if actual_low is not None else None,
                        observation_source=str(observation_details.get("source") or "unknown"),
                        observation_timestamp=str(observation_details.get("timestamp") or ""),
                        observation_secondary_source=str(observation_details.get("secondary_source") or ""),
                        observation_secondary_high_c=(
                            float(observation_details["secondary_high_c"])
                            if observation_details.get("secondary_high_c") is not None else None
                        ),
                        observation_secondary_low_c=None,
                        observation_disagreement_c=(
                            float(observation_details["disagreement_c"])
                            if observation_details.get("disagreement_c") is not None else None
                        ),
                    )

            _write_sigma_adjustment_for_city(
                db,
                crps_tracker,
                city=city,
                metric=forecast_metric,
                current_sigma_c=float(forecast_sigma_c),
            )
        elif str(market_context.get("category")) == "weather":
            logger.warning(
                "Skipping CRPS for %s: missing Gaussian evidence (mu=%s sigma=%s)",
                market_id,
                forecast_mu_c,
                forecast_sigma_c,
            )

    # C) Compute rolling strategy metrics
    scored_strategies: set[str] = set()
    for row in decision_rows:
        if row["predicted_probability"] is not None:
            scored_strategies.add(row["strategy_id"])

    for strategy_id in scored_strategies:
        await _compute_rolling_metrics(db, strategy_id)


async def _compute_rolling_metrics(db: ArenaDB, strategy_id: str) -> None:
    with db.connect() as conn:
        rows = list(conn.execute(
            "SELECT predicted_probability, actual_outcome, brier_score, forecast_error_c "
            "FROM decision_scores WHERE strategy_id = ? "
            "ORDER BY created_at DESC LIMIT 50",
            (strategy_id,),
        ))

    if not rows:
        return

    sample_size = len(rows)
    brier_scores = [float(r["brier_score"]) for r in rows]
    rolling_brier = round(statistics.mean(brier_scores), 4)

    predictions = [float(r["predicted_probability"]) for r in rows]
    actuals = [float(r["actual_outcome"]) for r in rows]
    calibration_error = _compute_calibration_error(predictions, actuals)

    forecast_errors = [float(r["forecast_error_c"]) for r in rows if r["forecast_error_c"] is not None]
    mean_forecast_error_c = round(statistics.mean(forecast_errors), 3) if forecast_errors else None

    overconfident_count = 0
    total_count = 0
    for pred, act in zip(predictions, actuals):
        if pred > 0.7 or pred < 0.3:
            total_count += 1
            if (pred > 0.7 and act == 0.0) or (pred < 0.3 and act == 1.0):
                overconfident_count += 1
    overconfidence_rate = round(overconfident_count / total_count, 4) if total_count > 0 else None

    with db.connect() as conn:
        conn.execute(
            "INSERT INTO strategy_health "
            "(strategy_id, sample_size, rolling_brier, calibration_error, mean_forecast_error_c, overconfidence_rate) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (strategy_id, sample_size, rolling_brier, calibration_error, mean_forecast_error_c, overconfidence_rate),
        )

    # D) Auto-adjust parameters
    await _generate_adjustments(db, strategy_id, rolling_brier, calibration_error, mean_forecast_error_c, overconfidence_rate, sample_size)


async def _generate_adjustments(
    db: ArenaDB,
    strategy_id: str,
    rolling_brier: float,
    calibration_error: float,
    mean_forecast_error_c: float | None,
    overconfidence_rate: float | None,
    sample_size: int,
) -> None:
    adjustments: list[tuple[str, float, float, str]] = []

    if rolling_brier > 0.30:
        adjustments.append(("min_edge_bps", 300, 400, f"Brier score {rolling_brier:.3f} worse than random — raising threshold"))

    if overconfidence_rate is not None and overconfidence_rate > 0.4:
        adjustments.append(("max_confidence", 1.0, 0.85, f"Overconfident {overconfidence_rate:.0%} of the time — capping confidence"))

    if rolling_brier < 0.15 and sample_size >= 20:
        adjustments.append(("min_edge_bps", 300, 250, f"Strong calibration (Brier {rolling_brier:.3f}) — can trade tighter edges"))

    for param_name, current, recommended, reason in adjustments:
        logger.warning(f"CALIBRATION ADJUSTMENT: {param_name} {current} -> {recommended}: {reason}")
        _write_parameter_adjustment(
            db,
            strategy_id=strategy_id,
            parameter_name=param_name,
            current_value=current,
            recommended_value=recommended,
            reason=reason,
            city=None,
        )
