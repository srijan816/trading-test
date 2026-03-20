from __future__ import annotations

import asyncio
import json
import logging
import math
import sqlite3
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import httpx

from arena.data_sources.weather_constants import SOURCE_BIAS
from arena.data_sources.nvidia_fourcastnet import fetch_fourcastnet_forecast

logger = logging.getLogger(__name__)

TIMEOUT = 10.0
CALIBRATION_PATH = Path(__file__).resolve().parents[3] / "data" / "sigma_calibration.json"

WEATHERCODE_MAP = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Rime fog", 51: "Light drizzle", 53: "Moderate drizzle",
    55: "Dense drizzle", 61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Heavy hail",
}


class WeatherDataUnavailable(Exception):
    pass


SOURCE_RMSE = {
    "ecmwf": {
        "Hong Kong": 1.55, "Chicago": 1.37, "London": 0.81,
        "Tokyo": 1.03, "Seoul": 0.62, "Lucknow": 0.80,
        "_default": 1.08,
    },
    "gfs": {
        "Hong Kong": 0.75, "Chicago": 0.00, "London": 0.93,
        "Tokyo": 1.19, "Seoul": 1.26, "Lucknow": 2.07,
        "_default": 1.20,
    },
    "open_meteo": {
        "_default": 1.10,
    },
    "hko": {
        "Hong Kong": 0.90,
        "_default": 1.00,
    },
    "nvidia_fourcastnet": {
        "_default": 1.50,
    },
}

CALIBRATION_SOURCE_MAP = {
    "ecmwf": "ecmwf_ifs025",
    "gfs": "gfs_seamless",
    "open_meteo": "open_meteo",
    "hko": "hko",
    "nvidia_fourcastnet": "nvidia_fourcastnet",
}



@lru_cache(maxsize=1)
def _load_calibration() -> dict:
    calibration = {
        "best_sigma_multiplier": 1.10,
        "per_location": {},
        "source_rmse": SOURCE_RMSE,
        "source_bias": SOURCE_BIAS,
        "loaded_from": "hardcoded defaults",
    }
    if not CALIBRATION_PATH.exists():
        logger.info("sigma_calibration.json not found — using hardcoded RMSE/bias defaults")
        return calibration

    try:
        payload = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning(f"Failed to load sigma calibration from {CALIBRATION_PATH}: {exc} — using hardcoded defaults")
        return calibration

    rmse_by_source = {
        source: dict(defaults)
        for source, defaults in SOURCE_RMSE.items()
    }
    source_rmse_payload = payload.get("source_rmse", {})
    for source, calibration_key in CALIBRATION_SOURCE_MAP.items():
        source_payload = source_rmse_payload.get(calibration_key, {})
        rmse_by_source[source]["_default"] = float(source_payload.get("global", rmse_by_source[source].get("_default", 1.5)))
        for location, rmse in (source_payload.get("per_location") or {}).items():
            rmse_by_source[source][location] = float(rmse)

    calibration = {
        "best_sigma_multiplier": float(payload.get("best_sigma_multiplier", 1.10) or 1.10),
        "per_location": payload.get("per_location", {}) or {},
        "source_rmse": rmse_by_source,
        "source_bias": SOURCE_BIAS,
        "loaded_from": str(CALIBRATION_PATH),
    }
    logger.info(f"Loaded sigma calibration from {CALIBRATION_PATH}")
    return calibration


def _lookup_rmse(calibration: dict, source: str, location: str) -> float:
    source_rmse = calibration.get("source_rmse", {}).get(source, {})
    rmse = float(source_rmse.get(location, source_rmse.get("_default", 1.5)))
    if rmse < 0.10:
        default_rmse = float(source_rmse.get("_default", 1.5))
        logger.warning(
            f"Source {source} for {location} has suspiciously low RMSE ({rmse:.2f}C) — using default RMSE instead"
        )
        return default_rmse
    return rmse


def _lookup_bias(calibration: dict, source: str, location: str) -> float:
    source_bias = calibration.get("source_bias", {}).get(source, {})
    return float(source_bias.get(location, source_bias.get("_default", 0.0)))


def _is_hong_kong(latitude: float, longitude: float) -> bool:
    return abs(latitude - 22.3) < 1.0 and abs(longitude - 114.2) < 1.0


def _make_standardized(
    source: str,
    location: str,
    target_date: str,
    high_c: float | None,
    low_c: float | None,
    conditions: str,
    raw_response: dict,
) -> dict:
    return {
        "source": source,
        "location": location,
        "forecast_time": datetime.now(timezone.utc).isoformat(),
        "target_date": target_date,
        "temp_high_c": high_c,
        "temp_low_c": low_c,
        "conditions": conditions,
        "raw_response": raw_response,
    }


async def _fetch_open_meteo_generic(
    url: str,
    source_name: str,
    latitude: float,
    longitude: float,
    location_name: str,
    target_date: str,
) -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.get(
                url,
                params={
                    "latitude": latitude,
                    "longitude": longitude,
                    "daily": "temperature_2m_max,temperature_2m_min,weathercode",
                    "timezone": "auto",
                    "start_date": target_date,
                    "end_date": target_date,
                },
            )
            response.raise_for_status()
            payload = response.json()
        daily = payload.get("daily", {})
        dates = daily.get("time", [])
        highs = daily.get("temperature_2m_max", [])
        lows = daily.get("temperature_2m_min", [])
        codes = daily.get("weathercode", [])
        if not dates:
            logger.warning(f"Weather source {source_name} returned no data for {target_date}")
            return None
        idx = 0
        for i, d in enumerate(dates):
            if d == target_date:
                idx = i
                break
        high_c = highs[idx] if idx < len(highs) else None
        low_c = lows[idx] if idx < len(lows) else None
        code = codes[idx] if idx < len(codes) else None
        conditions = WEATHERCODE_MAP.get(code, f"Code {code}") if code is not None else ""
        if high_c is None:
            logger.warning(f"Weather source {source_name} returned null high for {target_date}")
            return None
        return _make_standardized(source_name, location_name, target_date, high_c, low_c, conditions, payload)
    except Exception as e:
        logger.warning(f"Weather source {source_name} failed: {e}")
        return None


async def _fetch_open_meteo(latitude: float, longitude: float, location_name: str, target_date: str) -> dict | None:
    return await _fetch_open_meteo_generic(
        "https://api.open-meteo.com/v1/forecast", "open_meteo", latitude, longitude, location_name, target_date
    )


async def _fetch_gfs(latitude: float, longitude: float, location_name: str, target_date: str) -> dict | None:
    return await _fetch_open_meteo_generic(
        "https://api.open-meteo.com/v1/gfs", "gfs", latitude, longitude, location_name, target_date
    )


async def _fetch_ecmwf(latitude: float, longitude: float, location_name: str, target_date: str) -> dict | None:
    return await _fetch_open_meteo_generic(
        "https://api.open-meteo.com/v1/ecmwf", "ecmwf", latitude, longitude, location_name, target_date
    )


async def _fetch_hko(location_name: str, target_date: str) -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.get(
                "https://data.weather.gov.hk/weatherAPI/opendata/weather.php",
                params={"dataType": "fnd", "lang": "en"},
            )
            response.raise_for_status()
            payload = response.json()
        target_compact = target_date.replace("-", "")
        high_c = None
        low_c = None
        conditions = ""
        for item in payload.get("weatherForecast", []):
            if item.get("forecastDate") == target_compact:
                high_c = float(item.get("forecastMaxtemp", {}).get("value", 0) or 0)
                low_c = float(item.get("forecastMintemp", {}).get("value", 0) or 0)
                conditions = item.get("forecastWeather", "")
                break
        if high_c is None or high_c == 0:
            logger.warning(f"Weather source hko returned no data for {target_date}")
            return None
        return _make_standardized("hko", location_name, target_date, high_c, low_c, conditions, payload)
    except Exception as e:
        logger.warning(f"Weather source hko failed: {e}")
        return None


async def _fetch_nvidia_fourcastnet(
    latitude: float,
    longitude: float,
    location_name: str,
    target_date: str,
) -> dict | None:
    try:
        high_f = await asyncio.to_thread(fetch_fourcastnet_forecast, latitude, longitude, target_date)
    except Exception as exc:
        logger.warning(f"Weather source nvidia_fourcastnet failed: {exc}")
        return None

    if high_f is None:
        return None

    high_c = (float(high_f) - 32.0) * 5.0 / 9.0
    return _make_standardized(
        "nvidia_fourcastnet",
        location_name,
        target_date,
        high_c,
        None,
        "FourCastNet polling forecast",
        {"forecast_high_f": round(float(high_f), 2)},
    )


def _resolve_db_path(db_path_or_db) -> Path | None:
    if db_path_or_db is None:
        return None
    if isinstance(db_path_or_db, (str, Path)):
        return Path(db_path_or_db)
    candidate = getattr(db_path_or_db, "path", None)
    return Path(candidate) if candidate else None


def _sigma_parameter_names(metric: str | None) -> list[str]:
    normalized = str(metric or "high").strip().lower()
    if normalized == "low":
        return ["ensemble_sigma_low", "ensemble_sigma"]
    return ["ensemble_sigma_high", "ensemble_sigma"]


def load_latest_sigma(db_path: str | Path, city: str, metric: str = "high") -> float | None:
    """Load and mark the most recent city+metric sigma recommendation."""
    db_file = Path(db_path)
    if not db_file.exists():
        return None

    conn = sqlite3.connect(db_file)
    try:
        conn.row_factory = sqlite3.Row
        columns = {row[1] for row in conn.execute("PRAGMA table_info(parameter_adjustments)").fetchall()}
        if "city" not in columns:
            return None
        for parameter_name in _sigma_parameter_names(metric):
            row = conn.execute(
                """
                SELECT id, recommended_value
                FROM parameter_adjustments
                WHERE lower(city) = lower(?) AND parameter_name = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (city, parameter_name),
            ).fetchone()
            if row is None:
                continue
            adj_id, recommended_sigma = int(row["id"]), float(row["recommended_value"])
            if recommended_sigma <= 0:
                return None
            conn.execute("UPDATE parameter_adjustments SET auto_applied = 1 WHERE id = ?", (adj_id,))
            conn.commit()
            logger.info(
                "Loaded calibrated sigma for %s/%s from parameter_adjustments: %.3fC",
                city,
                metric,
                recommended_sigma,
            )
            return recommended_sigma
        return None
    finally:
        conn.close()


async def get_ensemble_forecast(
    latitude: float,
    longitude: float,
    location_name: str,
    target_date: str,
    db=None,
) -> dict:
    tasks = [
        _fetch_open_meteo(latitude, longitude, location_name, target_date),
        _fetch_gfs(latitude, longitude, location_name, target_date),
        _fetch_ecmwf(latitude, longitude, location_name, target_date),
        _fetch_nvidia_fourcastnet(latitude, longitude, location_name, target_date),
    ]
    if _is_hong_kong(latitude, longitude):
        tasks.append(_fetch_hko(location_name, target_date))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    forecasts: list[dict] = []
    for r in results:
        if isinstance(r, Exception):
            logger.warning(f"Ensemble fetcher raised: {r}")
            continue
        if r is not None:
            forecasts.append(r)

    # Record each forecast for future bias tracking (Task 5)
    if db is not None:
        from arena.data_sources.weather_bias import ensure_forecast_history_table, record_forecast

        try:
            ensure_forecast_history_table(db)
            for f in forecasts:
                await record_forecast(db, location_name, f["source"], target_date, f["temp_high_c"], f["temp_low_c"])
        except Exception as e:
            logger.warning(f"Failed to record forecast history: {e}")

    if not forecasts:
        raise WeatherDataUnavailable(f"No weather sources returned data for {location_name} on {target_date}")

    calibration = _load_calibration()
    weighted_high_components: list[tuple[float, float]] = []
    weighted_low_components: list[tuple[float, float]] = []
    bias_corrections_applied: dict[str, float] = {}
    source_weights: dict[str, float] = {}

    for forecast in forecasts:
        high_c = forecast.get("temp_high_c")
        if high_c is None:
            continue
        source = str(forecast["source"])
        rmse = _lookup_rmse(calibration, source, location_name)
        weight = 1.0 / (rmse ** 2)
        bias = _lookup_bias(calibration, source, location_name)
        corrected_high = float(high_c) - bias
        weighted_high_components.append((weight, corrected_high))
        source_weights[source] = weight
        bias_corrections_applied[source] = bias

        low_c = forecast.get("temp_low_c")
        if low_c is not None:
            corrected_low = float(low_c) - bias
            weighted_low_components.append((weight, corrected_low))

    if not weighted_high_components:
        raise WeatherDataUnavailable(f"All weather sources returned null high temperature for {location_name}")

    total_weight = sum(weight for weight, _ in weighted_high_components)
    mean_high = sum(weight * forecast_high for weight, forecast_high in weighted_high_components) / total_weight
    mean_low = None
    if weighted_low_components:
        total_low_weight = sum(weight for weight, _ in weighted_low_components)
        mean_low = sum(weight * forecast_low for weight, forecast_low in weighted_low_components) / total_low_weight

    sigma_mult = float(
        (calibration.get("per_location", {}).get(location_name, {}) or {}).get(
            "sigma_mult",
            calibration.get("best_sigma_multiplier", 1.10),
        )
    )
    weighted_sigma = 1.0 / math.sqrt(total_weight)
    default_sigma = max(weighted_sigma * sigma_mult, 0.5)

    # Priority chain for sigma selection:
    # 1. Latest city-specific recommendation in parameter_adjustments
    # 2. sigma_calibration.json / hardcoded calibration multiplier
    # 3. Raw inverse-variance ensemble spread
    db_path = _resolve_db_path(db)
    adjusted_sigma_high = load_latest_sigma(db_path, location_name, metric="high") if db_path is not None else None
    adjusted_sigma_low = load_latest_sigma(db_path, location_name, metric="low") if db_path is not None else None
    if adjusted_sigma_high is not None:
        logger.info(
            "Using calibrated high sigma for %s: %.3fC (from parameter_adjustments)",
            location_name,
            adjusted_sigma_high,
        )
        final_sigma_high = max(float(adjusted_sigma_high), 0.5)
        sigma_source_high = "parameter_adjustments"
    else:
        logger.info("Using default high sigma for %s: %.3fC", location_name, default_sigma)
        final_sigma_high = default_sigma
        sigma_source_high = "sigma_calibration.json" if calibration.get("loaded_from") != "hardcoded defaults" else "default"

    if mean_low is not None:
        if adjusted_sigma_low is not None:
            logger.info(
                "Using calibrated low sigma for %s: %.3fC (from parameter_adjustments)",
                location_name,
                adjusted_sigma_low,
            )
            final_sigma_low = max(float(adjusted_sigma_low), 0.5)
            sigma_source_low = "parameter_adjustments"
        else:
            final_sigma_low = default_sigma
            sigma_source_low = "sigma_calibration.json" if calibration.get("loaded_from") != "hardcoded defaults" else "default"
    else:
        final_sigma_low = None
        sigma_source_low = None

    source_count = len(forecasts)
    if source_count >= 3:
        confidence = "high"
    elif source_count == 2:
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "location": location_name,
        "target_date": target_date,
        "sources_used": source_count,
        "source_names": [f["source"] for f in forecasts],
        "raw_forecasts": forecasts,
        "ensemble_high_c": round(mean_high, 2),
        "ensemble_low_c": round(mean_low, 2) if mean_low is not None else None,
        "ensemble_method": "rmse_weighted",
        "ensemble_sigma_c": round(final_sigma_high, 2),
        "ensemble_sigma_high_c": round(final_sigma_high, 2),
        "ensemble_sigma_low_c": round(final_sigma_low, 2) if final_sigma_low is not None else None,
        "sigma_multiplier_used": round(final_sigma_high / weighted_sigma, 3) if weighted_sigma > 0 else None,
        "sigma_multiplier_high_used": round(final_sigma_high / weighted_sigma, 3) if weighted_sigma > 0 else None,
        "sigma_multiplier_low_used": (
            round(final_sigma_low / weighted_sigma, 3) if weighted_sigma > 0 and final_sigma_low is not None else None
        ),
        "sigma_source": sigma_source_high,
        "sigma_source_high": sigma_source_high,
        "sigma_source_low": sigma_source_low,
        "bias_correction_applied_c": 0.0,
        "bias_corrections_applied": {source: round(bias, 3) for source, bias in bias_corrections_applied.items()},
        "source_weights": {source: round(weight, 6) for source, weight in source_weights.items()},
        "calibration_source": calibration.get("loaded_from"),
        "confidence": confidence,
    }
