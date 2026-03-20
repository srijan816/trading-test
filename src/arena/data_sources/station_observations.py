from __future__ import annotations

import asyncio
import logging
import statistics
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import httpx

from arena.db import ArenaDB
from arena.data_sources.weather_constants import CITY_TIMEZONES

logger = logging.getLogger(__name__)

TIMEOUT = 10.0


class ObservationUnavailable(Exception):
    pass

ICAO_CODES = {
    "hong kong": "VHHH",
    "chicago": "KORD",
    "london": "EGLL",
    "lucknow": "VILK",
    "seoul": "RKSI",
    "tokyo": "RJTT",
    "new york": "KJFK",
    "atlanta": "KATL",
    "ankara": "LTAC",
    "buenos aires": "SAEZ",
    "seattle": "KSEA",
    "toronto": "CYYZ",
    "taipei": "RCTP",
}


async def _fetch_open_meteo_current(
    latitude: float, longitude: float, location_name: str
) -> dict | None:
    """Fetch current conditions from Open-Meteo."""
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": latitude,
                    "longitude": longitude,
                    "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
                    "timezone": "auto",
                },
            )
            response.raise_for_status()
            payload = response.json()
        current = payload.get("current", {})
        temp = current.get("temperature_2m")
        if temp is None:
            return None
        obs_time = current.get("time", datetime.now(timezone.utc).isoformat())
        return {
            "source": "open_meteo_current",
            "location": location_name,
            "observation_time": obs_time,
            "temperature_c": float(temp),
            "is_current": True,
            "hourly_trajectory": None,
            "raw_response": payload,
        }
    except Exception as e:
        logger.warning(f"Open-Meteo current fetch failed for {location_name}: {e}")
        return None


async def _fetch_metar(
    latitude: float, longitude: float, location_name: str
) -> dict | None:
    """Fetch METAR observation from aviationweather.gov."""
    icao = ICAO_CODES.get(location_name.lower())
    if not icao:
        return None
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.get(
                "https://aviationweather.gov/api/data/metar",
                params={"ids": icao, "format": "json"},
            )
            response.raise_for_status()
            payload = response.json()
        if not payload:
            return None
        obs = payload[0] if isinstance(payload, list) else payload
        temp = obs.get("temp")
        if temp is None:
            return None
        obs_time = obs.get("reportTime", obs.get("obsTime", datetime.now(timezone.utc).isoformat()))
        return {
            "source": "metar",
            "location": location_name,
            "observation_time": obs_time,
            "temperature_c": float(temp),
            "is_current": True,
            "hourly_trajectory": None,
            "raw_response": obs,
        }
    except Exception as e:
        logger.warning(f"METAR fetch failed for {location_name} ({icao}): {e}")
        return None


async def _fetch_hourly_trajectory(
    latitude: float, longitude: float, location_name: str
) -> dict | None:
    """Fetch last 12 hours of hourly observations from Open-Meteo."""
    try:
        now = datetime.now(timezone.utc)
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": latitude,
                    "longitude": longitude,
                    "hourly": "temperature_2m",
                    "timezone": "auto",
                    "past_hours": 12,
                    "forecast_days": 1,
                },
            )
            response.raise_for_status()
            payload = response.json()
        hourly = payload.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        if not times or not temps:
            return None
        # Only keep past/current hours (filter out future forecast data)
        now_str = now.strftime("%Y-%m-%dT%H:00")
        trajectory = []
        latest_temp = None
        for t, temp in zip(times, temps):
            if t is None or temp is None:
                continue
            if t > now_str:
                break  # Stop at future hours
            trajectory.append({"hour": t, "temp_c": float(temp)})
            latest_temp = float(temp)
        if not trajectory:
            return None
        return {
            "source": "open_meteo_trajectory",
            "location": location_name,
            "observation_time": trajectory[-1]["hour"],
            "temperature_c": latest_temp,
            "is_current": False,
            "hourly_trajectory": trajectory,
            "raw_response": payload,
        }
    except Exception as e:
        logger.warning(f"Hourly trajectory fetch failed for {location_name}: {e}")
        return None


def _estimate_hours_remaining(longitude: float) -> float:
    """Estimate hours of daylight remaining based on longitude and current UTC time."""
    now = datetime.now(timezone.utc)
    # Approximate local solar time offset from UTC
    local_offset_hours = longitude / 15.0
    local_hour = (now.hour + now.minute / 60.0 + local_offset_hours) % 24
    # Approximate sunset at ~18:30 local solar time (varies by season but reasonable default)
    sunset_hour = 18.5
    remaining = max(sunset_hour - local_hour, 0.0)
    return round(remaining, 1)


def _compute_trending(trajectory: list[dict]) -> str:
    """Determine temperature trend from last 3 hours of trajectory data."""
    if len(trajectory) < 2:
        return "stable"
    recent = trajectory[-3:] if len(trajectory) >= 3 else trajectory
    temps = [pt["temp_c"] for pt in recent]
    if len(temps) < 2:
        return "stable"
    delta = temps[-1] - temps[0]
    if delta > 0.5:
        return "warming"
    elif delta < -0.5:
        return "cooling"
    return "stable"


async def get_current_observations(
    latitude: float,
    longitude: float,
    location_name: str,
) -> dict:
    """Fetch real-time weather observations from multiple sources.

    Returns a consolidated observation dict with current temperature,
    trajectory, trend analysis, and daylight remaining.
    """
    tasks = [
        _fetch_open_meteo_current(latitude, longitude, location_name),
        _fetch_metar(latitude, longitude, location_name),
        _fetch_hourly_trajectory(latitude, longitude, location_name),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    observations: list[dict] = []
    for r in results:
        if isinstance(r, Exception):
            logger.warning(f"Observation fetcher raised: {r}")
            continue
        if r is not None:
            observations.append(r)

    if not observations:
        raise ObservationUnavailable(f"No observation sources returned data for {location_name}")

    # Collect current temperature readings
    current_temps: list[float] = []
    source_names: list[str] = []
    trajectory: list[dict] = []
    obs_time = datetime.now(timezone.utc).isoformat()

    for obs in observations:
        source_names.append(obs["source"])
        if obs["is_current"] and obs["temperature_c"] is not None:
            current_temps.append(obs["temperature_c"])
            # Prefer observation time from current sources over trajectory
            obs_time = obs["observation_time"]
        if obs.get("hourly_trajectory"):
            trajectory = obs["hourly_trajectory"]

    # If we have no current temps but have trajectory, use the latest trajectory point
    if not current_temps and trajectory:
        current_temps.append(trajectory[-1]["temp_c"])

    if not current_temps:
        raise ObservationUnavailable(f"No temperature readings available for {location_name}")

    current_temp = statistics.median(current_temps)

    # Compute trajectory stats
    all_temps = [pt["temp_c"] for pt in trajectory] if trajectory else current_temps
    max_temp_so_far = max(all_temps)
    min_temp_so_far = min(all_temps)
    trending = _compute_trending(trajectory) if trajectory else "stable"
    hours_remaining = _estimate_hours_remaining(longitude)

    return {
        "location": location_name,
        "observation_time": obs_time,
        "current_temp_c": round(current_temp, 1),
        "max_temp_so_far_c": round(max_temp_so_far, 1),
        "min_temp_so_far_c": round(min_temp_so_far, 1),
        "trending": trending,
        "temp_trajectory": trajectory,
        "hours_remaining": hours_remaining,
        "sources_used": len(observations),
        "source_names": source_names,
    }


def _query_cached_daily_temperatures(
    db: ArenaDB,
    location_name: str,
    target_date: str,
) -> tuple[float | None, float | None]:
    with db.connect() as conn:
        station_row = conn.execute(
            """
            SELECT
                MAX(temperature_c) AS actual_high_c,
                MIN(temperature_c) AS actual_low_c
            FROM station_observations
            WHERE lower(location) = lower(?) AND substr(observation_time, 1, 10) = ?
            """,
            (location_name, target_date),
        ).fetchone()
        if station_row and station_row["actual_high_c"] is not None:
            return float(station_row["actual_high_c"]), float(station_row["actual_low_c"])

        forecast_row = conn.execute(
            """
            SELECT
                MAX(actual_high_c) AS actual_high_c,
                MIN(actual_low_c) AS actual_low_c
            FROM forecast_history
            WHERE lower(location) = lower(?) AND target_date = ? AND actual_high_c IS NOT NULL
            """,
            (location_name, target_date),
        ).fetchone()
        if forecast_row and forecast_row["actual_high_c"] is not None:
            low = forecast_row["actual_low_c"]
            return float(forecast_row["actual_high_c"]), float(low) if low is not None else None

    return None, None


async def _fetch_open_meteo_daily_archive(
    latitude: float,
    longitude: float,
    target_date: str,
) -> dict[str, object]:
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.get(
                "https://archive-api.open-meteo.com/v1/archive",
                params={
                    "latitude": latitude,
                    "longitude": longitude,
                    "daily": "temperature_2m_max,temperature_2m_min",
                    "timezone": "auto",
                    "start_date": target_date,
                    "end_date": target_date,
                },
            )
            response.raise_for_status()
            payload = response.json()
        daily = payload.get("daily", {})
        high = daily.get("temperature_2m_max", [None])[0]
        low = daily.get("temperature_2m_min", [None])[0]
        return {
            "actual_high_c": float(high) if high is not None else None,
            "actual_low_c": float(low) if low is not None else None,
            "observation_source": "open_meteo_archive",
            "observation_timestamp": f"{target_date}T23:59:59",
            "raw_response": payload,
        }
    except Exception as exc:
        logger.warning("Open-Meteo archive fetch failed for %s %s: %s", latitude, longitude, exc)
        return {
            "actual_high_c": None,
            "actual_low_c": None,
            "observation_source": "open_meteo_archive",
            "observation_timestamp": None,
            "raw_response": None,
        }


def _parse_observation_time(value: str | None) -> datetime | None:
    if not value:
        return None
    candidate = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


async def _fetch_metar_daily_extremes(location_name: str, target_date: str) -> dict[str, object]:
    icao = ICAO_CODES.get(location_name.lower())
    if not icao:
        return {
            "actual_high_c": None,
            "actual_low_c": None,
            "observation_source": None,
            "observation_timestamp": None,
        }

    local_tz_name = CITY_TIMEZONES.get(location_name.lower(), "UTC")
    local_tz = ZoneInfo(local_tz_name)
    target = date.fromisoformat(target_date)
    now_local = datetime.now(local_tz)
    hours_back = max(int((now_local.date() - target).days * 24) + 36, 36)
    if hours_back > 96:
        return {
            "actual_high_c": None,
            "actual_low_c": None,
            "observation_source": f"metar_{icao}",
            "observation_timestamp": None,
        }

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.get(
                "https://aviationweather.gov/api/data/metar",
                params={"ids": icao, "format": "json", "hours": hours_back},
            )
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        logger.warning("METAR archive fetch failed for %s (%s): %s", location_name, icao, exc)
        return {
            "actual_high_c": None,
            "actual_low_c": None,
            "observation_source": f"metar_{icao}",
            "observation_timestamp": None,
        }

    observations = payload if isinstance(payload, list) else [payload]
    temps: list[float] = []
    latest_time: datetime | None = None
    for obs in observations:
        obs_time = _parse_observation_time(obs.get("reportTime") or obs.get("obsTime"))
        temp = obs.get("temp")
        if obs_time is None or temp is None:
            continue
        if obs_time.astimezone(local_tz).date().isoformat() != target_date:
            continue
        temps.append(float(temp))
        if latest_time is None or obs_time > latest_time:
            latest_time = obs_time

    if not temps:
        return {
            "actual_high_c": None,
            "actual_low_c": None,
            "observation_source": f"metar_{icao}",
            "observation_timestamp": None,
        }

    return {
        "actual_high_c": max(temps),
        "actual_low_c": min(temps),
        "observation_source": f"metar_{icao}",
        "observation_timestamp": latest_time.isoformat() if latest_time is not None else None,
    }


async def get_daily_observed_temperature_details(
    db: ArenaDB | None,
    latitude: float,
    longitude: float,
    location_name: str,
    target_date: str | date,
) -> dict[str, object]:
    target_date_str = target_date.isoformat() if isinstance(target_date, date) else str(target_date)

    if db is not None:
        cached_high, cached_low = _query_cached_daily_temperatures(db, location_name, target_date_str)
        if cached_high is not None:
            return {
                "actual_high_c": cached_high,
                "actual_low_c": cached_low,
                "observation_source": "cached_station_observations",
                "observation_timestamp": f"{target_date_str}T23:59:59",
                "observation_secondary_source": None,
                "observation_secondary_high_c": None,
                "observation_disagreement_c": None,
            }

    primary = await _fetch_open_meteo_daily_archive(latitude, longitude, target_date_str)
    secondary = await _fetch_metar_daily_extremes(location_name, target_date_str)

    primary_high = primary.get("actual_high_c")
    secondary_high = secondary.get("actual_high_c")
    disagreement = None
    if primary_high is not None and secondary_high is not None:
        disagreement = abs(float(primary_high) - float(secondary_high))
        if disagreement > 2.0:
            logger.warning(
                "Resolution source disagreement for %s on %s: primary=%.1fC, secondary=%.1fC",
                location_name,
                target_date_str,
                float(primary_high),
                float(secondary_high),
            )

    return {
        "actual_high_c": primary_high,
        "actual_low_c": primary.get("actual_low_c"),
        "observation_source": primary.get("observation_source"),
        "observation_timestamp": primary.get("observation_timestamp"),
        "observation_secondary_source": secondary.get("observation_source"),
        "observation_secondary_high_c": secondary_high,
        "observation_disagreement_c": round(float(disagreement), 3) if disagreement is not None else None,
    }


async def get_daily_observed_temperatures(
    db: ArenaDB | None,
    latitude: float,
    longitude: float,
    location_name: str,
    target_date: str | date,
) -> tuple[float | None, float | None]:
    details = await get_daily_observed_temperature_details(
        db=db,
        latitude=latitude,
        longitude=longitude,
        location_name=location_name,
        target_date=target_date,
    )
    return details.get("actual_high_c"), details.get("actual_low_c")
