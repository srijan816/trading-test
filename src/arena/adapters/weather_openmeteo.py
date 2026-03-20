from __future__ import annotations

from datetime import date, datetime, timezone
from typing import ClassVar

import httpx

from arena.adapters.base import WeatherDataSource
from arena.models import WeatherForecast, WeatherObservation


CITY_COORDS = {
    "hong kong": (22.3193, 114.1694),
    "new york": (40.7128, -74.0060),
    "chicago": (41.8781, -87.6298),
    "london": (51.5072, -0.1276),
    "atlanta": (33.7490, -84.3880),
    "ankara": (39.9334, 32.8597),
    "buenos aires": (-34.6037, -58.3816),
    "lucknow": (26.8467, 80.9462),
    "madrid": (40.4168, -3.7038),
    "seattle": (47.6062, -122.3321),
    "toronto": (43.6532, -79.3832),
    "taipei": (25.0330, 121.5654),
}


class OpenMeteoSource(WeatherDataSource):
    _geo_cache: ClassVar[dict[str, tuple[float, float]]] = dict(CITY_COORDS)

    def __init__(self, timeout: float = 20.0) -> None:
        self.timeout = timeout

    async def get_forecast(self, city: str, on_date: date) -> WeatherForecast:
        lat, lon = await self._resolve_coords(city)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "daily": "temperature_2m_max,temperature_2m_min,precipitation_probability_max",
                    "timezone": "UTC",
                    "start_date": on_date.isoformat(),
                    "end_date": on_date.isoformat(),
                },
            )
            response.raise_for_status()
            payload = response.json()
        daily = payload.get("daily", {})
        high = daily.get("temperature_2m_max", [None])[0]
        low = daily.get("temperature_2m_min", [None])[0]
        precip = daily.get("precipitation_probability_max", [None])[0]
        return WeatherForecast("open-meteo", city, on_date, high, low, precip, "Open-Meteo forecast")

    async def get_current_observation(self, city: str) -> WeatherObservation:
        lat, lon = await self._resolve_coords(city)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                "https://api.open-meteo.com/v1/forecast",
                params={"latitude": lat, "longitude": lon, "current": "temperature_2m,precipitation", "timezone": "UTC"},
            )
            response.raise_for_status()
            payload = response.json()
        current = payload.get("current", {})
        return WeatherObservation(
            "open-meteo",
            city,
            datetime.now(timezone.utc),
            current.get("temperature_2m"),
            current.get("precipitation"),
            "Open-Meteo current conditions",
        )

    async def _resolve_coords(self, city: str) -> tuple[float, float]:
        key = city.lower().strip()
        cached = self._geo_cache.get(key)
        if cached:
            return cached
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                "https://geocoding-api.open-meteo.com/v1/search",
                params={"name": city, "count": 1, "language": "en", "format": "json"},
            )
            response.raise_for_status()
            payload = response.json()
        results = payload.get("results") or []
        if not results:
            return CITY_COORDS["hong kong"]
        coords = (float(results[0]["latitude"]), float(results[0]["longitude"]))
        self._geo_cache[key] = coords
        return coords
