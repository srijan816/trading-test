from __future__ import annotations

from datetime import date, datetime, timezone

import httpx

from arena.adapters.base import WeatherDataSource
from arena.models import WeatherForecast, WeatherObservation


class HKOWeatherSource(WeatherDataSource):
    def __init__(self, timeout: float = 20.0) -> None:
        self.timeout = timeout

    async def get_forecast(self, city: str, on_date: date) -> WeatherForecast:
        if city.lower() != "hong kong":
            raise ValueError("HKO forecasts are only available for Hong Kong")
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get("https://data.weather.gov.hk/weatherAPI/opendata/weather.php", params={"dataType": "fnd", "lang": "en"})
            response.raise_for_status()
            payload = response.json()
        summary = ""
        high_c = None
        low_c = None
        for item in payload.get("weatherForecast", []):
            if item.get("forecastDate") == on_date.strftime("%Y%m%d"):
                summary = item.get("forecastWeather", "")
                high_c = float(item.get("forecastMaxtemp", {}).get("value", 0.0) or 0.0)
                low_c = float(item.get("forecastMintemp", {}).get("value", 0.0) or 0.0)
                break
        return WeatherForecast("hko", city, on_date, high_c, low_c, None, summary)

    async def get_current_observation(self, city: str) -> WeatherObservation:
        if city.lower() != "hong kong":
            raise ValueError("HKO observations are only available for Hong Kong")
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get("https://data.weather.gov.hk/weatherAPI/opendata/weather.php", params={"dataType": "rhrread", "lang": "en"})
            response.raise_for_status()
            payload = response.json()
        temp = None
        if payload.get("temperature", {}).get("data"):
            temp = float(payload["temperature"]["data"][0]["value"])
        return WeatherObservation("hko", city, datetime.now(timezone.utc), temp, None, "Hong Kong current conditions")
