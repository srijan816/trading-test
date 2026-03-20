from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from arena.models import (
    CalibrationRow,
    CostRow,
    Market,
    OrderBookSnapshot,
    SearchResult,
    WeatherForecast,
    WeatherObservation,
)


class MarketDataAdapter(ABC):
    venue: str

    @abstractmethod
    async def list_active_markets(self, categories: list[str] | None = None) -> list[Market]:
        raise NotImplementedError

    @abstractmethod
    async def get_orderbook(self, market_id: str, outcome_id: str) -> OrderBookSnapshot:
        raise NotImplementedError

    @abstractmethod
    async def get_resolution_status(self, market_id: str) -> Market:
        raise NotImplementedError

    @abstractmethod
    async def search_markets(self, query: str) -> list[Market]:
        raise NotImplementedError


class LLMClient(ABC):
    @abstractmethod
    async def complete_json(
        self,
        system_prompt: str,
        user_content: str,
        json_schema: dict,
        max_tokens: int = 4096,
        temperature: float = 0.3,
        model_id: str | None = None,
    ) -> tuple[dict, int, int, float]:
        raise NotImplementedError

    @abstractmethod
    async def complete_text(
        self,
        system_prompt: str,
        user_content: str,
        max_tokens: int = 4096,
        model_id: str | None = None,
    ) -> tuple[str, int, int, float]:
        raise NotImplementedError


class SearchClient(ABC):
    @abstractmethod
    async def search(self, query: str, num_results: int = 5) -> list[SearchResult]:
        raise NotImplementedError


class WeatherDataSource(ABC):
    @abstractmethod
    async def get_forecast(self, city: str, on_date: date) -> WeatherForecast:
        raise NotImplementedError

    @abstractmethod
    async def get_current_observation(self, city: str) -> WeatherObservation:
        raise NotImplementedError


class DashboardSink(ABC):
    @abstractmethod
    async def export_leaderboard(self, snapshots: list[dict]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def export_trade_log(self, executions: list[dict]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def export_reasoning_log(self, decisions: list[dict]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def export_calibration(self, data: list[CalibrationRow]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def export_costs(self, data: list[CostRow]) -> None:
        raise NotImplementedError
