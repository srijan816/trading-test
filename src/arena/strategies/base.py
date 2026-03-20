from __future__ import annotations

from abc import ABC, abstractmethod

from arena.db import ArenaDB
from arena.models import Decision


class BaseStrategy(ABC):
    def __init__(self, db: ArenaDB, strategy_config: dict) -> None:
        self.db = db
        self.strategy_config = strategy_config
        self.strategy_id = strategy_config["id"]
        self.trade_enabled = bool(strategy_config.get("trade_enabled", True))
        scope = strategy_config.get("scope", {})
        self.supported_formats: list[str] = (
            scope.get("supported_formats")
            or strategy_config.get("supported_formats")
            or ["binary", "numeric_bracket"]
        )
        self.supported_categories: list[str] = scope.get("categories", [])

    def is_market_eligible(self, row) -> bool:
        """Check if a market's format and category are supported by this strategy."""
        fmt = row["market_format"] if "market_format" in row.keys() else None
        if fmt and fmt not in self.supported_formats:
            return False
        if self.supported_categories:
            cat = row["category"] if "category" in row.keys() else None
            if cat and cat not in self.supported_categories:
                return False
        return True

    def should_execute_trade(self) -> bool:
        return self.trade_enabled

    @abstractmethod
    async def generate_decision(self) -> Decision:
        raise NotImplementedError


class Strategy(BaseStrategy):
    pass
