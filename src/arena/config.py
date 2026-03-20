from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import tomllib


ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "config"


@dataclass(slots=True)
class StrategyConfig:
    raw: dict[str, Any]

    @property
    def strategy(self) -> dict[str, Any]:
        return self.raw["strategy"]

    @property
    def strategy_id(self) -> str:
        return self.strategy["id"]

    @property
    def strategy_type(self) -> str:
        return self.strategy["type"]

    @property
    def cadence_minutes(self) -> int:
        return int(self.strategy.get("schedule", {}).get("cadence_minutes", 60))


@dataclass(slots=True)
class AppConfig:
    arena: dict[str, Any]
    scheduler: dict[str, Any]
    venues: dict[str, Any]
    fees: dict[str, Any]
    reports: dict[str, Any]
    manual: dict[str, Any]
    models: dict[str, Any]
    strategies: dict[str, StrategyConfig]
    execution: dict[str, Any]
    risk: dict[str, Any]
    position_sizing: dict[str, Any]

    @property
    def db_path(self) -> Path:
        return ROOT / self.arena["db_path"]

    @property
    def stop_file(self) -> Path:
        return ROOT / self.arena["stop_file"]


def _read_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def load_strategy_configs(config_dir: Path | None = None) -> dict[str, StrategyConfig]:
    directory = (config_dir or CONFIG_DIR) / "strategies"
    configs: dict[str, StrategyConfig] = {}
    for path in sorted(directory.glob("*.toml")):
        raw = _read_toml(path)
        config = StrategyConfig(raw=raw)
        configs[config.strategy_id] = config
    return configs


def load_app_config(root: Path | None = None) -> AppConfig:
    base = root or ROOT
    arena_raw = _read_toml(base / "config" / "arena.toml")
    models_raw = _read_toml(base / "config" / "models.toml")
    strategies = load_strategy_configs(base / "config")
    return AppConfig(
        arena=arena_raw["arena"],
        scheduler=arena_raw["scheduler"],
        venues=arena_raw["venues"],
        fees=arena_raw["fees"],
        reports=arena_raw["reports"],
        manual=arena_raw["manual"],
        models=models_raw,
        strategies=strategies,
        execution=arena_raw.get("execution", {}),
        risk=arena_raw.get("risk", {}),
        position_sizing=arena_raw.get("position_sizing", {}),
    )
