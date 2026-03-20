from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
import json
import uuid


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def serialize_dataclass(value: Any) -> Any:
    if is_dataclass(value):
        return {key: serialize_dataclass(item) for key, item in asdict(value).items()}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, list):
        return [serialize_dataclass(item) for item in value]
    if isinstance(value, tuple):
        return [serialize_dataclass(item) for item in value]
    if isinstance(value, dict):
        return {key: serialize_dataclass(item) for key, item in value.items()}
    return value


def to_json(value: Any) -> str:
    return json.dumps(serialize_dataclass(value), sort_keys=True)


@dataclass(slots=True)
class Outcome:
    outcome_id: str
    label: str
    best_bid: float
    best_ask: float
    mid_price: float
    bid_depth: list[tuple[float, float]] = field(default_factory=list)
    ask_depth: list[tuple[float, float]] = field(default_factory=list)
    last_trade_price: float = 0.0
    volume_usd: float = 0.0


@dataclass(slots=True)
class Market:
    market_id: str
    venue: str
    slug: str
    question: str
    category: str
    market_type: str
    outcomes: list[Outcome]
    resolution_source: str
    end_time: datetime
    volume_usd: float
    liquidity_usd: float
    status: str
    resolved_outcome_id: str | None = None
    fetched_at: datetime = field(default_factory=utc_now)
    event_group: str | None = None


@dataclass(slots=True)
class OrderBookSnapshot:
    market_id: str
    outcome_id: str
    venue: str
    timestamp: datetime
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    mid: float
    spread: float
    snapshot_id: str = field(default_factory=lambda: new_id("book"))


@dataclass(slots=True)
class Position:
    position_id: str
    strategy_id: str
    market_id: str
    venue: str
    outcome_id: str
    outcome_label: str
    side: str
    quantity: float
    avg_entry_price: float
    current_price: float
    unrealized_pnl: float
    entry_time: datetime
    entry_decision_id: str
    status: str = "open"
    last_updated_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class Portfolio:
    strategy_id: str
    cash: float
    positions: list[Position]
    total_value: float
    realized_pnl: float
    unrealized_pnl: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    max_drawdown: float
    peak_value: float
    updated_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class EvidenceItem:
    source: str
    content: str
    retrieved_at: datetime


@dataclass(slots=True)
class SearchRecord:
    query: str
    results_summary: str
    source_urls: list[str]
    retrieved_at: datetime


@dataclass(slots=True)
class ProposedAction:
    action_type: str
    market_id: str
    venue: str
    outcome_id: str
    outcome_label: str
    amount_usd: float
    limit_price: float | None
    reasoning_summary: str


@dataclass(slots=True)
class Decision:
    decision_id: str
    strategy_id: str
    strategy_type: str
    timestamp: datetime
    markets_considered: list[str]
    predicted_probability: float | None
    market_implied_probability: float | None
    expected_edge_bps: int | None
    confidence: float | None
    evidence_items: list[EvidenceItem]
    risk_notes: str
    exit_plan: str
    thinking: str
    web_searches_used: list[SearchRecord]
    actions: list[ProposedAction]
    no_action_reason: str | None
    llm_model_used: str | None = None
    llm_input_tokens: int | None = None
    llm_output_tokens: int | None = None
    llm_cost_usd: float | None = None
    search_queries_count: int = 0
    search_cost_usd: float = 0.0


@dataclass(slots=True)
class ExecutionResult:
    execution_id: str
    decision_id: str
    strategy_id: str
    timestamp: datetime
    action_type: str
    market_id: str
    venue: str
    outcome_id: str
    status: str
    requested_amount_usd: float
    filled_quantity: float
    avg_fill_price: float
    slippage_applied: float
    fees_applied: float
    total_cost: float
    rejection_reason: str | None
    orderbook_snapshot_id: str


@dataclass(slots=True)
class ResolutionEvent:
    resolution_id: str
    market_id: str
    venue: str
    resolved_at: datetime
    winning_outcome_id: str
    winning_outcome_label: str
    resolution_source_url: str
    positions_settled: list[str]
    total_pnl_impact: dict[str, float]


@dataclass(slots=True)
class DailySnapshot:
    snapshot_date: date
    strategy_id: str
    portfolio_value: float
    cash: float
    positions_count: int
    realized_pnl_cumulative: float
    unrealized_pnl: float
    trades_today: int
    wins_today: int
    losses_today: int
    api_cost_today: float


@dataclass(slots=True)
class SearchResult:
    title: str
    url: str
    snippet: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ResearchBrief:
    query: str
    provider: str
    report_summary: str
    source_urls: list[str]
    follow_ups: list[str]
    session_id: str | None = None
    retrieved_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class WeatherForecast:
    source: str
    city: str
    forecast_date: date
    high_c: float | None
    low_c: float | None
    precipitation_chance: float | None
    summary: str
    retrieved_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class WeatherObservation:
    source: str
    city: str
    observed_at: datetime
    temperature_c: float | None
    precipitation_mm: float | None
    summary: str


@dataclass(slots=True)
class CalibrationRow:
    strategy_id: str
    bucket: str
    predictions: int
    win_rate: float
    avg_confidence: float


@dataclass(slots=True)
class CostRow:
    strategy_id: str
    as_of: date
    llm_input_tokens: int
    llm_output_tokens: int
    llm_cost_usd: float
    search_queries: int
    search_cost_usd: float
    total_cost_usd: float


@dataclass(slots=True)
class StrategyRunContext:
    strategy_id: str
    triggered_at: datetime
    dry_run: bool = False
