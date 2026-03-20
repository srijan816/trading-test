from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator
import json
import sqlite3

from .event_groups import derive_event_group
from .engine.order_schema import LIMIT_ORDERS_DDL, ORDER_EVENTS_DDL
from .intelligence.discovery_logger import DISCOVERY_ALERTS_DDL
from .models import (
    DailySnapshot,
    Decision,
    ExecutionResult,
    Market,
    OrderBookSnapshot,
    Portfolio,
    Position,
    ResolutionEvent,
    to_json,
)


SCHEMA = """
CREATE TABLE IF NOT EXISTS markets (
    market_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    slug TEXT NOT NULL,
    question TEXT NOT NULL,
    category TEXT NOT NULL,
    event_group TEXT,
    market_type TEXT NOT NULL,
    outcomes_json TEXT NOT NULL,
    resolution_source TEXT NOT NULL,
    end_time TEXT NOT NULL,
    volume_usd REAL NOT NULL,
    liquidity_usd REAL NOT NULL,
    status TEXT NOT NULL,
    resolved_outcome_id TEXT,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (market_id, venue)
);

CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    outcome_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    bids_json TEXT NOT NULL,
    asks_json TEXT NOT NULL,
    mid REAL NOT NULL,
    spread REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolios (
    strategy_id TEXT PRIMARY KEY,
    cash REAL NOT NULL,
    positions_json TEXT NOT NULL,
    total_value REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    total_trades INTEGER NOT NULL,
    winning_trades INTEGER NOT NULL,
    losing_trades INTEGER NOT NULL,
    max_drawdown REAL NOT NULL,
    peak_value REAL NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
    position_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    outcome_id TEXT NOT NULL,
    outcome_label TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity REAL NOT NULL,
    avg_entry_price REAL NOT NULL,
    current_price REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    realized_pnl REAL NOT NULL DEFAULT 0,
    entry_time TEXT NOT NULL,
    entry_decision_id TEXT NOT NULL,
    status TEXT NOT NULL,
    last_updated_at TEXT NOT NULL,
    closed_at TEXT
);

CREATE TABLE IF NOT EXISTS decisions (
    decision_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    strategy_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    markets_considered_json TEXT NOT NULL,
    predicted_probability REAL,
    market_implied_probability REAL,
    expected_edge_bps INTEGER,
    confidence REAL,
    evidence_items_json TEXT NOT NULL,
    risk_notes TEXT NOT NULL,
    exit_plan TEXT NOT NULL,
    thinking TEXT NOT NULL,
    web_searches_used_json TEXT NOT NULL,
    actions_json TEXT NOT NULL,
    no_action_reason TEXT,
    llm_model_used TEXT,
    llm_input_tokens INTEGER,
    llm_output_tokens INTEGER,
    llm_cost_usd REAL,
    search_queries_count INTEGER NOT NULL,
    search_cost_usd REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS executions (
    execution_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    action_type TEXT NOT NULL,
    market_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    outcome_id TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_amount_usd REAL NOT NULL,
    filled_quantity REAL NOT NULL,
    avg_fill_price REAL NOT NULL,
    slippage_applied REAL NOT NULL,
    fees_applied REAL NOT NULL,
    total_cost REAL NOT NULL,
    rejection_reason TEXT,
    orderbook_snapshot_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS resolutions (
    resolution_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    resolved_at TEXT NOT NULL,
    winning_outcome_id TEXT NOT NULL,
    winning_outcome_label TEXT NOT NULL,
    resolution_source_url TEXT NOT NULL,
    positions_settled_json TEXT NOT NULL,
    total_pnl_impact_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_snapshots (
    snapshot_date TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    portfolio_value REAL NOT NULL,
    cash REAL NOT NULL,
    positions_count INTEGER NOT NULL,
    realized_pnl_cumulative REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    trades_today INTEGER NOT NULL,
    wins_today INTEGER NOT NULL,
    losses_today INTEGER NOT NULL,
    api_cost_today REAL NOT NULL,
    PRIMARY KEY (snapshot_date, strategy_id)
);

CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    strategy_id TEXT,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS config_recommendations (
    recommendation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    applied INTEGER NOT NULL DEFAULT 0,
    applied_at TEXT
);

CREATE TABLE IF NOT EXISTS research_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    strategy TEXT,
    market_id TEXT,
    market_question TEXT,
    query_sent TEXT,
    endpoint TEXT,
    mode TEXT,
    model_used TEXT,
    duration_ms INTEGER,
    report_length INTEGER,
    sources_count INTEGER,
    sources_json TEXT,
    report_summary TEXT,
    full_report TEXT,
    reasoning_trace TEXT,
    probability REAL,
    confidence TEXT,
    edge_assessment TEXT,
    from_cache INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    used_in_decision INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_markets_status ON markets(status);
CREATE INDEX IF NOT EXISTS idx_markets_category ON markets(category);
CREATE INDEX IF NOT EXISTS idx_positions_strategy_status ON positions(strategy_id, status);
CREATE INDEX IF NOT EXISTS idx_decisions_strategy_timestamp ON decisions(strategy_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_executions_strategy_timestamp ON executions(strategy_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_resolutions_market ON resolutions(market_id, venue);
CREATE INDEX IF NOT EXISTS idx_research_log_timestamp ON research_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_research_log_strategy_timestamp ON research_log(strategy, timestamp);
CREATE INDEX IF NOT EXISTS idx_research_log_market_timestamp ON research_log(market_id, timestamp);
"""


class ArenaDB:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            conn.executescript(LIMIT_ORDERS_DDL)
            conn.executescript(ORDER_EVENTS_DDL)
            conn.executescript(DISCOVERY_ALERTS_DDL)
            self._migrate(conn)

    def _migrate(self, conn: sqlite3.Connection) -> None:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(markets)")}
        position_columns = {row["name"] for row in conn.execute("PRAGMA table_info(positions)")}
        research_log_columns = {row["name"] for row in conn.execute("PRAGMA table_info(research_log)")}
        parameter_adjustment_columns = {row["name"] for row in conn.execute("PRAGMA table_info(parameter_adjustments)")}
        if "event_group" not in columns:
            conn.execute("ALTER TABLE markets ADD COLUMN event_group TEXT")
        if "secondary_category" not in columns:
            conn.execute("ALTER TABLE markets ADD COLUMN secondary_category TEXT")
        if "market_format" not in columns:
            conn.execute("ALTER TABLE markets ADD COLUMN market_format TEXT")
        if "realized_pnl" not in position_columns:
            conn.execute("ALTER TABLE positions ADD COLUMN realized_pnl REAL NOT NULL DEFAULT 0")
        if "closed_at" not in position_columns:
            conn.execute("ALTER TABLE positions ADD COLUMN closed_at TEXT")
        if "reasoning_trace" not in research_log_columns:
            conn.execute("ALTER TABLE research_log ADD COLUMN reasoning_trace TEXT")
        if "city" not in parameter_adjustment_columns and parameter_adjustment_columns:
            conn.execute("ALTER TABLE parameter_adjustments ADD COLUMN city TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_markets_event_group ON markets(event_group)")

        # Week 1: forecast history for bias tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS forecast_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location TEXT NOT NULL,
                source TEXT NOT NULL,
                forecast_date TEXT NOT NULL,
                target_date TEXT NOT NULL,
                predicted_high_c REAL,
                predicted_low_c REAL,
                actual_high_c REAL,
                actual_low_c REAL,
                error_high_c REAL,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_fh_location_source
            ON forecast_history(location, source, target_date)
        """)

        # Week 2: calibration feedback tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS decision_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                predicted_probability REAL NOT NULL,
                actual_outcome REAL NOT NULL,
                brier_score REAL NOT NULL,
                forecast_error_c REAL,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ds_strategy
            ON decision_scores(strategy_id, created_at)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS strategy_health (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT NOT NULL,
                sample_size INTEGER NOT NULL,
                rolling_brier REAL NOT NULL,
                calibration_error REAL NOT NULL,
                mean_forecast_error_c REAL,
                overconfidence_rate REAL,
                computed_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS parameter_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT NOT NULL,
                city TEXT,
                parameter_name TEXT NOT NULL,
                current_value REAL NOT NULL,
                recommended_value REAL NOT NULL,
                reason TEXT NOT NULL,
                auto_applied BOOLEAN DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_parameter_adjustments_city_param
            ON parameter_adjustments(city, parameter_name, created_at)
        """)

        # Intraday: station observation history
        conn.execute("""
            CREATE TABLE IF NOT EXISTS station_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location TEXT NOT NULL,
                source TEXT NOT NULL,
                observation_time TEXT NOT NULL,
                temperature_c REAL NOT NULL,
                trending TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_so_location_time
            ON station_observations(location, observation_time)
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_research_log_timestamp ON research_log(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_research_log_strategy_timestamp ON research_log(strategy, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_research_log_market_timestamp ON research_log(market_id, timestamp)")
        conn.executescript(DISCOVERY_ALERTS_DDL)
        rows = list(
            conn.execute(
                "SELECT market_id, venue, slug, question, category FROM markets WHERE event_group IS NULL OR event_group = ''"
            )
        )
        for row in rows:
            event_group = derive_event_group(row["question"], row["category"], row["venue"], row["slug"])
            if not event_group:
                continue
            conn.execute(
                "UPDATE markets SET event_group = ? WHERE market_id = ? AND venue = ?",
                (event_group, row["market_id"], row["venue"]),
            )

    def record_event(self, event_type: str, payload: dict[str, Any], strategy_id: str | None = None) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO events(event_type, strategy_id, payload_json) VALUES (?, ?, ?)",
                (event_type, strategy_id, json.dumps(payload, sort_keys=True)),
            )

    def log_research_entry(
        self,
        *,
        strategy: str | None = None,
        market_id: str | None = None,
        market_question: str | None = None,
        query_sent: str | None = None,
        endpoint: str | None = None,
        mode: str | None = None,
        model_used: str | None = None,
        duration_ms: int | None = None,
        report_length: int | None = None,
        sources_count: int | None = None,
        sources_json: list[dict[str, Any]] | str | None = None,
        report_summary: str | None = None,
        full_report: str | None = None,
        reasoning_trace: str | None = None,
        probability: float | None = None,
        confidence: str | None = None,
        edge_assessment: dict[str, Any] | str | None = None,
        from_cache: bool = False,
        error: str | None = None,
        used_in_decision: bool = False,
    ) -> int:
        timestamp = datetime.utcnow().replace(microsecond=0).isoformat()
        sources_value: str | None
        if sources_json is None:
            sources_value = None
        elif isinstance(sources_json, str):
            sources_value = sources_json
        else:
            sources_value = json.dumps(sources_json, ensure_ascii=False)
        edge_value: str | None
        if edge_assessment is None:
            edge_value = None
        elif isinstance(edge_assessment, str):
            edge_value = edge_assessment
        else:
            edge_value = json.dumps(edge_assessment, ensure_ascii=False)
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO research_log (
                    timestamp, strategy, market_id, market_question, query_sent, endpoint, mode,
                    model_used, duration_ms, report_length, sources_count, sources_json,
                    report_summary, full_report, reasoning_trace, probability, confidence,
                    edge_assessment, from_cache, error, used_in_decision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp,
                    strategy,
                    market_id,
                    market_question,
                    query_sent,
                    endpoint,
                    mode,
                    model_used,
                    duration_ms,
                    report_length,
                    sources_count,
                    sources_value,
                    report_summary,
                    full_report,
                    reasoning_trace,
                    probability,
                    confidence,
                    edge_value,
                    int(from_cache),
                    error,
                    int(used_in_decision),
                ),
            )
            return int(cursor.lastrowid)

    def mark_research_used_in_decision(
        self,
        *,
        strategy: str,
        market_ids: list[str],
        decision_time: datetime,
        lookback_minutes: int = 90,
    ) -> int:
        unique_market_ids = [market_id for market_id in dict.fromkeys(str(item) for item in market_ids if item)]
        if not unique_market_ids:
            return 0
        normalized_time = decision_time.astimezone(UTC).replace(tzinfo=None) if decision_time.tzinfo else decision_time
        lower_bound = (normalized_time - timedelta(minutes=lookback_minutes)).replace(microsecond=0)
        upper_bound = (normalized_time + timedelta(minutes=5)).replace(microsecond=0)
        placeholders = ",".join("?" for _ in unique_market_ids)
        params: list[Any] = [
            strategy,
            lower_bound.isoformat(),
            upper_bound.isoformat(),
            *unique_market_ids,
        ]
        with self.connect() as conn:
            cursor = conn.execute(
                f"""
                UPDATE research_log
                SET used_in_decision = 1
                WHERE strategy = ?
                  AND timestamp >= ?
                  AND timestamp <= ?
                  AND market_id IN ({placeholders})
                """,
                params,
            )
            return int(cursor.rowcount or 0)

    def upsert_market(self, market: Market) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO markets (
                    market_id, venue, slug, question, category, event_group, market_type, outcomes_json,
                    resolution_source, end_time, volume_usd, liquidity_usd, status,
                    resolved_outcome_id, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_id, venue) DO UPDATE SET
                    slug=excluded.slug,
                    question=excluded.question,
                    category=excluded.category,
                    event_group=excluded.event_group,
                    market_type=excluded.market_type,
                    outcomes_json=excluded.outcomes_json,
                    resolution_source=excluded.resolution_source,
                    end_time=excluded.end_time,
                    volume_usd=excluded.volume_usd,
                    liquidity_usd=excluded.liquidity_usd,
                    status=CASE
                        WHEN markets.status = 'resolved' AND excluded.status = 'active' THEN markets.status
                        ELSE excluded.status
                    END,
                    resolved_outcome_id=COALESCE(excluded.resolved_outcome_id, markets.resolved_outcome_id),
                    fetched_at=excluded.fetched_at
                """,
                (
                    market.market_id,
                    market.venue,
                    market.slug,
                    market.question,
                    market.category,
                    market.event_group or derive_event_group(market.question, market.category, market.venue, market.slug),
                    market.market_type,
                    to_json(market.outcomes),
                    market.resolution_source,
                    market.end_time.isoformat(),
                    market.volume_usd,
                    market.liquidity_usd,
                    market.status,
                    market.resolved_outcome_id,
                    market.fetched_at.isoformat(),
                ),
            )

    def list_markets(self, category: str | None = None, status: str | None = None) -> list[sqlite3.Row]:
        query = "SELECT * FROM markets WHERE 1=1"
        params: list[Any] = []
        if category:
            query += " AND category = ?"
            params.append(category)
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY end_time ASC"
        with self.connect() as conn:
            return list(conn.execute(query, params))

    def get_market(self, market_id: str, venue: str) -> sqlite3.Row | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM markets WHERE market_id = ? AND venue = ?",
                (market_id, venue),
            ).fetchone()
        return row

    def save_orderbook_snapshot(self, snapshot: OrderBookSnapshot) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO orderbook_snapshots (
                    snapshot_id, market_id, outcome_id, venue, timestamp,
                    bids_json, asks_json, mid, spread
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.snapshot_id,
                    snapshot.market_id,
                    snapshot.outcome_id,
                    snapshot.venue,
                    snapshot.timestamp.isoformat(),
                    json.dumps(snapshot.bids),
                    json.dumps(snapshot.asks),
                    snapshot.mid,
                    snapshot.spread,
                ),
            )

    def ensure_portfolio(self, strategy_id: str, starting_balance: float) -> Portfolio:
        existing = self.get_portfolio(strategy_id)
        if existing:
            return existing
        portfolio = Portfolio(
            strategy_id=strategy_id,
            cash=starting_balance,
            positions=[],
            total_value=starting_balance,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            max_drawdown=0.0,
            peak_value=starting_balance,
        )
        self.save_portfolio(portfolio)
        return portfolio

    def get_portfolio(self, strategy_id: str) -> Portfolio | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM portfolios WHERE strategy_id = ?",
                (strategy_id,),
            ).fetchone()
        if not row:
            return None
        positions = self.list_open_positions(strategy_id)
        return Portfolio(
            strategy_id=row["strategy_id"],
            cash=row["cash"],
            positions=positions,
            total_value=row["total_value"],
            realized_pnl=row["realized_pnl"],
            unrealized_pnl=row["unrealized_pnl"],
            total_trades=row["total_trades"],
            winning_trades=row["winning_trades"],
            losing_trades=row["losing_trades"],
            max_drawdown=row["max_drawdown"],
            peak_value=row["peak_value"],
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def save_portfolio(self, portfolio: Portfolio) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO portfolios (
                    strategy_id, cash, positions_json, total_value, realized_pnl,
                    unrealized_pnl, total_trades, winning_trades, losing_trades,
                    max_drawdown, peak_value, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(strategy_id) DO UPDATE SET
                    cash=excluded.cash,
                    positions_json=excluded.positions_json,
                    total_value=excluded.total_value,
                    realized_pnl=excluded.realized_pnl,
                    unrealized_pnl=excluded.unrealized_pnl,
                    total_trades=excluded.total_trades,
                    winning_trades=excluded.winning_trades,
                    losing_trades=excluded.losing_trades,
                    max_drawdown=excluded.max_drawdown,
                    peak_value=excluded.peak_value,
                    updated_at=excluded.updated_at
                """,
                (
                    portfolio.strategy_id,
                    portfolio.cash,
                    to_json(portfolio.positions),
                    portfolio.total_value,
                    portfolio.realized_pnl,
                    portfolio.unrealized_pnl,
                    portfolio.total_trades,
                    portfolio.winning_trades,
                    portfolio.losing_trades,
                    portfolio.max_drawdown,
                    portfolio.peak_value,
                    portfolio.updated_at.isoformat(),
                ),
            )

    def upsert_position(self, position: Position) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO positions (
                    position_id, strategy_id, market_id, venue, outcome_id, outcome_label, side,
                    quantity, avg_entry_price, current_price, unrealized_pnl, entry_time,
                    entry_decision_id, status, last_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(position_id) DO UPDATE SET
                    quantity=excluded.quantity,
                    current_price=excluded.current_price,
                    unrealized_pnl=excluded.unrealized_pnl,
                    status=excluded.status,
                    last_updated_at=excluded.last_updated_at
                """,
                (
                    position.position_id,
                    position.strategy_id,
                    position.market_id,
                    position.venue,
                    position.outcome_id,
                    position.outcome_label,
                    position.side,
                    position.quantity,
                    position.avg_entry_price,
                    position.current_price,
                    position.unrealized_pnl,
                    position.entry_time.isoformat(),
                    position.entry_decision_id,
                    position.status,
                    position.last_updated_at.isoformat(),
                ),
            )

    def list_open_positions(self, strategy_id: str | None = None) -> list[Position]:
        query = "SELECT * FROM positions WHERE status = 'open'"
        params: list[Any] = []
        if strategy_id:
            query += " AND strategy_id = ?"
            params.append(strategy_id)
        with self.connect() as conn:
            rows = list(conn.execute(query, params))
        return [
            Position(
                position_id=row["position_id"],
                strategy_id=row["strategy_id"],
                market_id=row["market_id"],
                venue=row["venue"],
                outcome_id=row["outcome_id"],
                outcome_label=row["outcome_label"],
                side=row["side"],
                quantity=row["quantity"],
                avg_entry_price=row["avg_entry_price"],
                current_price=row["current_price"],
                unrealized_pnl=row["unrealized_pnl"],
                entry_time=datetime.fromisoformat(row["entry_time"]),
                entry_decision_id=row["entry_decision_id"],
                status=row["status"],
                last_updated_at=datetime.fromisoformat(row["last_updated_at"]),
            )
            for row in rows
        ]

    def save_decision(self, decision: Decision) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision.decision_id,
                    decision.strategy_id,
                    decision.strategy_type,
                    decision.timestamp.isoformat(),
                    json.dumps(decision.markets_considered),
                    decision.predicted_probability,
                    decision.market_implied_probability,
                    decision.expected_edge_bps,
                    decision.confidence,
                    to_json(decision.evidence_items),
                    decision.risk_notes,
                    decision.exit_plan,
                    decision.thinking,
                    to_json(decision.web_searches_used),
                    to_json(decision.actions),
                    decision.no_action_reason,
                    decision.llm_model_used,
                    decision.llm_input_tokens,
                    decision.llm_output_tokens,
                    decision.llm_cost_usd,
                    decision.search_queries_count,
                    decision.search_cost_usd,
                ),
            )

    def list_recent_decisions(self, strategy_id: str | None = None, limit: int = 5) -> list[sqlite3.Row]:
        query = "SELECT * FROM decisions"
        params: list[Any] = []
        if strategy_id:
            query += " WHERE strategy_id = ?"
            params.append(strategy_id)
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        with self.connect() as conn:
            return list(conn.execute(query, params))

    def save_execution(self, execution: ExecutionResult) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO executions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    execution.execution_id,
                    execution.decision_id,
                    execution.strategy_id,
                    execution.timestamp.isoformat(),
                    execution.action_type,
                    execution.market_id,
                    execution.venue,
                    execution.outcome_id,
                    execution.status,
                    execution.requested_amount_usd,
                    execution.filled_quantity,
                    execution.avg_fill_price,
                    execution.slippage_applied,
                    execution.fees_applied,
                    execution.total_cost,
                    execution.rejection_reason,
                    execution.orderbook_snapshot_id,
                ),
            )

    def list_recent_executions(self, limit: int = 20) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    "SELECT * FROM executions ORDER BY timestamp DESC LIMIT ?",
                    (limit,),
                )
            )

    def save_resolution(self, resolution: ResolutionEvent) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO resolutions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    resolution.resolution_id,
                    resolution.market_id,
                    resolution.venue,
                    resolution.resolved_at.isoformat(),
                    resolution.winning_outcome_id,
                    resolution.winning_outcome_label,
                    resolution.resolution_source_url,
                    json.dumps(resolution.positions_settled),
                    json.dumps(resolution.total_pnl_impact),
                ),
            )

    def save_daily_snapshot(self, snapshot: DailySnapshot) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO daily_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    snapshot.snapshot_date.isoformat(),
                    snapshot.strategy_id,
                    snapshot.portfolio_value,
                    snapshot.cash,
                    snapshot.positions_count,
                    snapshot.realized_pnl_cumulative,
                    snapshot.unrealized_pnl,
                    snapshot.trades_today,
                    snapshot.wins_today,
                    snapshot.losses_today,
                    snapshot.api_cost_today,
                ),
            )

    def list_daily_snapshots(self, snapshot_date: date | None = None) -> list[sqlite3.Row]:
        query = "SELECT * FROM daily_snapshots"
        params: list[Any] = []
        if snapshot_date:
            query += " WHERE snapshot_date = ?"
            params.append(snapshot_date.isoformat())
        with self.connect() as conn:
            return list(conn.execute(query, params))

    def recategorize_markets(self, categorize_fn) -> int:
        with self.connect() as conn:
            rows = list(conn.execute("SELECT market_id, venue, question, category FROM markets"))
            updated = 0
            for row in rows:
                new_category = categorize_fn(row["question"])
                if new_category == row["category"]:
                    continue
                conn.execute(
                    "UPDATE markets SET category = ? WHERE market_id = ? AND venue = ?",
                    (new_category, row["market_id"], row["venue"]),
                )
                updated += 1
        return updated

    def counts(self) -> dict[str, int]:
        tables = ["markets", "portfolios", "positions", "decisions", "executions"]
        result: dict[str, int] = {}
        with self.connect() as conn:
            for table in tables:
                result[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return result

    def get_decision(self, decision_id: str) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM decisions WHERE decision_id = ?",
                (decision_id,),
            ).fetchone()

    def list_portfolios(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM portfolios ORDER BY strategy_id"))

    def sync_portfolios_to_targets(self, targets: dict[str, float]) -> None:
        for strategy_id, target_value in targets.items():
            portfolio = self.ensure_portfolio(strategy_id, target_value)
            open_positions = self.list_open_positions(strategy_id)
            open_value = sum(position.quantity * position.current_price for position in open_positions)
            if portfolio.total_value >= target_value and portfolio.cash >= max(target_value - open_value, 0.0):
                continue
            portfolio.positions = open_positions
            portfolio.cash = max(target_value - open_value, 0.0)
            portfolio.total_value = portfolio.cash + open_value
            portfolio.peak_value = max(portfolio.peak_value, portfolio.total_value)
            portfolio.updated_at = datetime.now()
            self.save_portfolio(portfolio)
