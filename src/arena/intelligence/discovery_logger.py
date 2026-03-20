"""Logs discovery signals to SQLite for dashboard display and strategy consumption."""

from __future__ import annotations

import sqlite3

from arena.intelligence.discovery import DiscoverySignal, SignalType

DISCOVERY_ALERTS_DDL = """
CREATE TABLE IF NOT EXISTS discovery_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    market_question TEXT,
    category TEXT,
    signal_type TEXT NOT NULL,
    headline TEXT NOT NULL,
    detail TEXT,
    source_url TEXT,
    source_name TEXT,
    recency_minutes INTEGER,
    relevance_score REAL,
    direction TEXT,
    strategy_id TEXT,
    acted_on INTEGER DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (market_id) REFERENCES markets(market_id)
);

CREATE INDEX IF NOT EXISTS idx_discovery_alerts_market ON discovery_alerts(market_id);
CREATE INDEX IF NOT EXISTS idx_discovery_alerts_type ON discovery_alerts(signal_type);
CREATE INDEX IF NOT EXISTS idx_discovery_alerts_time ON discovery_alerts(created_at);
"""


class DiscoveryLogger:
    """Records discovery signals and provides query methods."""

    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path
        if db_path:
            self._ensure_table()

    def init(self, db_path: str):
        self.db_path = db_path
        self._ensure_table()

    def _connect(self) -> sqlite3.Connection:
        if not self.db_path:
            raise RuntimeError("DiscoveryLogger is not initialized with a database path")
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_table(self):
        """Create discovery_alerts table if it doesn't exist."""

        with self._connect() as conn:
            conn.executescript(DISCOVERY_ALERTS_DDL)

    def log_signal(
        self,
        signal: DiscoverySignal,
        strategy_id: str = None,
        *,
        market_question: str | None = None,
        category: str | None = None,
    ) -> int:
        """Insert a discovery signal. Returns the row id."""

        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO discovery_alerts (
                    market_id, market_question, category, signal_type, headline, detail,
                    source_url, source_name, recency_minutes, relevance_score, direction,
                    strategy_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.market_id,
                    market_question,
                    category,
                    signal.signal_type.value,
                    signal.headline,
                    signal.detail,
                    signal.source_url,
                    signal.source_name,
                    signal.recency_minutes,
                    signal.relevance_score,
                    signal.direction,
                    strategy_id,
                ),
            )
            return int(cursor.lastrowid)

    def log_no_signal(
        self,
        market_id: str,
        market_question: str,
        category: str,
        strategy_id: str = None,
    ):
        """Record that we searched and found nothing new."""

        signal = DiscoverySignal(
            signal_type=SignalType.NO_SIGNAL,
            headline="No new signals detected",
            detail="Discovery search completed without finding fresh market-moving information.",
            source_url="",
            source_name="Nexus discovery",
            recency_minutes=-1,
            relevance_score=0.0,
            market_id=market_id,
            direction="none",
        )
        return self.log_signal(
            signal,
            strategy_id=strategy_id,
            market_question=market_question,
            category=category,
        )

    def mark_acted_on(self, alert_id: int):
        """Mark that a strategy acted on this signal (placed a trade)."""

        with self._connect() as conn:
            conn.execute(
                "UPDATE discovery_alerts SET acted_on = 1 WHERE id = ?",
                (alert_id,),
            )

    def get_recent_signals(
        self,
        hours: int = 24,
        signal_type: str = None,
        market_id: str = None,
    ) -> list[dict]:
        """Query recent signals for dashboard display."""

        filters = ["created_at >= datetime('now', ?)"]
        params: list[object] = [f"-{int(hours)} hours"]
        if signal_type:
            filters.append("signal_type = ?")
            params.append(signal_type)
        if market_id:
            filters.append("market_id = ?")
            params.append(market_id)
        where_clause = " AND ".join(filters)

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM discovery_alerts
                WHERE {where_clause}
                ORDER BY created_at DESC, id DESC
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def get_signal_stats(self, hours: int = 24) -> dict:
        """Return signal statistics for monitoring."""

        with self._connect() as conn:
            cutoff = f"-{int(hours)} hours"
            total_searches = conn.execute(
                """
                SELECT COUNT(*)
                FROM discovery_alerts
                WHERE created_at >= datetime('now', ?)
                """,
                (cutoff,),
            ).fetchone()[0]
            signals_found = conn.execute(
                """
                SELECT COUNT(*)
                FROM discovery_alerts
                WHERE created_at >= datetime('now', ?)
                  AND signal_type != ?
                """,
                (cutoff, SignalType.NO_SIGNAL.value),
            ).fetchone()[0]
            no_signal_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM discovery_alerts
                WHERE created_at >= datetime('now', ?)
                  AND signal_type = ?
                """,
                (cutoff, SignalType.NO_SIGNAL.value),
            ).fetchone()[0]
            acted_on_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM discovery_alerts
                WHERE created_at >= datetime('now', ?)
                  AND signal_type != ?
                  AND acted_on = 1
                """,
                (cutoff, SignalType.NO_SIGNAL.value),
            ).fetchone()[0]
            avg_relevance = conn.execute(
                """
                SELECT AVG(relevance_score)
                FROM discovery_alerts
                WHERE created_at >= datetime('now', ?)
                  AND signal_type != ?
                """,
                (cutoff, SignalType.NO_SIGNAL.value),
            ).fetchone()[0]
            type_rows = conn.execute(
                """
                SELECT signal_type, COUNT(*) AS count
                FROM discovery_alerts
                WHERE created_at >= datetime('now', ?)
                  AND signal_type != ?
                GROUP BY signal_type
                ORDER BY count DESC
                """,
                (cutoff, SignalType.NO_SIGNAL.value),
            ).fetchall()

        signal_rate = (float(signals_found) / float(total_searches)) if total_searches else 0.0
        return {
            "total_searches": int(total_searches),
            "signals_found": int(signals_found),
            "no_signal_count": int(no_signal_count),
            "signal_rate": round(signal_rate, 4),
            "by_type": {str(row["signal_type"]): int(row["count"]) for row in type_rows},
            "acted_on_count": int(acted_on_count),
            "avg_relevance": round(float(avg_relevance or 0.0), 4),
        }
