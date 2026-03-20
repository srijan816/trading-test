from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from arena.db import ArenaDB
from arena.event_groups import derive_event_group

logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(self, db: ArenaDB, config: dict) -> None:
        self.db = db
        self.max_daily_trades = config.get("max_daily_trades", 20)
        self.max_daily_loss_usd = config.get("max_daily_loss_usd", 50.0)
        self.max_open_positions = config.get("max_open_positions", 10)
        self.max_exposure_per_market = float(
            os.getenv(
                "RISK_MAX_EXPOSURE_PER_MARKET",
                str(config.get("max_exposure_per_market_usd", 75.0)),
            )
        )
        self.max_total_exposure = config.get("max_total_exposure_usd", 200.0)
        self.max_exposure_per_event = float(os.getenv("RISK_MAX_EXPOSURE_PER_EVENT", "100"))
        self.max_positions_per_market = int(os.getenv("RISK_MAX_POSITIONS_PER_MARKET", "2"))
        self.cooldown_after_loss_streak = config.get("cooldown_after_loss_streak", 3)
        self.cooldown_minutes = config.get("cooldown_minutes", 60)

    def _current_risk_window_start(self, strategy_id: str) -> str:
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT MAX(created_at) AS latest FROM events WHERE event_type = 'paper_reset'"
            ).fetchone()
        latest_reset = None
        if row and row["latest"]:
            latest_reset = datetime.fromisoformat(str(row["latest"])).replace(tzinfo=timezone.utc)
        window_start = max(day_start, latest_reset) if latest_reset else day_start
        return window_start.strftime("%Y-%m-%d %H:%M:%S")

    def get_open_exposure(self, market_id: str, venue: str | None = None) -> float:
        query = (
            "SELECT COALESCE(SUM(quantity * avg_entry_price), 0) AS exposure "
            "FROM positions WHERE market_id = ? AND status = 'open'"
        )
        params: list[object] = [market_id]
        if venue:
            query += " AND venue = ?"
            params.append(venue)
        with self.db.connect() as conn:
            row = conn.execute(query, params).fetchone()
        return float(row["exposure"]) if row else 0.0

    def get_open_positions_count(self, market_id: str, venue: str | None = None) -> int:
        query = "SELECT COUNT(*) AS cnt FROM positions WHERE market_id = ? AND status = 'open'"
        params: list[object] = [market_id]
        if venue:
            query += " AND venue = ?"
            params.append(venue)
        with self.db.connect() as conn:
            row = conn.execute(query, params).fetchone()
        return int(row["cnt"]) if row else 0

    def _resolve_event_group(self, market_id: str, venue: str | None = None) -> str | None:
        with self.db.connect() as conn:
            market_row = None
            if venue:
                market_row = conn.execute(
                    "SELECT event_group, question, category, slug, venue FROM markets WHERE market_id = ? AND venue = ?",
                    (market_id, venue),
                ).fetchone()
            if market_row is None:
                market_row = conn.execute(
                    "SELECT event_group, question, category, slug, venue FROM markets WHERE market_id = ? ORDER BY venue LIMIT 1",
                    (market_id,),
                ).fetchone()
        if market_row and market_row["event_group"]:
            return str(market_row["event_group"])
        if market_row:
            derived = derive_event_group(
                str(market_row["question"] or ""),
                str(market_row["category"] or ""),
                str(market_row["venue"] or venue or ""),
                str(market_row["slug"] or ""),
            )
            if derived:
                return derived
        normalized_venue = venue or "unknown"
        return f"{normalized_venue}:fallback:{str(market_id)[:6]}"

    def get_open_exposure_by_event(self, event_id: str | None, market_id: str, venue: str | None = None) -> float:
        event_group = event_id or self._resolve_event_group(market_id, venue)
        if not event_group:
            return 0.0
        with self.db.connect() as conn:
            rows = list(
                conn.execute(
                    "SELECT p.market_id, p.venue, p.quantity, p.avg_entry_price, "
                    "m.event_group, m.question, m.category, m.slug "
                    "FROM positions p "
                    "LEFT JOIN markets m ON m.market_id = p.market_id AND m.venue = p.venue "
                    "WHERE p.status = 'open' AND m.status = 'active'"
                )
            )
        exposure = 0.0
        for row in rows:
            row_event_group = row["event_group"]
            if not row_event_group:
                row_event_group = derive_event_group(
                    str(row["question"] or ""),
                    str(row["category"] or ""),
                    str(row["venue"] or ""),
                    str(row["slug"] or ""),
                )
            if not row_event_group:
                row_event_group = f"{row['venue']}:fallback:{str(row['market_id'])[:6]}"
            if row_event_group == event_group:
                exposure += float(row["quantity"] or 0.0) * float(row["avg_entry_price"] or 0.0)
        return exposure

    def _reject(self, reason: str, **context) -> dict:
        log_context = {key: value for key, value in context.items() if value is not None}
        logger.warning("Risk reject: %s | %s", reason, log_context)
        return {"approved": False, "reason": reason, **log_context}

    async def check_trade(
        self,
        strategy_id: str,
        market_id: str,
        amount_usd: float,
        side: str,
        venue: str | None = None,
    ) -> dict:
        risk_window_start = self._current_risk_window_start(strategy_id)

        with self.db.connect() as conn:
            # Check 1: Daily trade count
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM executions "
                "WHERE strategy_id = ? AND status IN ('filled', 'partial') "
                "AND datetime(timestamp) >= datetime(?)",
                (strategy_id, risk_window_start),
            ).fetchone()
            daily_trades = row["cnt"] if row else 0
            if daily_trades >= self.max_daily_trades:
                return {"approved": False, "reason": f"Daily trade limit reached ({daily_trades}/{self.max_daily_trades})"}

            # Check 2: Daily P&L (approximate from total_cost of today's executions)
            row = conn.execute(
                "SELECT COALESCE(SUM(total_cost), 0) AS total FROM executions "
                "WHERE strategy_id = ? AND status IN ('filled', 'partial') "
                "AND datetime(timestamp) >= datetime(?)",
                (strategy_id, risk_window_start),
            ).fetchone()
            daily_spend = float(row["total"]) if row else 0.0
            if daily_spend > self.max_daily_loss_usd:
                return {"approved": False, "reason": f"Daily loss limit reached (${daily_spend:.2f}/${self.max_daily_loss_usd:.2f})"}

            # Check 3: Open position count
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM positions "
                "WHERE strategy_id = ? AND status = 'open'",
                (strategy_id,),
            ).fetchone()
            open_count = row["cnt"] if row else 0
            if open_count >= self.max_open_positions:
                return {"approved": False, "reason": f"Max open positions reached ({open_count}/{self.max_open_positions})"}

            # Check 4: Per-market exposure
            # Check 4: Per-market exposure and duplicate position count
            market_exposure = self.get_open_exposure(market_id, venue)
            market_positions = self.get_open_positions_count(market_id, venue)
            if market_positions >= self.max_positions_per_market:
                return self._reject(
                    "Max positions per market exceeded.",
                    strategy_id=strategy_id,
                    market_id=market_id,
                    venue=venue,
                    current_positions=market_positions,
                    positions_cap=self.max_positions_per_market,
                    proposed_trade_size=round(amount_usd, 2),
                )
            if market_exposure + amount_usd > self.max_exposure_per_market:
                return self._reject(
                    "Max exposure per market exceeded.",
                    strategy_id=strategy_id,
                    market_id=market_id,
                    venue=venue,
                    current_market_exposure=round(market_exposure, 2),
                    proposed_trade_size=round(amount_usd, 2),
                    exposure_after=round(market_exposure + amount_usd, 2),
                    cap=round(self.max_exposure_per_market, 2),
                )

            # Check 5: Per-event exposure
            event_group = self._resolve_event_group(market_id, venue)
            event_exposure = self.get_open_exposure_by_event(event_group, market_id, venue)
            if event_exposure + amount_usd > self.max_exposure_per_event:
                return self._reject(
                    "Max exposure per event exceeded.",
                    strategy_id=strategy_id,
                    market_id=market_id,
                    venue=venue,
                    event_group=event_group,
                    current_event_exposure=round(event_exposure, 2),
                    proposed_trade_size=round(amount_usd, 2),
                    exposure_after=round(event_exposure + amount_usd, 2),
                    cap=round(self.max_exposure_per_event, 2),
                )

            # Check 6: Total exposure
            row = conn.execute(
                "SELECT COALESCE(SUM(quantity * avg_entry_price), 0) AS exposure FROM positions "
                "WHERE strategy_id = ? AND status = 'open'",
                (strategy_id,),
            ).fetchone()
            total_exposure = float(row["exposure"]) if row else 0.0
            if total_exposure + amount_usd > self.max_total_exposure:
                return self._reject(
                    f"Max total exposure (${total_exposure + amount_usd:.2f}/${self.max_total_exposure:.2f})",
                    strategy_id=strategy_id,
                    market_id=market_id,
                    venue=venue,
                    current_total_exposure=round(total_exposure, 2),
                    proposed_trade_size=round(amount_usd, 2),
                    cap=round(self.max_total_exposure, 2),
                )

            # Check 7: Loss streak cooldown
            recent_execs = list(conn.execute(
                "SELECT status, timestamp FROM executions "
                "WHERE strategy_id = ? ORDER BY timestamp DESC LIMIT ?",
                (strategy_id, self.cooldown_after_loss_streak),
            ))
            if len(recent_execs) >= self.cooldown_after_loss_streak:
                all_rejected = all(r["status"] == "rejected" for r in recent_execs)
                if all_rejected:
                    most_recent = datetime.fromisoformat(recent_execs[0]["timestamp"])
                    now = datetime.now(timezone.utc)
                    minutes_since = (now - most_recent).total_seconds() / 60
                    if minutes_since < self.cooldown_minutes:
                        return {
                            "approved": False,
                            "reason": f"Cooling down after {self.cooldown_after_loss_streak} consecutive failures ({minutes_since:.0f}m/{self.cooldown_minutes}m)",
                        }

        return {"approved": True, "reason": "All checks passed"}
