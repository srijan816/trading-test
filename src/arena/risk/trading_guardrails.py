from __future__ import annotations

from datetime import UTC, datetime, timedelta

from arena.db import ArenaDB

FAILURE_STATUSES = {"rejected", "cancelled", "error"}


def get_active_trading_pause(db: ArenaDB, strategy_id: str) -> dict | None:
    now_iso = datetime.now(UTC).isoformat()
    with db.connect() as conn:
        row = conn.execute(
            """
            SELECT created_at, payload_json
            FROM events
            WHERE strategy_id = ?
              AND event_type = 'trading_pause'
              AND json_extract(payload_json, '$.pause_until') > ?
            ORDER BY created_at DESC, event_id DESC
            LIMIT 1
            """,
            (strategy_id, now_iso),
        ).fetchone()
    if not row:
        return None
    import json

    payload = json.loads(row["payload_json"]) if isinstance(row["payload_json"], str) else (row["payload_json"] or {})
    return payload


def pause_trading(
    db: ArenaDB,
    strategy_id: str,
    *,
    minutes: int,
    reason: str,
    details: dict | None = None,
) -> dict:
    pause_until = datetime.now(UTC) + timedelta(minutes=max(int(minutes), 1))
    payload = {
        "reason": reason,
        "pause_minutes": int(minutes),
        "pause_until": pause_until.isoformat(),
    }
    if details:
        payload.update(details)
    db.record_event("trading_pause", payload, strategy_id=strategy_id)
    return payload


def maybe_trigger_trading_pause(
    db: ArenaDB,
    strategy_id: str,
    *,
    threshold: int,
    minutes: int,
) -> dict | None:
    if get_active_trading_pause(db, strategy_id) is not None:
        return None

    with db.connect() as conn:
        rows = list(
            conn.execute(
                "SELECT status, timestamp FROM executions WHERE strategy_id = ? ORDER BY timestamp DESC LIMIT ?",
                (strategy_id, int(threshold)),
            )
        )
    if len(rows) < int(threshold):
        return None
    if not all(str(row["status"]).lower() in FAILURE_STATUSES for row in rows):
        return None
    return pause_trading(
        db,
        strategy_id,
        minutes=minutes,
        reason=f"{threshold} consecutive order failures",
        details={
            "failure_threshold": int(threshold),
            "failure_statuses": [str(row["status"]).lower() for row in rows],
        },
    )


def compute_daily_pnl(db: ArenaDB, strategy_id: str, window_start: str) -> float:
    with db.connect() as conn:
        realized_row = conn.execute(
            """
            SELECT COALESCE(SUM(realized_pnl), 0) AS realized
            FROM positions
            WHERE strategy_id = ?
              AND lower(status) IN ('closed', 'settled')
              AND datetime(COALESCE(closed_at, last_updated_at)) >= datetime(?)
            """,
            (strategy_id, window_start),
        ).fetchone()
        open_row = conn.execute(
            """
            SELECT COALESCE(SUM(unrealized_pnl), 0) AS unrealized
            FROM positions
            WHERE strategy_id = ?
              AND status = 'open'
            """,
            (strategy_id,),
        ).fetchone()
    realized = float(realized_row["realized"] or 0.0) if realized_row else 0.0
    unrealized = float(open_row["unrealized"] or 0.0) if open_row else 0.0
    return realized + unrealized
