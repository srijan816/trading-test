from __future__ import annotations

import json
import logging
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

RESET_BALANCE = 10_000.00
STATE_FILE_PATTERNS = (
    "portfolio_state.json",
    "paper_positions.json",
    "account_state.json",
    "paper_executor_state.json",
    "portfolio_manager_state.json",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _ensure_reset_columns(conn: sqlite3.Connection) -> None:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(positions)")}
    if "realized_pnl" not in columns:
        conn.execute("ALTER TABLE positions ADD COLUMN realized_pnl REAL NOT NULL DEFAULT 0")
    if "closed_at" not in columns:
        conn.execute("ALTER TABLE positions ADD COLUMN closed_at TEXT")


def _discover_state_files(data_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    for pattern in STATE_FILE_PATTERNS:
        path = data_dir / pattern
        if path.exists():
            candidates.append(path)
    for path in sorted(data_dir.glob("*")):
        if not path.is_file():
            continue
        lowered = path.name.lower()
        if any(token in lowered for token in ("portfolio", "paper", "account")) and path.suffix in {".json", ".pkl", ".pickle"}:
            if path not in candidates:
                candidates.append(path)
    return sorted(candidates)


def _backup_and_reset_state_files(state_files: list[Path], data_dir: Path, stamp: str) -> tuple[Path | None, list[str]]:
    if not state_files:
        return None, []
    backup_dir = data_dir / "backups" / f"pre_reset_{stamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    reset_files: list[str] = []
    for path in state_files:
        shutil.copy2(path, backup_dir / path.name)
        if path.suffix == ".json":
            path.write_text("{}\n", encoding="utf-8")
        else:
            path.write_bytes(b"")
        reset_files.append(str(path))
    return backup_dir, reset_files


def reset_paper_trading(db_path: str | Path, reason: str = "dashboard reset") -> dict:
    """Reset all paper trading state. Returns a summary dict."""
    db_path = Path(db_path)
    data_dir = db_path.parent
    now_iso = _utc_now_iso()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        _ensure_reset_columns(conn)

        open_row = conn.execute(
            "SELECT COUNT(*) AS cnt, "
            "COALESCE(SUM(quantity * avg_entry_price), 0) AS exposure "
            "FROM positions WHERE status = 'open'"
        ).fetchone()
        open_positions = int(open_row["cnt"] or 0)
        total_exposure = float(open_row["exposure"] or 0.0)

        portfolio_rows = list(conn.execute("SELECT strategy_id, cash FROM portfolios ORDER BY strategy_id"))
        previous_balances = {row["strategy_id"]: float(row["cash"] or 0.0) for row in portfolio_rows}

        closed_positions = conn.execute(
            "UPDATE positions "
            "SET status = 'reset_cancelled', realized_pnl = 0, closed_at = ?, last_updated_at = ? "
            "WHERE status = 'open'",
            (now_iso, now_iso),
        ).rowcount or 0

        cancelled_orders = conn.execute(
            "UPDATE executions "
            "SET status = 'reset_cancelled', rejection_reason = COALESCE(rejection_reason, 'paper_reset') "
            "WHERE status IN ('open', 'pending', 'submitted', 'accepted')",
        ).rowcount or 0

        for row in portfolio_rows:
            strategy_id = str(row["strategy_id"])
            conn.execute(
                "UPDATE portfolios SET cash = ?, positions_json = ?, total_value = ?, realized_pnl = 0, "
                "unrealized_pnl = 0, total_trades = 0, winning_trades = 0, losing_trades = 0, "
                "max_drawdown = 0, peak_value = ?, updated_at = ? WHERE strategy_id = ?",
                (RESET_BALANCE, json.dumps([]), RESET_BALANCE, RESET_BALANCE, now_iso, strategy_id),
            )

        state_files = _discover_state_files(data_dir)
        backup_dir, reset_files = _backup_and_reset_state_files(state_files, data_dir, stamp)

        conn.execute(
            "INSERT INTO events(event_type, strategy_id, payload_json) VALUES (?, ?, ?)",
            (
                "paper_reset",
                None,
                json.dumps(
                    {
                        "timestamp": now_iso,
                        "positions_closed": closed_positions,
                        "orders_cancelled": cancelled_orders,
                        "previous_balances": previous_balances,
                        "cash_reset_to": RESET_BALANCE,
                        "reason": reason,
                        "state_files_reset": reset_files,
                        "backup_dir": str(backup_dir) if backup_dir else None,
                    },
                    sort_keys=True,
                ),
            ),
        )
        conn.commit()

        summary = {
            "positions_closed": closed_positions,
            "orders_cancelled": cancelled_orders,
            "previous_balances": previous_balances,
            "cash_reset_to": RESET_BALANCE,
            "total_exposure_before": round(total_exposure, 2),
            "state_files_reset": len(reset_files),
            "backup_dir": str(backup_dir) if backup_dir else None,
        }
        logger.info("Paper reset complete: %s", summary)
        return summary
    finally:
        conn.close()
