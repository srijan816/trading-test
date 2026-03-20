from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import json
import shutil
import sqlite3


REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "data" / "arena.db"
CRPS_PATH = REPO_ROOT / "data" / "crps_history.jsonl"
RESET_BALANCE = 10_000.00
STATE_FILE_PATTERNS = (
    "portfolio_state.json",
    "paper_positions.json",
    "account_state.json",
    "paper_executor_state.json",
    "portfolio_manager_state.json",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset paper trading state while preserving historical research and calibration data.")
    parser.add_argument("--confirm", action="store_true", help="Apply the reset. Without this flag the script only prints what it would do.")
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_reset_columns(conn: sqlite3.Connection, dry_run: bool) -> list[str]:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(positions)")}
    actions: list[str] = []
    if "realized_pnl" not in columns:
        actions.append("positions.realized_pnl")
        if not dry_run:
            conn.execute("ALTER TABLE positions ADD COLUMN realized_pnl REAL NOT NULL DEFAULT 0")
    if "closed_at" not in columns:
        actions.append("positions.closed_at")
        if not dry_run:
            conn.execute("ALTER TABLE positions ADD COLUMN closed_at TEXT")
    return actions


def discover_resettable_state_files() -> list[Path]:
    data_dir = REPO_ROOT / "data"
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


def backup_and_reset_state_files(state_files: list[Path], timestamp: str, confirm: bool) -> tuple[Path | None, list[str]]:
    if not state_files:
        return None, []
    backup_dir = REPO_ROOT / "data" / "backups" / f"pre_reset_{timestamp}"
    reset_descriptions: list[str] = []
    if confirm:
        backup_dir.mkdir(parents=True, exist_ok=True)
    for path in state_files:
        if confirm:
            shutil.copy2(path, backup_dir / path.name)
            if path.suffix == ".json":
                path.write_text("{}\n", encoding="utf-8")
            else:
                path.write_bytes(b"")
        reset_descriptions.append(str(path))
    return backup_dir, reset_descriptions


def count_crps_records() -> int:
    if not CRPS_PATH.exists():
        return 0
    with CRPS_PATH.open("r", encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def main() -> None:
    args = parse_args()
    confirm = args.confirm
    mode_label = "CONFIRM" if confirm else "DRY RUN"
    now_iso = utc_now_iso()
    backup_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        added_columns = ensure_reset_columns(conn, dry_run=not confirm)

        open_position_row = conn.execute(
            "SELECT COUNT(*) AS cnt, "
            "COALESCE(SUM(quantity * avg_entry_price), 0) AS exposure, "
            "COALESCE(SUM(unrealized_pnl), 0) AS unrealized "
            "FROM positions WHERE status = 'open'"
        ).fetchone()
        open_positions = int(open_position_row["cnt"] or 0)
        total_exposure = float(open_position_row["exposure"] or 0.0)
        unrealized_pnl = float(open_position_row["unrealized"] or 0.0)

        pending_order_row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM executions "
            "WHERE status IN ('open', 'pending', 'submitted', 'accepted')"
        ).fetchone()
        pending_orders = int(pending_order_row["cnt"] or 0)

        portfolio_rows = list(conn.execute("SELECT strategy_id, cash FROM portfolios ORDER BY strategy_id"))
        previous_balances = {row["strategy_id"]: float(row["cash"] or 0.0) for row in portfolio_rows}
        decisions_preserved = int(conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0])
        research_logs_preserved = int(conn.execute("SELECT COUNT(*) FROM research_log").fetchone()[0])
        crps_preserved = count_crps_records()
        state_files = discover_resettable_state_files()

        print(f"[{mode_label}] Resetting paper trading state in {DB_PATH}")
        print(f"Open positions: {open_positions}")
        print(f"Total exposure: ${total_exposure:,.2f}")
        print(f"Unrealized P&L: ${unrealized_pnl:,.2f}")
        print(f"Pending/open orders: {pending_orders}")
        if added_columns:
            print(f"Would add reset-support columns: {', '.join(added_columns)}")
        if state_files:
            print("Auxiliary state files to back up/reset:")
            for path in state_files:
                print(f"  - {path}")
        else:
            print("Auxiliary state files to back up/reset: none found")

        if not confirm:
            print("Dry run only. Re-run with --confirm to apply the reset.")
            return

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
                (
                    RESET_BALANCE,
                    json.dumps([]),
                    RESET_BALANCE,
                    RESET_BALANCE,
                    now_iso,
                    strategy_id,
                ),
            )

        backup_dir, reset_files = backup_and_reset_state_files(state_files, backup_stamp, confirm=True)

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
                        "reason": "pre-patch positions invalid",
                        "state_files_reset": reset_files,
                        "backup_dir": str(backup_dir) if backup_dir else None,
                    },
                    sort_keys=True,
                ),
            ),
        )
        conn.commit()

        print("Reset complete.")
        print(f"Positions closed: {closed_positions}")
        print(f"Orders cancelled: {cancelled_orders}")
        print(f"Cash reset to: ${RESET_BALANCE:,.2f}")
        print(f"Historical decisions preserved: {decisions_preserved}")
        print(f"Historical research logs preserved: {research_logs_preserved}")
        print(f"CRPS records preserved: {crps_preserved}")
        if backup_dir:
            print(f"State backup directory: {backup_dir}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
