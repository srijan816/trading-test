#!/usr/bin/env python3
"""Recategorize all markets using the improved scoring system.

Prints before/after distribution and updates both primary and secondary categories.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from arena.categorization import categorize_market, categorize_market_detailed, detect_market_format
from arena.config import load_app_config
from arena.db import ArenaDB
from arena.env import load_local_env


def main() -> None:
    load_local_env()
    app_config = load_app_config()
    db = ArenaDB(app_config.db_path)
    db.initialize()

    # --- Before snapshot ---
    with db.connect() as conn:
        before_rows = list(conn.execute(
            "SELECT category, COUNT(*) AS cnt FROM markets GROUP BY category ORDER BY cnt DESC"
        ))
        before_format_rows = list(conn.execute(
            "SELECT market_format, COUNT(*) AS cnt FROM markets GROUP BY market_format ORDER BY cnt DESC"
        ))
    print("=== BEFORE (categories) ===")
    total = 0
    for row in before_rows:
        print(f"  {row['category']:20s} {row['cnt']:>6d}")
        total += row["cnt"]
    print(f"  {'TOTAL':20s} {total:>6d}")
    print()
    print("=== BEFORE (market_format) ===")
    for row in before_format_rows:
        fmt = row["market_format"] or "(NULL)"
        print(f"  {fmt:20s} {row['cnt']:>6d}")
    print()

    # --- Recategorize ---
    updated = 0
    secondary_updated = 0
    format_updated = 0
    with db.connect() as conn:
        rows = list(conn.execute(
            "SELECT market_id, venue, question, category, market_format FROM markets"
        ))
        for row in rows:
            primary, secondary = categorize_market_detailed(
                row["question"], current_category=row["category"]
            )
            fmt = detect_market_format(row["question"])
            if primary != row["category"]:
                conn.execute(
                    "UPDATE markets SET category = ? WHERE market_id = ? AND venue = ?",
                    (primary, row["market_id"], row["venue"]),
                )
                updated += 1
            try:
                conn.execute(
                    "UPDATE markets SET secondary_category = ? WHERE market_id = ? AND venue = ?",
                    (secondary, row["market_id"], row["venue"]),
                )
                if secondary:
                    secondary_updated += 1
            except Exception:
                pass
            try:
                if fmt != row["market_format"]:
                    conn.execute(
                        "UPDATE markets SET market_format = ? WHERE market_id = ? AND venue = ?",
                        (fmt, row["market_id"], row["venue"]),
                    )
                    format_updated += 1
            except Exception:
                pass

    print(f"Updated {updated} primary categories, {secondary_updated} secondary, {format_updated} market_format.")
    print()

    # --- After snapshot ---
    with db.connect() as conn:
        after_rows = list(conn.execute(
            "SELECT category, COUNT(*) AS cnt FROM markets GROUP BY category ORDER BY cnt DESC"
        ))
        after_format_rows = list(conn.execute(
            "SELECT market_format, COUNT(*) AS cnt FROM markets GROUP BY market_format ORDER BY cnt DESC"
        ))
        # Cross-tab: category × format
        cross_rows = list(conn.execute(
            "SELECT category, market_format, COUNT(*) AS cnt "
            "FROM markets GROUP BY category, market_format ORDER BY category, market_format"
        ))
    print("=== AFTER (categories) ===")
    total = 0
    for row in after_rows:
        print(f"  {row['category']:20s} {row['cnt']:>6d}")
        total += row["cnt"]
    print(f"  {'TOTAL':20s} {total:>6d}")

    event_count = next((row["cnt"] for row in after_rows if row["category"] == "event"), 0)
    if total > 0:
        event_pct = event_count / total * 100
        categorized_pct = 100 - event_pct
        print(f"\n  Categorized: {categorized_pct:.1f}%  |  Still 'event': {event_pct:.1f}%")
    print()

    print("=== AFTER (market_format) ===")
    for row in after_format_rows:
        fmt = row["market_format"] or "(NULL)"
        print(f"  {fmt:20s} {row['cnt']:>6d}")
    print()

    print("=== AFTER (category × format) ===")
    for row in cross_rows:
        fmt = row["market_format"] or "(NULL)"
        print(f"  {row['category']:20s} {fmt:20s} {row['cnt']:>6d}")


if __name__ == "__main__":
    main()
