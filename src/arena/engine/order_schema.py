from __future__ import annotations


LIMIT_ORDERS_DDL = """
CREATE TABLE IF NOT EXISTS limit_orders (
    order_id TEXT PRIMARY KEY,
    venue_order_id TEXT,
    market_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    side TEXT NOT NULL,
    limit_price REAL NOT NULL,
    size_dollars REAL NOT NULL,
    quantity REAL NOT NULL,
    model_probability REAL,
    edge_bps INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    ttl_seconds INTEGER DEFAULT 300,
    placed_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    filled_at TEXT,
    fill_price REAL,
    fill_quantity REAL,
    cancel_reason TEXT,
    replaced_by TEXT,
    replaces TEXT,
    metadata_json TEXT,
    FOREIGN KEY (market_id) REFERENCES markets(market_id)
);

CREATE INDEX IF NOT EXISTS idx_limit_orders_status ON limit_orders(status);
CREATE INDEX IF NOT EXISTS idx_limit_orders_market ON limit_orders(market_id);
CREATE INDEX IF NOT EXISTS idx_limit_orders_strategy ON limit_orders(strategy_id);
"""


ORDER_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS order_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    old_status TEXT,
    new_status TEXT,
    details_json TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (order_id) REFERENCES limit_orders(order_id)
);
"""
