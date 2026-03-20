import asyncio
from datetime import datetime, timezone

from arena.db import ArenaDB
from arena.engine.limit_order_manager import LimitOrderManager
from arena.engine.order_types import LimitOrder, OrderSide, OrderStatus, PlacedOrder


def _manager() -> LimitOrderManager:
    return LimitOrderManager(config={"db_path": ""})


def test_compute_limit_price_returns_none_for_empty_book():
    manager = _manager()
    assert manager.compute_limit_price(OrderSide.BUY_YES, {"bids": [], "asks": []}, {"tick_size": 0.01}) is None


def test_compute_limit_price_returns_none_for_crossed_quotes():
    manager = _manager()
    book = {"bids": [(0.51, 100.0)], "asks": [(0.50, 100.0)]}
    assert manager.compute_limit_price(OrderSide.BUY_YES, book, {"tick_size": 0.01}) is None


def test_compute_limit_price_returns_none_for_narrow_spread():
    manager = _manager()
    book = {"bids": [(0.40, 100.0)], "asks": [(0.41, 100.0)]}
    assert manager.compute_limit_price(OrderSide.BUY_YES, book, {"tick_size": 0.01}) is None


def test_compute_limit_price_uses_passive_baseline_for_small_edge():
    manager = _manager()
    book = {"bids": [(0.40, 20.0)], "asks": [(0.50, 25.0)]}
    price = manager.compute_limit_price(
        OrderSide.BUY_YES,
        book,
        {"tick_size": 0.01, "min_edge_after_fees": 0.01},
        model_probability=0.46,
    )
    assert price == 0.43


def test_compute_limit_price_gets_more_aggressive_with_displayed_size():
    manager = _manager()
    thin_book = {"bids": [(0.40, 20.0)], "asks": [(0.50, 25.0)]}
    book = {"bids": [(0.40, 400.0)], "asks": [(0.50, 450.0)]}
    passive = manager.compute_limit_price(
        OrderSide.BUY_YES,
        thin_book,
        {"tick_size": 0.01, "min_edge_after_fees": 0.01},
        model_probability=0.46,
    )
    deeper = manager.compute_limit_price(
        OrderSide.BUY_YES,
        book,
        {"tick_size": 0.01, "min_edge_after_fees": 0.01},
        model_probability=0.46,
    )
    assert passive == 0.43
    assert deeper == 0.44
    assert deeper > passive


def test_compute_limit_price_gets_more_aggressive_with_edge():
    manager = _manager()
    book = {"bids": [(0.40, 400.0)], "asks": [(0.50, 450.0)]}
    passive = manager.compute_limit_price(
        OrderSide.BUY_YES,
        book,
        {"tick_size": 0.01, "min_edge_after_fees": 0.01},
        model_probability=0.46,
    )
    aggressive = manager.compute_limit_price(
        OrderSide.BUY_YES,
        book,
        {"tick_size": 0.01, "min_edge_after_fees": 0.01},
        model_probability=0.58,
    )
    assert passive == 0.44
    assert aggressive == 0.46
    assert aggressive > passive


def test_compute_limit_price_blocks_when_posted_edge_too_small():
    manager = _manager()
    book = {"bids": [(0.40, 400.0)], "asks": [(0.50, 450.0)]}
    price = manager.compute_limit_price(
        OrderSide.BUY_YES,
        book,
        {"tick_size": 0.01, "min_edge_after_fees": 0.01},
        model_probability=0.45,
    )
    assert price is None


def test_book_fill_merges_partial_fills_into_single_position(tmp_path):
    db_path = tmp_path / "arena.db"
    db = ArenaDB(db_path)
    db.initialize()
    db.ensure_portfolio("s1", 1000.0)
    manager = LimitOrderManager(db_path=str(db_path), config={"default_starting_balance": 1000.0})
    order = LimitOrder(
        market_id="m1",
        side=OrderSide.BUY_YES,
        price=0.44,
        size_dollars=44.0,
        quantity=100.0,
        strategy_id="s1",
        model_probability=0.55,
        edge_bps=1100,
        metadata={"venue": "polymarket", "outcome_id": "yes", "outcome_label": "Yes", "decision_id": "d1"},
    )
    placed = PlacedOrder(
        order_id="o1",
        venue_order_id="vo1",
        order=order,
        status=OrderStatus.OPEN,
        placed_at=datetime.now(timezone.utc),
    )

    async def run() -> None:
        await manager._book_fill(placed, 0.44, 30.0, OrderStatus.PARTIALLY_FILLED, snapshot_id="snap1", midpoint_at_fill=0.45)
        await manager._book_fill(placed, 0.46, 20.0, OrderStatus.PARTIALLY_FILLED, snapshot_id="snap2", midpoint_at_fill=0.47)

    asyncio.run(run())

    positions = db.list_open_positions("s1")
    assert len(positions) == 1
    assert positions[0].quantity == 50.0
    assert round(positions[0].avg_entry_price, 4) == 0.448
    portfolio = db.get_portfolio("s1")
    assert portfolio is not None
    assert round(portfolio.cash, 2) == 977.60
