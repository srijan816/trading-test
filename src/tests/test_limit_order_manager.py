from arena.engine.limit_order_manager import LimitOrderManager
from arena.engine.order_types import OrderSide


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
