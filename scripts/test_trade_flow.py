from __future__ import annotations

from dataclasses import asdict
import asyncio
import json

from arena.config import load_app_config
from arena.db import ArenaDB
from arena.engine.paper_executor import PaperExecutor
from arena.engine.portfolio import apply_execution_to_portfolio
from arena.main import build_market_adapters, infer_fee_bps
from arena.models import Decision, EvidenceItem, OrderBookSnapshot, ProposedAction, new_id, utc_now


STRATEGY_ID = "algo_forecast"
TRADE_NOTIONAL_USD = 50.0


def choose_candidate_markets(db: ArenaDB):
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM markets
            WHERE status = 'active'
              AND category = 'weather'
              AND volume_usd >= 5000
              AND end_time > ?
            ORDER BY volume_usd DESC
            LIMIT 20
            """,
            (utc_now().isoformat(),),
        ).fetchall()
    return rows


async def main() -> None:
    app_config = load_app_config()
    db = ArenaDB(app_config.db_path)
    db.initialize()
    db.ensure_portfolio(STRATEGY_ID, 1000.0)

    candidate_markets = choose_candidate_markets(db)
    if not candidate_markets:
        raise RuntimeError("No active weather market with >$5K volume found")

    adapters = build_market_adapters(app_config)
    market = None
    best = None
    orderbook: OrderBookSnapshot | None = None
    for row in candidate_markets:
        outcomes = sorted(
            json.loads(row["outcomes_json"]),
            key=lambda item: float(item.get("mid_price", 0.0) or 0.0),
            reverse=True,
        )
        for outcome in outcomes:
            live_book = await adapters[row["venue"]].get_orderbook(row["market_id"], str(outcome["outcome_id"]))
            if live_book.asks:
                market = row
                best = outcome
                orderbook = live_book
                break
        if market is not None:
            break
    if market is None or best is None or orderbook is None:
        raise RuntimeError("No tradeable weather outcome with live asks found")

    action = ProposedAction(
        action_type="BUY",
        market_id=market["market_id"],
        venue=market["venue"],
        outcome_id=str(best["outcome_id"]),
        outcome_label=str(best["label"]),
        amount_usd=TRADE_NOTIONAL_USD,
        limit_price=float(orderbook.asks[0][0]),
        reasoning_summary="Manual end-to-end executor validation trade.",
    )
    decision = Decision(
        decision_id=new_id("decision"),
        strategy_id=STRATEGY_ID,
        strategy_type="algo",
        timestamp=utc_now(),
        markets_considered=[market["market_id"]],
        predicted_probability=float(best.get("mid_price") or 0.0),
        market_implied_probability=float(best.get("mid_price") or 0.0),
        expected_edge_bps=0,
        confidence=0.5,
        evidence_items=[
            EvidenceItem(
                source="manual_test",
                content=f"Selected highest mid-price outcome for {market['question']}",
                retrieved_at=utc_now(),
            )
        ],
        risk_notes="Executor smoke test only.",
        exit_plan="Hold for paper-trading validation.",
        thinking="Injected manual decision to verify the paper execution pipeline end-to-end.",
        web_searches_used=[],
        actions=[action],
        no_action_reason=None,
        search_queries_count=0,
        search_cost_usd=0.0,
    )
    db.save_decision(decision)

    db.save_orderbook_snapshot(orderbook)

    portfolio = db.get_portfolio(STRATEGY_ID)
    if portfolio is None:
        raise RuntimeError(f"Portfolio missing for {STRATEGY_ID}")
    executor = PaperExecutor(db, extra_slippage_bps=int(app_config.arena["extra_slippage_bps"]))
    execution, position = executor.execute(
        decision_id=decision.decision_id,
        strategy_id=STRATEGY_ID,
        action=action,
        orderbook=orderbook,
        portfolio=portfolio,
        risk_limits=app_config.strategies[STRATEGY_ID].strategy["risk"],
        fee_bps=infer_fee_bps(app_config, market),
    )
    db.save_execution(execution)
    if position:
        db.upsert_position(position)
        portfolio = apply_execution_to_portfolio(portfolio, position, execution)
        db.save_portfolio(portfolio)

    print("MARKET")
    print(json.dumps(
        {
            "market_id": market["market_id"],
            "venue": market["venue"],
            "category": market["category"],
            "question": market["question"],
            "volume_usd": market["volume_usd"],
            "liquidity_usd": market["liquidity_usd"],
            "selected_outcome": best,
        },
        indent=2,
        default=str,
    ))
    print()
    print("ORDERBOOK")
    print(json.dumps(asdict(orderbook), indent=2, default=str))
    print()
    print("DECISION")
    print(json.dumps(asdict(decision), indent=2, default=str))
    print()
    print("EXECUTION")
    print(json.dumps(asdict(execution), indent=2, default=str))
    print()
    refreshed = db.get_portfolio(STRATEGY_ID)
    if refreshed is None:
        raise RuntimeError("Portfolio disappeared after execution")
    print("PORTFOLIO")
    print(json.dumps(asdict(refreshed), indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())
