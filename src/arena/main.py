from __future__ import annotations

# Load .env before importing modules that may read environment variables.
try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - fallback for lean runtime environments
    def load_dotenv(*_args, **_kwargs):
        return False

load_dotenv()

import argparse
import asyncio
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
import logging
import os
from pathlib import Path
import json
import signal
import sys
from typing import Any

from arena.categorization import categorize_market
from arena.calibration.confidence_gate import ConfidenceGate
from arena.config import ROOT, AppConfig, load_app_config
from arena.db import ArenaDB
from arena.engine.limit_order_manager import LimitOrderManager
from arena.engine.paper_executor import PaperExecutor
from arena.engine.paper_limit_executor import PaperLimitExecutor
from arena.engine.portfolio import apply_execution_to_portfolio, close_position, compute_position_unrealized
from arena.engine.settlement import SettlementEngine
from arena.exchanges.kalshi_adapter import KalshiAdapter as KalshiExchangeAdapter
from arena.exchanges.polymarket_limit import PolymarketPublicReader
from arena.export.cli_reports import render_table
from arena.export.sheets_sync import build_dashboard_payloads
from arena.env import load_local_env
from arena.filters.spread_filter import SpreadFilter
from arena.models import DailySnapshot, Decision, ExecutionResult, Market, OrderBookSnapshot, ProposedAction, utc_now
from arena.risk.kelly import compute_position_size
from arena.risk.risk_manager import RiskManager
from arena.risk.trading_guardrails import FAILURE_STATUSES, maybe_trigger_trading_pause
from arena.strategies.algo_forecast import ForecastConsensusStrategy
from arena.strategies.algo_harvester import LateStageHarvesterStrategy
from arena.strategies.algo_partition import PartitionArbitrageStrategy
from arena.strategies.llm_strategy import LLMStrategy

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DISABLED_STRATEGIES = {
    "algo_meanrev": "invalid model, do not run",
    "algo_momentum": "invalid model, do not run",
    "llm_contrarian": "no identified edge as independent probability estimator",
    "llm_generalist": "no identified edge as independent probability estimator",
    "llm_strategist": "no identified edge as independent probability estimator",
}


@dataclass(slots=True)
class ExecutionServices:
    adapters: dict[str, object]
    paper_executor: PaperExecutor
    paper_limit_executor: PaperLimitExecutor
    limit_order_manager: LimitOrderManager
    confidence_gate: ConfidenceGate
    public_reader: PolymarketPublicReader

    async def close(self) -> None:
        await self.public_reader.close()


def get_reentry_price_delta_threshold() -> float:
    return float(os.getenv("RISK_REENTRY_PRICE_DELTA_CENTS", "5")) / 100.0


def get_db(app_config: AppConfig) -> ArenaDB:
    return ArenaDB(app_config.db_path)


def _limit_order_config(app_config: AppConfig) -> dict[str, Any]:
    limit_cfg = dict(app_config.execution.get("limit_orders", {}))
    limit_cfg.setdefault("db_path", str(app_config.db_path))
    limit_cfg.setdefault("default_starting_balance", float(app_config.arena.get("default_starting_balance", 1000.0)))
    return limit_cfg


def build_execution_services(app_config: AppConfig, db: ArenaDB) -> ExecutionServices:
    adapters = build_market_adapters(app_config)
    public_reader = PolymarketPublicReader(base_url=app_config.venues["polymarket"]["clob_base_url"])
    limit_cfg = _limit_order_config(app_config)
    paper_limit_executor = PaperLimitExecutor(
        config=limit_cfg,
        market_data_adapter=public_reader,
    )
    limit_order_manager = LimitOrderManager(
        db_path=str(app_config.db_path),
        venue_adapter=paper_limit_executor,
        config=limit_cfg,
    )
    return ExecutionServices(
        adapters=adapters,
        paper_executor=PaperExecutor(db, extra_slippage_bps=int(app_config.arena["extra_slippage_bps"])),
        paper_limit_executor=paper_limit_executor,
        limit_order_manager=limit_order_manager,
        confidence_gate=ConfidenceGate(
            db_path=str(app_config.db_path),
            crps_history_path=str(ROOT / "data" / "crps_history.jsonl"),
        ),
        public_reader=public_reader,
    )


def build_market_adapters(app_config: AppConfig) -> dict[str, object]:
    from arena.adapters.kalshi import KalshiAdapter
    from arena.adapters.polymarket import PolymarketAdapter

    return {
        "polymarket": PolymarketAdapter(
            gamma_base_url=app_config.venues["polymarket"]["gamma_base_url"],
            clob_base_url=app_config.venues["polymarket"]["clob_base_url"],
        ),
        "kalshi": KalshiAdapter(base_url=app_config.venues["kalshi"]["base_url"]),
    }


def build_llm_client(app_config: AppConfig, strategy_cfg: dict, provider_override: str | None = None):
    from arena.adapters.llm_google import GoogleAILLMClient
    from arena.adapters.llm_manual import ManualLLMClient
    from arena.adapters.llm_minimax import MiniMaxLLMClient
    from arena.adapters.llm_nvidia import NvidiaLLMClient
    from arena.adapters.llm_openrouter import OpenRouterLLMClient

    provider = provider_override or strategy_cfg.get("model", {}).get("provider")
    if provider == "minimax":
        return MiniMaxLLMClient(app_config.models["providers"]["minimax"]["base_url"])
    if provider == "openrouter":
        return OpenRouterLLMClient(app_config.models["providers"]["openrouter"]["base_url"])
    if provider == "google_ai_studio":
        return GoogleAILLMClient(app_config.models["providers"]["google_ai_studio"]["base_url"])
    if provider in {"nvidia_direct", "nvidia_nim"}:
        provider_key = "nvidia_nim" if "nvidia_nim" in app_config.models["providers"] else "nvidia_direct"
        return NvidiaLLMClient(app_config.models["providers"][provider_key]["base_url"])
    if provider == "manual":
        return ManualLLMClient(ROOT / app_config.manual["pending_dir"], ROOT / app_config.manual["responses_dir"])
    raise ValueError(f"Unsupported provider {provider}")


def build_search_client(app_config: AppConfig, strategy_cfg: dict):
    from arena.adapters.search_fallback import FallbackSearchClient
    from arena.adapters.search_perplexia import PerplexiaSearchClient
    from arena.adapters.search_serper import SerperSearchClient

    def _single_client(provider_name: str | None):
        if provider_name == "serper":
            return SerperSearchClient(app_config.models["providers"]["serper"]["base_url"])
        if provider_name in {"perplexia", "nexus"}:
            provider_cfg = app_config.models["providers"]["perplexia"]
            return PerplexiaSearchClient(
                base_url=provider_cfg["base_url"],
                api_key_env=provider_cfg.get("env_key", "NEXUS_API_KEY"),
                timeout=float(provider_cfg.get("timeout_seconds", 90)),
                mode=search_cfg.get("mode", "standard"),
                output_length=search_cfg.get("output_length", "short"),
                model=search_cfg.get("model_id"),
                max_sources=int(search_cfg.get("max_sources", max(search_cfg.get("max_searches_per_cycle", 3), 3))),
                session_id=search_cfg.get("session_id"),
            )
        return None

    search_cfg = strategy_cfg.get("search", {})
    provider = search_cfg.get("provider") or "perplexia"
    fallback_provider = search_cfg.get("fallback_provider")
    primary_client = _single_client(provider)
    fallback_client = _single_client(fallback_provider)
    if primary_client and fallback_client:
        return FallbackSearchClient(primary_client, fallback_client)
    return primary_client


async def scan_markets(app_config: AppConfig, db: ArenaDB) -> None:
    from arena.adapters.polymarket_weather import PolymarketWeatherDiscovery

    adapters = build_market_adapters(app_config)
    for venue, adapter in adapters.items():
        try:
            markets = await adapter.list_active_markets()
        except Exception as exc:
            db.record_event("scan_error", {"venue": venue, "error": str(exc)})
            continue
        for market in markets:
            db.upsert_market(market)
        if venue == "polymarket":
            try:
                discovery = PolymarketWeatherDiscovery()
                result = await discovery.discover_raw_markets()
                for warning in result.warnings:
                    db.record_event("weather_discovery_warning", {"venue": venue, "warning": warning})
                for raw_market in result.markets:
                    db.upsert_market(adapter._normalize_market(raw_market))
            except Exception as exc:
                db.record_event("weather_discovery_error", {"venue": venue, "error": str(exc)})


async def run_discovery_scout(app_config: AppConfig, db: ArenaDB) -> None:
    from arena.intelligence.discovery_scout import DiscoveryUniverseScanner

    try:
        scanner = DiscoveryUniverseScanner(db)
        result = await scanner.scan()
        db.record_event("discovery_scout", result)
    except Exception as exc:
        logger.exception("Discovery scout failed")
        db.record_event("discovery_scout_error", {"error": str(exc)})


def _apply_execution_circuit_breaker(db: ArenaDB, execution: ExecutionResult) -> None:
    if str(execution.status).lower() not in FAILURE_STATUSES:
        return
    pause = maybe_trigger_trading_pause(
        db,
        execution.strategy_id,
        threshold=int(os.getenv("RISK_ORDER_FAILURE_PAUSE_THRESHOLD", "5")),
        minutes=int(os.getenv("RISK_ORDER_FAILURE_PAUSE_MINUTES", "5")),
    )
    if pause is not None:
        db.record_event(
            "execution_circuit_breaker",
            {
                "strategy_id": execution.strategy_id,
                "execution_id": execution.execution_id,
                "market_id": execution.market_id,
                "reason": pause.get("reason"),
                "pause_until": pause.get("pause_until"),
            },
            strategy_id=execution.strategy_id,
        )


def print_status(app_config: AppConfig, db: ArenaDB) -> None:
    counts = db.counts()
    print(
        f"{counts['markets']} markets, {len(app_config.strategies)} strategies, "
        f"{counts['executions']} trades, {counts['decisions']} decisions"
    )
    rows = [
        {
            "strategy_id": row["strategy_id"],
            "cash": row["cash"],
            "total_value": row["total_value"],
            "realized_pnl": row["realized_pnl"],
        }
        for row in db.list_portfolios()
    ]
    if rows:
        print()
        print(render_table(rows))


def print_markets(db: ArenaDB, category: str | None = None) -> None:
    rows = [
        {
            "market_id": row["market_id"],
            "venue": row["venue"],
            "category": row["category"],
            "question": row["question"],
            "status": row["status"],
            "end_time": row["end_time"],
        }
        for row in db.list_markets(category=category)
    ]
    print(render_table(rows))


def ensure_portfolios(app_config: AppConfig, db: ArenaDB) -> None:
    for strategy in app_config.strategies.values():
        db.ensure_portfolio(strategy.strategy_id, float(strategy.strategy.get("starting_balance", 1000.0)))


def init_portfolios(app_config: AppConfig, db: ArenaDB) -> None:
    ensure_portfolios(app_config, db)
    db.sync_portfolios_to_targets(
        {
            strategy.strategy_id: float(strategy.strategy.get("starting_balance", 1000.0))
            for strategy in app_config.strategies.values()
        }
    )
    print("Portfolios initialized.")
    print_status(app_config, db)


def recategorize_markets(db: ArenaDB) -> None:
    updated = db.recategorize_markets(categorize_market)
    print(f"Recategorized {updated} markets.")


async def run_strategy_once(
    app_config: AppConfig,
    db: ArenaDB,
    strategy_id: str,
    execution_services: ExecutionServices | None = None,
) -> Decision:
    strategy_cfg = app_config.strategies[strategy_id].strategy
    if not bool(strategy_cfg.get("enabled", True)):
        raise RuntimeError(f"Strategy {strategy_id} is disabled in config")
    if strategy_id in DISABLED_STRATEGIES:
        raise RuntimeError(f"Strategy {strategy_id} is disabled: {DISABLED_STRATEGIES[strategy_id]}")
    ensure_portfolios(app_config, db)
    if app_config.stop_file.exists():
        raise RuntimeError("STOP file present; strategy execution halted")
    if strategy_cfg["type"] == "llm":
        fallback_provider = strategy_cfg.get("model", {}).get("fallback_provider")
        strategy = LLMStrategy(
            db=db,
            strategy_config=strategy_cfg,
            llm_client=build_llm_client(app_config, strategy_cfg),
            fallback_client=build_llm_client(app_config, strategy_cfg, provider_override=fallback_provider) if fallback_provider else None,
            fallback_model_id=strategy_cfg.get("model", {}).get("fallback_model_id"),
            search_client=build_search_client(app_config, strategy_cfg),
        )
    else:
        factory = {
            "algo_forecast": ForecastConsensusStrategy,
            "algo_harvester": LateStageHarvesterStrategy,
            "algo_partition": PartitionArbitrageStrategy,
        }[strategy_id]
        strategy = factory(db=db, strategy_config=strategy_cfg)
    decision = await strategy.generate_decision()
    db.save_decision(decision)
    if decision.actions:
        marked = db.mark_research_used_in_decision(
            strategy=decision.strategy_id,
            market_ids=[str(action.market_id) for action in decision.actions if action.market_id],
            decision_time=decision.timestamp,
        )
        if marked:
            logger.info(
                "Marked %s research log rows as used in decision %s",
                marked,
                decision.decision_id,
            )
    await execute_decision(app_config, db, strategy_cfg, decision, execution_services=execution_services)
    packet_builder = getattr(strategy, "packet_builder", None)
    if packet_builder and hasattr(packet_builder, "get_research_stats"):
        stats = packet_builder.get_research_stats()
        logger.info(
            "Research stats: %s markets evaluated, %s cache hits, %s Nexus calls, %s remaining in rate window",
            stats.get("markets_evaluated", 0),
            stats.get("cache_hits", 0),
            stats.get("nexus_calls", 0),
            stats.get("remaining_in_rate_window", 0),
        )
    return decision


async def execute_decision(
    app_config: AppConfig,
    db: ArenaDB,
    strategy_cfg: dict,
    decision: Decision,
    execution_services: ExecutionServices | None = None,
) -> None:
    if not decision.actions:
        return
    owns_services = execution_services is None
    services = execution_services or build_execution_services(app_config, db)
    try:
        adapters = services.adapters
        kalshi_exchange = KalshiExchangeAdapter()
        portfolio = db.get_portfolio(decision.strategy_id)
        if not portfolio:
            return
        risk_cfg = strategy_cfg.get("risk_management", strategy_cfg.get("risk", {}))
        risk_manager = RiskManager(db, risk_cfg)
        contract_parser = ForecastConsensusStrategy(
            db=db,
            strategy_config={"id": "execution_helper", "starting_balance": 1000.0, "scope": {}, "risk": strategy_cfg.get("risk", {})},
        )
        orderbook_cache: dict[tuple[str, str, str], OrderBookSnapshot | None] = {}
        for action in decision.actions:
            market_row = db.get_market(action.market_id, action.venue)

            market_active = market_row is not None and market_row["status"] == "active"
            db.record_event(
                "execution_gate_market_active",
                {
                    "decision_id": decision.decision_id,
                    "strategy_id": decision.strategy_id,
                    "market_id": action.market_id,
                    "outcome_id": action.outcome_id,
                    "venue": action.venue,
                    "pass": market_active,
                    "market_status": market_row["status"] if market_row else None,
                },
                strategy_id=decision.strategy_id,
            )
            if not market_active:
                db.record_event(
                    "execution_skip",
                    {
                        "decision_id": decision.decision_id,
                        "strategy_id": decision.strategy_id,
                        "market_id": action.market_id,
                        "outcome_id": action.outcome_id,
                        "venue": action.venue,
                        "error": "market_not_active",
                        "gate": "market_active",
                    },
                    strategy_id=decision.strategy_id,
                )
                continue

            if market_row and market_row["category"] == "weather":
                contract = contract_parser._parse_weather_contract(market_row["question"])
                city = contract.get("city") if contract else None
                if city:
                    tradeable, reason = services.confidence_gate.is_tradeable(city)
                    db.record_event(
                        "execution_gate_confidence",
                        {
                            "decision_id": decision.decision_id,
                            "strategy_id": decision.strategy_id,
                            "market_id": action.market_id,
                            "outcome_id": action.outcome_id,
                            "venue": action.venue,
                            "city": city,
                            "pass": tradeable,
                            "reason": reason,
                        },
                        strategy_id=decision.strategy_id,
                    )
                    if not tradeable:
                        db.record_event(
                            "execution_skip",
                            {
                                "decision_id": decision.decision_id,
                                "strategy_id": decision.strategy_id,
                                "market_id": action.market_id,
                                "outcome_id": action.outcome_id,
                                "venue": action.venue,
                                "error": f"confidence_gate: {reason}",
                                "city": city,
                            },
                            strategy_id=decision.strategy_id,
                        )
                        continue

            risk_result = await risk_manager.check_trade(
                strategy_id=decision.strategy_id,
                market_id=action.market_id,
                amount_usd=action.amount_usd,
                side=action.action_type,
                venue=action.venue,
            )
            risk_approved = risk_result.get("approved", False)
            db.record_event(
                "execution_gate_risk_approval",
                {
                    "decision_id": decision.decision_id,
                    "strategy_id": decision.strategy_id,
                    "market_id": action.market_id,
                    "outcome_id": action.outcome_id,
                    "venue": action.venue,
                    "pass": risk_approved,
                    "reason": risk_result.get("reason"),
                },
                strategy_id=decision.strategy_id,
            )
            if not risk_approved:
                db.record_event(
                    "execution_skip",
                    {
                        "decision_id": decision.decision_id,
                        "strategy_id": decision.strategy_id,
                        "market_id": action.market_id,
                        "outcome_id": action.outcome_id,
                        "venue": action.venue,
                        "error": f"risk_check: {risk_result.get('reason')}",
                        "risk_result": risk_result,
                    },
                    strategy_id=decision.strategy_id,
                )
                continue

            if kalshi_exchange.enabled and market_row and market_row["category"] == "weather":
                try:
                    contract = contract_parser._parse_weather_contract(market_row["question"])
                    if contract and contract.get("dated", True):
                        outcomes = json.loads(market_row["outcomes_json"])
                        yes_outcome, no_outcome = contract_parser._binary_outcomes(outcomes)
                        if yes_outcome and no_outcome:
                            comparison = kalshi_exchange.compare_market_prices(
                                city=contract["city"],
                                target_date=contract["forecast_date"].isoformat(),
                                polymarket_question=market_row["question"],
                                polymarket_yes_ask=contract_parser._buy_price(yes_outcome),
                                polymarket_no_ask=contract_parser._buy_price(no_outcome),
                            )
                            if comparison:
                                db.record_event(
                                    "cross_platform_price_comparison",
                                    {
                                        "decision_id": decision.decision_id,
                                        "strategy_id": decision.strategy_id,
                                        "market_id": action.market_id,
                                        "venue": action.venue,
                                        "comparison": comparison,
                                    },
                                    strategy_id=decision.strategy_id,
                                )
                except Exception as exc:
                    logger.warning("Kalshi price comparison skipped for %s: %s", action.market_id, exc)

            adapter = adapters[action.venue]
            orderbook = None
            orderbook_error = None
            try:
                orderbook_key = (str(action.venue), str(action.market_id), str(action.outcome_id))
                if orderbook_key in orderbook_cache:
                    orderbook = orderbook_cache[orderbook_key]
                else:
                    orderbook = await adapter.get_orderbook(action.market_id, action.outcome_id)
                    orderbook_cache[orderbook_key] = orderbook
            except Exception as exc:
                orderbook_error = str(exc)
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                if status_code == 404:
                    db.record_event(
                        "orderbook_stale",
                        {
                            "decision_id": decision.decision_id,
                            "strategy_id": decision.strategy_id,
                            "market_id": action.market_id,
                            "outcome_id": action.outcome_id,
                            "venue": action.venue,
                            "status_code": 404,
                        },
                        strategy_id=decision.strategy_id,
                    )

            has_orderbook = orderbook is not None
            best_bid = None
            best_ask = None
            if orderbook:
                best_bid = max((float(price) for price, _ in orderbook.bids), default=None)
                best_ask = min((float(price) for price, _ in orderbook.asks), default=None)
            db.record_event(
                "execution_gate_orderbook",
                {
                    "decision_id": decision.decision_id,
                    "strategy_id": decision.strategy_id,
                    "market_id": action.market_id,
                    "outcome_id": action.outcome_id,
                    "venue": action.venue,
                    "pass": has_orderbook,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread": round(best_ask - best_bid, 4) if best_bid is not None and best_ask is not None else None,
                    "error": orderbook_error,
                },
                strategy_id=decision.strategy_id,
            )
            if orderbook_error or not has_orderbook:
                db.record_event(
                    "execution_skip",
                    {
                        "decision_id": decision.decision_id,
                        "strategy_id": decision.strategy_id,
                        "market_id": action.market_id,
                        "outcome_id": action.outcome_id,
                        "venue": action.venue,
                        "error": orderbook_error or "no orderbook",
                    },
                    strategy_id=decision.strategy_id,
                )
                continue

            db.save_orderbook_snapshot(orderbook)
            with db.connect() as conn:
                existing_row = conn.execute(
                    "SELECT COUNT(*) AS cnt, "
                    "COALESCE(SUM(quantity * avg_entry_price), 0) / NULLIF(SUM(quantity), 0) AS avg_entry_price "
                    "FROM positions WHERE market_id = ? AND venue = ? AND status = 'open'",
                    (action.market_id, action.venue),
                ).fetchone()
            existing_count = int(existing_row["cnt"] or 0) if existing_row else 0
            avg_entry_price = float(existing_row["avg_entry_price"]) if existing_row and existing_row["avg_entry_price"] is not None else None
            current_market_price = float(orderbook.mid)
            reentry_threshold = get_reentry_price_delta_threshold()
            price_delta = abs(current_market_price - avg_entry_price) if avg_entry_price is not None else None
            reentry_allowed = not (existing_count > 0 and avg_entry_price is not None and price_delta <= reentry_threshold)
            db.record_event(
                "execution_gate_reentry",
                {
                    "decision_id": decision.decision_id,
                    "strategy_id": decision.strategy_id,
                    "market_id": action.market_id,
                    "venue": action.venue,
                    "outcome_id": action.outcome_id,
                    "pass": reentry_allowed,
                    "existing_positions": existing_count,
                    "avg_entry_price": round(avg_entry_price, 4) if avg_entry_price is not None else None,
                    "current_market_price": round(current_market_price, 4),
                    "price_delta": round(price_delta, 4) if existing_count > 0 and avg_entry_price is not None else None,
                    "reentry_threshold": round(reentry_threshold, 4),
                },
                strategy_id=decision.strategy_id,
            )
            if not reentry_allowed:
                db.record_event(
                    "reentry_blocked",
                    {
                        "decision_id": decision.decision_id,
                        "strategy_id": decision.strategy_id,
                        "market_id": action.market_id,
                        "venue": action.venue,
                        "existing_positions": existing_count,
                        "avg_entry_price": round(avg_entry_price, 4),
                        "current_market_price": round(current_market_price, 4),
                        "delta": round(price_delta, 4),
                        "threshold": round(reentry_threshold, 4),
                    },
                    strategy_id=decision.strategy_id,
                )
                continue

            outcome_side = "yes" if action.outcome_label.lower() == "yes" else "no"
            selected_probability = (
                float(decision.predicted_probability)
                if outcome_side == "yes"
                else 1.0 - float(decision.predicted_probability)
            ) if decision.predicted_probability is not None else None
            volume_proxy = None
            if market_row is not None:
                raw_volume = market_row["volume_usd"]
                if raw_volume is not None:
                    try:
                        volume_proxy = int(round(float(raw_volume)))
                    except (TypeError, ValueError):
                        volume_proxy = None
            spread_result = (
                SpreadFilter.check(
                    our_probability=selected_probability,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    volume=volume_proxy,
                    side=outcome_side,
                )
                if selected_probability is not None and best_bid is not None and best_ask is not None
                else {
                    "pass": False,
                    "reason": "missing probability or bid/ask for spread filter",
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "our_probability": selected_probability,
                    "side": outcome_side,
                    "volume": volume_proxy,
                }
            )
            spread_value = round(best_ask - best_bid, 4) if best_bid is not None and best_ask is not None else None
            db.record_event(
                "execution_gate_spread_filter",
                {
                    "decision_id": decision.decision_id,
                    "strategy_id": decision.strategy_id,
                    "market_id": action.market_id,
                    "outcome_id": action.outcome_id,
                    "venue": action.venue,
                    "pass": spread_result["pass"],
                    "spread_value": spread_value,
                    "spread_threshold_cents": 8,
                    "reason": spread_result.get("reason"),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "volume": volume_proxy,
                    "side": outcome_side,
                },
                strategy_id=decision.strategy_id,
            )
            if not spread_result["pass"]:
                db.record_event(
                    "execution_skip",
                    {
                        "decision_id": decision.decision_id,
                        "strategy_id": decision.strategy_id,
                        "market_id": action.market_id,
                        "outcome_id": action.outcome_id,
                        "outcome_label": action.outcome_label,
                        "venue": action.venue,
                        "spread_filter": spread_result,
                        "error": f"spread_filter: {spread_result['reason']}",
                    },
                    strategy_id=decision.strategy_id,
                )
                continue

            fee_bps = infer_fee_bps(app_config, db.get_market(action.market_id, action.venue))
            original_amount_usd = action.amount_usd
            market_price = min((float(p) for p, _ in orderbook.asks), default=None) if orderbook.asks else None
            market_price = market_price if market_price is not None else current_market_price
            sizing_cfg = app_config.position_sizing
            kelly_result = compute_position_size(
                predicted_probability=selected_probability if selected_probability is not None else market_price,
                market_ask_price=market_price,
                bankroll=portfolio.cash,
                kelly_fraction=sizing_cfg.get("kelly_fraction"),
                max_position_pct=float(sizing_cfg.get("max_position_pct", 0.02)),
                max_position_usd=float(sizing_cfg.get("max_position_usd", 25.0)),
                fee_rate=fee_bps / 10000.0,
                yes_side_probability=float(decision.predicted_probability) if decision.predicted_probability is not None else None,
            )
            kelly_action = kelly_result.get("action", "no_trade")
            kelly_computed_size = kelly_result.get("amount_usd", 0)
            db.record_event(
                "execution_gate_kelly_sizing",
                {
                    "decision_id": decision.decision_id,
                    "strategy_id": decision.strategy_id,
                    "market_id": action.market_id,
                    "outcome_id": action.outcome_id,
                    "venue": action.venue,
                    "pass": kelly_action == "trade",
                    "kelly_action": kelly_action,
                    "computed_size": round(kelly_computed_size, 2),
                    "min_threshold": float(os.getenv("RISK_MIN_TRADE_SIZE", "5")),
                    "hard_cap": float(os.getenv("RISK_MAX_SINGLE_TRADE_SIZE", "50")),
                    "reason": kelly_result.get("reason"),
                    "selected_probability": selected_probability,
                    "market_ask_price": market_price,
                    "kelly_full": kelly_result.get("kelly_full"),
                    "half_kelly_amount_usd": kelly_result.get("half_kelly_amount_usd"),
                    "capped_amount_usd": kelly_result.get("capped_amount_usd"),
                    "low_confidence_reduction": kelly_result.get("low_confidence_reduction_applied"),
                },
                strategy_id=decision.strategy_id,
            )
            if kelly_result["action"] == "no_trade":
                db.record_event(
                    "trade_sizing",
                    {
                        "decision_id": decision.decision_id,
                        "strategy_id": decision.strategy_id,
                        "market_id": action.market_id,
                        "venue": action.venue,
                        "outcome_id": action.outcome_id,
                        "original_amount_usd": round(original_amount_usd, 2),
                        "kelly_result": kelly_result,
                        "final_amount_usd": 0,
                        "action": "blocked",
                    },
                    strategy_id=decision.strategy_id,
                )
                continue

            kelly_amount = kelly_result["amount_usd"]
            hard_cap = float(os.getenv("RISK_MAX_SINGLE_TRADE_SIZE", "50"))
            enforced_amount = min(kelly_amount, hard_cap, original_amount_usd)
            action.amount_usd = enforced_amount
            db.record_event(
                "trade_sizing",
                {
                    "decision_id": decision.decision_id,
                    "strategy_id": decision.strategy_id,
                    "market_id": action.market_id,
                    "venue": action.venue,
                    "outcome_id": action.outcome_id,
                    "original_amount_usd": round(original_amount_usd, 2),
                    "kelly_full": kelly_result.get("kelly_full"),
                    "raw_amount_usd": kelly_result.get("raw_amount_usd"),
                    "half_kelly_amount_usd": kelly_result.get("half_kelly_amount_usd"),
                    "capped_amount_usd": kelly_result.get("capped_amount_usd"),
                    "low_confidence_reduction": kelly_result.get("low_confidence_reduction_applied"),
                    "kelly_final_usd": kelly_amount,
                    "hard_cap_usd": hard_cap,
                    "enforced_amount_usd": round(enforced_amount, 2),
                    "action": "sized",
                },
                strategy_id=decision.strategy_id,
            )

            execution_mode = str(os.getenv("EXECUTION_MODE", "paper_limit") or "paper_limit").strip().lower()
            if execution_mode == "live_limit":
                db.record_event(
                    "execution_mode_fallback",
                    {
                        "decision_id": decision.decision_id,
                        "strategy_id": decision.strategy_id,
                        "market_id": action.market_id,
                        "outcome_id": action.outcome_id,
                        "requested_mode": "live_limit",
                        "effective_mode": "paper_limit",
                        "reason": "live_limit_not_implemented",
                    },
                    strategy_id=decision.strategy_id,
                )
                execution_mode = "paper_limit"

            if execution_mode == "paper_limit" and str(action.venue) == "polymarket":
                placed = await _submit_paper_limit_order(
                    app_config=app_config,
                    db=db,
                    decision=decision,
                    action=action,
                    orderbook=orderbook,
                    selected_probability=selected_probability,
                    limit_order_manager=services.limit_order_manager,
                )
                if placed is None:
                    continue
                db.record_event(
                    "limit_order_submitted",
                    {
                        "decision_id": decision.decision_id,
                        "strategy_id": decision.strategy_id,
                        "market_id": action.market_id,
                        "outcome_id": action.outcome_id,
                        "venue": action.venue,
                        "order_id": placed.order_id,
                        "venue_order_id": placed.venue_order_id,
                        "status": placed.status.value,
                    },
                    strategy_id=decision.strategy_id,
                )
                continue
            if execution_mode == "paper_limit":
                db.record_event(
                    "execution_mode_fallback",
                    {
                        "decision_id": decision.decision_id,
                        "strategy_id": decision.strategy_id,
                        "market_id": action.market_id,
                        "outcome_id": action.outcome_id,
                        "requested_mode": "paper_limit",
                        "effective_mode": "paper",
                        "reason": f"unsupported_venue:{action.venue}",
                    },
                    strategy_id=decision.strategy_id,
                )

            try:
                execution, position = services.paper_executor.execute(
                    decision_id=decision.decision_id,
                    strategy_id=decision.strategy_id,
                    action=action,
                    orderbook=orderbook,
                    portfolio=portfolio,
                    risk_limits=strategy_cfg["risk"],
                    fee_bps=fee_bps,
                )
            except Exception as exc:
                db.record_event(
                    "execution_error",
                    {
                        "decision_id": decision.decision_id,
                        "strategy_id": decision.strategy_id,
                        "market_id": action.market_id,
                        "outcome_id": action.outcome_id,
                        "venue": action.venue,
                        "error": str(exc),
                    },
                    strategy_id=decision.strategy_id,
                )
                continue
            db.save_execution(execution)
            _apply_execution_circuit_breaker(db, execution)
            if position:
                db.upsert_position(position)
                portfolio = apply_execution_to_portfolio(portfolio, position, execution)
                db.save_portfolio(portfolio)
    finally:
        if owns_services:
            await services.close()


def infer_fee_bps(app_config: AppConfig, market_row) -> float:
    if not market_row:
        return float(app_config.fees["default_event_bps"])
    category = market_row["category"]
    mapping = {
        "weather": "default_weather_bps",
        "crypto": "default_crypto_bps",
        "event": "default_event_bps",
        "politics": "default_event_bps",
        "economics": "default_economics_bps",
    }
    return float(app_config.fees[mapping.get(category, "default_event_bps")])


def _position_exit_signal(position, candidate: dict, now: datetime) -> dict | None:
    held_yes = str(position.outcome_label).lower() == "yes"
    outcome = candidate["yes_outcome"] if held_yes else candidate["no_outcome"]
    fair_probability = float(candidate["predicted_yes"]) if held_yes else 1.0 - float(candidate["predicted_yes"])
    exit_bid = float(outcome.get("best_bid") or outcome.get("mid_price") or 0.0)
    if exit_bid <= 0:
        return None

    hold_edge_bps = int(round((fair_probability - exit_bid) * 10000))
    pnl_pct = (exit_bid - float(position.avg_entry_price)) / max(float(position.avg_entry_price), 1e-9)
    end_time = datetime.fromisoformat(str(candidate["market"]["end_time"]))
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)
    hours_remaining = max((end_time - now).total_seconds() / 3600.0, 0.0)

    if hold_edge_bps <= -300:
        return {"reason": "edge_reversal", "exit_bid": exit_bid, "hold_edge_bps": hold_edge_bps, "pnl_pct": pnl_pct, "hours_remaining": hours_remaining}
    if pnl_pct <= -0.15:
        return {"reason": "stop_loss", "exit_bid": exit_bid, "hold_edge_bps": hold_edge_bps, "pnl_pct": pnl_pct, "hours_remaining": hours_remaining}
    if hours_remaining <= 12.0 and abs(hold_edge_bps) <= 300:
        return {"reason": "time_exit", "exit_bid": exit_bid, "hold_edge_bps": hold_edge_bps, "pnl_pct": pnl_pct, "hours_remaining": hours_remaining}
    return None


async def _submit_paper_limit_order(
    app_config: AppConfig,
    db: ArenaDB,
    decision: Decision,
    action: ProposedAction,
    orderbook,
    selected_probability: float | None,
    limit_order_manager: LimitOrderManager,
):
    from arena.engine.order_types import LimitOrder, OrderSide

    if selected_probability is None:
        db.record_event(
            "execution_skip",
            {
                "decision_id": decision.decision_id,
                "strategy_id": decision.strategy_id,
                "market_id": action.market_id,
                "outcome_id": action.outcome_id,
                "venue": action.venue,
                "error": "missing selected_probability for paper_limit execution",
            },
            strategy_id=decision.strategy_id,
        )
        return None

    limit_price = limit_order_manager.compute_limit_price(
        OrderSide.BUY_YES if str(action.outcome_label).lower() == "yes" else OrderSide.BUY_NO,
        orderbook,
        limit_order_manager.config,
        model_probability=selected_probability,
    )
    if limit_price is None:
        db.record_event(
            "execution_skip",
            {
                "decision_id": decision.decision_id,
                "strategy_id": decision.strategy_id,
                "market_id": action.market_id,
                "outcome_id": action.outcome_id,
                "venue": action.venue,
                "error": "no_profitable_maker_price",
            },
            strategy_id=decision.strategy_id,
        )
        return None

    quantity = round(float(action.amount_usd) / float(limit_price), 6)
    order = LimitOrder(
        market_id=str(action.market_id),
        side=OrderSide.BUY_YES if str(action.outcome_label).lower() == "yes" else OrderSide.BUY_NO,
        price=float(limit_price),
        size_dollars=float(action.amount_usd),
        quantity=quantity,
        strategy_id=decision.strategy_id,
        model_probability=float(selected_probability),
        edge_bps=int(decision.expected_edge_bps or 0),
        ttl_seconds=int(app_config.execution.get("limit_orders", {}).get("order_ttl_seconds", 300)),
        metadata={
            "decision_id": decision.decision_id,
            "outcome_id": str(action.outcome_id),
            "token_id": str(action.outcome_id),
            "outcome_label": str(action.outcome_label),
            "venue": str(action.venue),
            "reasoning_summary": str(action.reasoning_summary),
            "market_implied_probability": decision.market_implied_probability,
        },
    )
    return await limit_order_manager.place_limit_order(order)


async def monitor_limit_orders(
    app_config: AppConfig,
    db: ArenaDB,
    execution_services: ExecutionServices,
) -> None:
    updates = await execution_services.limit_order_manager.monitor_orders()
    replacements = await execution_services.limit_order_manager.reprice_stale_orders()
    metrics = _limit_order_metrics(db)
    if updates:
        db.record_event(
            "limit_order_monitor_cycle",
            {
                "updates": [
                    {
                        "order_id": update.order_id,
                        "old_status": update.old_status.value,
                        "new_status": update.new_status.value,
                        "fill_price": update.fill_price,
                        "fill_quantity": update.fill_quantity,
                        "timestamp": update.timestamp.isoformat(),
                    }
                    for update in updates
                ],
                "replacement_count": len(replacements),
                "metrics": metrics,
            },
        )
    elif replacements:
        db.record_event(
            "limit_order_monitor_cycle",
            {
                "updates": [],
                "replacement_count": len(replacements),
                "metrics": metrics,
            },
        )
    if updates or replacements:
        logger.info(
            "limit order monitor: %d updates, %d reprices, fill_rate=%s avg_fill_seconds=%s avg_midpoint_slippage_cents=%s",
            len(updates),
            len(replacements),
            metrics.get("fill_rate"),
            metrics.get("avg_time_to_fill_seconds"),
            metrics.get("avg_midpoint_slippage_cents"),
        )


def _limit_order_metrics(db: ArenaDB, lookback_hours: int = 24) -> dict[str, float | int | None]:
    with db.connect() as conn:
        rows = list(
            conn.execute(
                """
                SELECT status, placed_at, filled_at, fill_price, metadata_json
                FROM limit_orders
                WHERE updated_at >= datetime('now', ?)
                """,
                (f"-{int(lookback_hours)} hours",),
            )
        )
    closed_rows = [row for row in rows if str(row["status"]).lower() in {"filled", "cancelled", "expired", "rejected"}]
    filled_rows = [row for row in rows if str(row["status"]).lower() == "filled"]
    fill_rate = (len(filled_rows) / len(closed_rows)) if closed_rows else None
    fill_durations = []
    midpoint_slippages = []
    for row in filled_rows:
        placed_at = datetime.fromisoformat(str(row["placed_at"]).replace("Z", "+00:00")) if row["placed_at"] else None
        filled_at = datetime.fromisoformat(str(row["filled_at"]).replace("Z", "+00:00")) if row["filled_at"] else None
        if placed_at and filled_at:
            fill_durations.append((filled_at - placed_at).total_seconds())
        metadata = json.loads(row["metadata_json"] or "{}") if row["metadata_json"] else {}
        midpoint_at_fill = metadata.get("midpoint_at_fill")
        fill_price = row["fill_price"]
        if midpoint_at_fill is not None and fill_price is not None:
            midpoint_slippages.append((float(fill_price) - float(midpoint_at_fill)) * 100.0)
    return {
        "closed_orders": len(closed_rows),
        "filled_orders": len(filled_rows),
        "fill_rate": round(fill_rate, 6) if fill_rate is not None else None,
        "avg_time_to_fill_seconds": round(sum(fill_durations) / len(fill_durations), 3) if fill_durations else None,
        "avg_midpoint_slippage_cents": round(sum(midpoint_slippages) / len(midpoint_slippages), 4) if midpoint_slippages else None,
    }


async def manage_open_positions(app_config: AppConfig, db: ArenaDB) -> None:
    now = utc_now()
    closed_count = 0
    forecast_cfg = app_config.strategies.get("algo_forecast")
    if forecast_cfg is None:
        return
    strategy = ForecastConsensusStrategy(db=db, strategy_config=forecast_cfg.strategy)
    weather_markets = {
        (str(row["market_id"]), str(row["venue"])): row
        for row in db.list_markets(category="weather", status="active")
    }

    for position in db.list_open_positions():
        key = (str(position.market_id), str(position.venue))
        market_row = weather_markets.get(key)
        if market_row is None:
            continue
        candidate = await strategy._evaluate_market(market_row, now)
        if candidate is None:
            continue
        exit_signal = _position_exit_signal(position, candidate, now)
        if exit_signal is None:
            continue

        exit_price = float(exit_signal["exit_bid"])
        payout = position.quantity * exit_price
        fee_bps = infer_fee_bps(app_config, market_row)
        fees = payout * (fee_bps / 10000.0)
        net_payout = max(payout - fees, 0.0)

        portfolio = db.get_portfolio(position.strategy_id)
        if portfolio is None:
            continue

        execution = ExecutionResult(
            execution_id=new_id("exec"),
            decision_id=f"exit_{position.position_id}",
            strategy_id=position.strategy_id,
            timestamp=now,
            action_type="SELL",
            market_id=position.market_id,
            venue=position.venue,
            outcome_id=position.outcome_id,
            status="filled",
            requested_amount_usd=payout,
            filled_quantity=position.quantity,
            avg_fill_price=exit_price,
            slippage_applied=0.0,
            fees_applied=fees,
            total_cost=net_payout,
            rejection_reason=None,
            orderbook_snapshot_id=new_id("book"),
        )
        closed_position = replace(
            position,
            status="closed",
            current_price=exit_price,
            unrealized_pnl=0.0,
            last_updated_at=now,
        )
        db.save_execution(execution)
        db.upsert_position(closed_position)
        updated_portfolio = close_position(portfolio, position.position_id, net_payout)
        db.save_portfolio(updated_portfolio)
        db.record_event(
            "position_exit",
            {
                "strategy_id": position.strategy_id,
                "market_id": position.market_id,
                "venue": position.venue,
                "outcome_id": position.outcome_id,
                "reason": exit_signal["reason"],
                "exit_bid": exit_price,
                "hold_edge_bps": exit_signal["hold_edge_bps"],
                "pnl_pct": round(exit_signal["pnl_pct"], 6),
                "hours_remaining": round(exit_signal["hours_remaining"], 3),
            },
            strategy_id=position.strategy_id,
        )
        closed_count += 1

    if closed_count:
        logger.info("position manager closed %d open positions", closed_count)


async def mark_to_market(app_config: AppConfig, db: ArenaDB) -> None:
    adapters = build_market_adapters(app_config)
    touched: set[str] = set()
    for position in db.list_open_positions():
        adapter = adapters[position.venue]
        orderbook = await adapter.get_orderbook(position.market_id, position.outcome_id)
        db.save_orderbook_snapshot(orderbook)
        position.current_price = orderbook.mid
        position.unrealized_pnl = compute_position_unrealized(position, orderbook.mid)
        position.last_updated_at = utc_now()
        db.upsert_position(position)
        touched.add(position.strategy_id)
    for strategy_id in touched:
        portfolio = db.get_portfolio(strategy_id)
        if not portfolio:
            continue
        open_positions = [pos for pos in db.list_open_positions(strategy_id)]
        portfolio.positions = open_positions
        portfolio.unrealized_pnl = sum(pos.unrealized_pnl for pos in open_positions)
        portfolio.total_value = portfolio.cash + sum(pos.quantity * pos.current_price for pos in open_positions)
        portfolio.peak_value = max(portfolio.peak_value, portfolio.total_value)
        if portfolio.peak_value:
            current_drawdown = max((portfolio.peak_value - portfolio.total_value) / portfolio.peak_value, 0.0)
            portfolio.max_drawdown = max(portfolio.max_drawdown, current_drawdown)
        portfolio.updated_at = utc_now()
        db.save_portfolio(portfolio)


async def poll_resolutions(app_config: AppConfig, db: ArenaDB) -> None:
    from arena.adapters.weather_openmeteo import CITY_COORDS
    from arena.data_sources.station_observations import get_daily_observed_temperatures

    settlement = SettlementEngine(db)
    adapters = build_market_adapters(app_config)

    # Helper to parse weather contracts (reuse algo_forecast parser)
    contract_parser = ForecastConsensusStrategy(
        db=db,
        strategy_config={
            "id": "resolution_helper",
            "starting_balance": 1000.0,
            "scope": {},
            "risk": {"max_position_pct": 0.02, "max_positions": 10, "max_daily_loss_pct": 0.20, "min_edge_bps": 200},
        },
    )

    # Query both active and resolved markets whose end_time has passed
    now = utc_now()
    with db.connect() as conn:
        pending_rows = list(conn.execute(
            "SELECT * FROM markets WHERE status IN ('active', 'resolved') AND end_time < ? ORDER BY end_time ASC",
            (now.isoformat(),),
        ))

    # Exclude markets that already have a resolution record
    with db.connect() as conn:
        already_resolved = {
            (row["market_id"], row["venue"])
            for row in conn.execute("SELECT market_id, venue FROM resolutions")
        }

    settled_count = 0
    skipped_count = 0
    error_count = 0

    for row in pending_rows:
        market_id = row["market_id"]
        venue = row["venue"]
        if (market_id, venue) in already_resolved:
            continue

        try:
            # --- Weather-specific local resolution ---
            if row["category"] == "weather":
                contract = contract_parser._parse_weather_contract(row["question"])
                if contract and contract.get("dated", True):
                    city = contract["city"]
                    coords = CITY_COORDS.get(city.lower())
                    if coords:
                        threshold_c = contract_parser._contract_threshold_c(contract)
                        if threshold_c is not None:
                            try:
                                actual_high_c, actual_low_c = await get_daily_observed_temperatures(
                                    db, coords[0], coords[1], city, contract["forecast_date"]
                                )
                            except Exception as exc:
                                logger.warning("Weather observation fetch failed for %s: %s", market_id, exc)
                                actual_high_c = None

                            if actual_high_c is not None:
                                # Determine winning outcome based on contract shape
                                shape = contract.get("shape", "at_or_above")
                                if shape == "at_or_above":
                                    yes_wins = actual_high_c >= threshold_c
                                elif shape == "at_or_below":
                                    yes_wins = actual_high_c <= threshold_c
                                elif shape == "between":
                                    lower = contract.get("lower", 0)
                                    upper = contract.get("upper", 999)
                                    if contract["unit"] == "f":
                                        lower_c = (lower - 32) * 5 / 9
                                        upper_c = (upper - 32) * 5 / 9
                                    else:
                                        lower_c, upper_c = lower, upper
                                    yes_wins = lower_c <= actual_high_c <= upper_c
                                elif shape == "exact":
                                    yes_wins = round(actual_high_c) == round(threshold_c)
                                else:
                                    yes_wins = actual_high_c >= threshold_c

                                outcomes = json.loads(row["outcomes_json"]) if isinstance(row["outcomes_json"], str) else row["outcomes_json"]
                                yes_outcome, no_outcome = contract_parser._binary_outcomes(outcomes)
                                if yes_outcome and no_outcome:
                                    winning = yes_outcome if yes_wins else no_outcome
                                    winning_id = str(winning.get("outcome_id", winning.get("id", "")))
                                    winning_label = "Yes" if yes_wins else "No"

                                    logger.info(
                                        "Weather resolution: %s -> %s (actual_high=%.1fC, threshold=%.1fC, shape=%s)",
                                        row["question"][:80], winning_label, actual_high_c, threshold_c, shape,
                                    )
                                    settlement.settle_market(
                                        market_id=market_id,
                                        venue=venue,
                                        winning_outcome_id=winning_id,
                                        winning_outcome_label=winning_label,
                                        resolution_source_url="local:station_observations",
                                        resolution_data={
                                            "actual_high_c": actual_high_c,
                                            "actual_low_c": actual_low_c,
                                            "threshold_c": threshold_c,
                                            "shape": shape,
                                            "city": city,
                                        },
                                    )
                                    # Mark market as resolved in DB
                                    with db.connect() as conn:
                                        conn.execute(
                                            "UPDATE markets SET status = 'resolved', resolved_outcome_id = ? WHERE market_id = ? AND venue = ?",
                                            (winning_id, market_id, venue),
                                        )
                                    settled_count += 1
                                    continue
                            else:
                                skipped_count += 1
                                continue
                    else:
                        logger.debug("No coords for city %s in market %s", city if contract else "?", market_id)
                        skipped_count += 1
                        continue

            # --- Non-weather: use Polymarket/Kalshi API resolution ---
            adapter = adapters.get(venue)
            if not adapter:
                continue
            try:
                market = await adapter.get_resolution_status(row["market_id"])
            except Exception as exc:
                db.record_event("resolution_poll_error", {"market_id": market_id, "venue": venue, "error": str(exc)})
                skipped_count += 1
                continue

            if market.status == "resolved" and market.resolved_outcome_id:
                winner = next((outcome for outcome in market.outcomes if outcome.outcome_id == market.resolved_outcome_id), None)
                settlement.settle_market(
                    market_id=market.market_id,
                    venue=market.venue,
                    winning_outcome_id=market.resolved_outcome_id,
                    winning_outcome_label=winner.label if winner else market.resolved_outcome_id,
                    resolution_source_url=market.resolution_source,
                )
                db.upsert_market(market)
                settled_count += 1
            else:
                # Log warning if end_time passed by 24h+ with no resolution
                end_time = datetime.fromisoformat(row["end_time"])
                hours_overdue = (now - end_time).total_seconds() / 3600
                if hours_overdue > 24:
                    logger.warning(
                        "Market %s (%s) is %.0fh overdue with no resolution from API",
                        market_id, row["question"][:60], hours_overdue,
                    )
                skipped_count += 1
        except Exception as exc:
            logger.error("Settlement failed for market %s/%s: %s", market_id, venue, exc, exc_info=True)
            db.record_event("settlement_error", {"market_id": market_id, "venue": venue, "error": str(exc)})
            error_count += 1
            continue

    if settled_count or error_count:
        logger.info(
            "poll_resolutions complete: %d settled, %d skipped, %d errors (out of %d pending)",
            settled_count, skipped_count, error_count, len(pending_rows),
        )


async def monitor_intraday_weather(app_config: AppConfig, db: ArenaDB) -> None:
    """Lightweight check for same-day weather markets.

    Fetches current observations and logs material threshold events.
    This is a monitoring layer — it does NOT auto-trade, but logs
    when conditions suggest a strategy re-run would be valuable.
    """
    from arena.adapters.weather_openmeteo import CITY_COORDS
    from arena.data_sources.station_observations import ObservationUnavailable, get_current_observations
    from arena.strategies.algo_forecast import ForecastConsensusStrategy

    helper = ForecastConsensusStrategy(
        db=db,
        strategy_config={
            "id": "intraday_monitor",
            "starting_balance": 1000.0,
            "scope": {},
            "risk": {"max_position_pct": 0.02, "max_positions": 10, "max_daily_loss_pct": 0.20, "min_edge_bps": 200},
        },
    )
    now = datetime.now(timezone.utc)
    today = now.date()
    weather_markets = db.list_markets(category="weather", status="active")
    triggered_strategies: set[str] = set()

    for row in weather_markets:
        contract = helper._parse_weather_contract(row["question"])
        if not contract or not contract.get("dated", True):
            continue
        if contract["forecast_date"] != today:
            continue

        city = contract["city"]
        coords = CITY_COORDS.get(city.lower())
        if not coords:
            continue

        threshold_c = helper._contract_threshold_c(contract)
        if threshold_c is None:
            continue

        try:
            obs = await get_current_observations(coords[0], coords[1], city)
        except ObservationUnavailable:
            continue

        market_id = row["market_id"]

        if obs["max_temp_so_far_c"] >= threshold_c:
            logger.warning(
                f"THRESHOLD HIT for market {market_id}: "
                f"{city} max so far {obs['max_temp_so_far_c']}C >= {threshold_c}C"
            )
            triggered_strategies.add("algo_forecast")
            db.record_event("intraday_threshold_hit", {
                "market_id": market_id,
                "city": city,
                "threshold_c": threshold_c,
                "max_temp_so_far_c": obs["max_temp_so_far_c"],
                "current_temp_c": obs["current_temp_c"],
            })

        elif (
            obs["trending"] == "cooling"
            and obs["current_temp_c"] < threshold_c - 1.0
            and obs["hours_remaining"] < 3
        ):
            logger.warning(
                f"LIKELY MISS for market {market_id}: "
                f"{city} cooling at {obs['current_temp_c']}C, threshold {threshold_c}C, "
                f"{obs['hours_remaining']}h remaining"
            )
            triggered_strategies.add("algo_forecast")
            db.record_event("intraday_likely_miss", {
                "market_id": market_id,
                "city": city,
                "threshold_c": threshold_c,
                "current_temp_c": obs["current_temp_c"],
                "trending": obs["trending"],
                "hours_remaining": obs["hours_remaining"],
            })

    if triggered_strategies:
        logger.info(
            f"Intraday monitor suggests re-running strategies: {triggered_strategies}"
        )


async def poll_fourcastnet_cache(app_config: AppConfig, db: ArenaDB) -> None:
    from arena.adapters.weather_openmeteo import CITY_COORDS
    from arena.data_sources.nvidia_fourcastnet import fetch_fourcastnet_forecast

    helper = ForecastConsensusStrategy(
        db=db,
        strategy_config={
            "id": "fourcastnet_poll",
            "starting_balance": 1000.0,
            "scope": {},
            "risk": {"max_position_pct": 0.02, "max_positions": 10, "max_daily_loss_pct": 0.20, "min_edge_bps": 200},
        },
    )

    targets: dict[tuple[str, str], tuple[str, float, float, str]] = {}
    for row in db.list_markets(category="weather", status="active"):
        contract = helper._parse_weather_contract(row["question"])
        if not contract or not contract.get("dated", True):
            continue
        city = contract["city"]
        coords = CITY_COORDS.get(city.lower())
        if not coords:
            continue
        target_date = contract["forecast_date"].isoformat()
        targets[(city.lower(), target_date)] = (city, coords[0], coords[1], target_date)

    for city, latitude, longitude, target_date in targets.values():
        value_f = await asyncio.to_thread(fetch_fourcastnet_forecast, latitude, longitude, target_date)
        if value_f is None:
            continue
        logger.info(
            "FourCastNet poll: %s (%.2f,%.2f) -> %.1f°F for %s",
            city,
            latitude,
            longitude,
            value_f,
            target_date,
        )


def capture_daily_snapshots(app_config: AppConfig, db: ArenaDB) -> None:
    today_str = date.today().isoformat()
    for strategy_id in app_config.strategies:
        portfolio = db.get_portfolio(strategy_id)
        if not portfolio:
            continue
        # Query today's executions instead of using cumulative portfolio counters
        with db.connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM executions "
                "WHERE strategy_id = ? AND status IN ('filled', 'partial') "
                "AND timestamp >= ?",
                (strategy_id, today_str),
            ).fetchone()
            trades_today = row["cnt"] if row else 0
        snapshot = DailySnapshot(
            snapshot_date=date.today(),
            strategy_id=strategy_id,
            portfolio_value=portfolio.total_value,
            cash=portfolio.cash,
            positions_count=len([pos for pos in portfolio.positions if pos.status == "open"]),
            realized_pnl_cumulative=portfolio.realized_pnl,
            unrealized_pnl=portfolio.unrealized_pnl,
            trades_today=trades_today,
            wins_today=0,
            losses_today=0,
            api_cost_today=0.0,
        )
        db.save_daily_snapshot(snapshot)


def run_weekly_retrospective(db: ArenaDB) -> Path:
    from arena.analytics.retrospective import generate_recommendations, write_weekly_report

    with db.connect() as conn:
        decisions = [dict(row) for row in conn.execute("SELECT * FROM decisions")]
        portfolios = [dict(row) for row in conn.execute("SELECT * FROM portfolios")]
    report = generate_recommendations(decisions, portfolios)
    path = write_weekly_report(report)
    return path


def run_monthly_meta_prompt(db: ArenaDB) -> Path:
    from arena.analytics.meta_analysis import generate_monthly_prompt

    with db.connect() as conn:
        payload = {
            "decisions": [dict(row) for row in conn.execute("SELECT * FROM decisions ORDER BY timestamp DESC LIMIT 500")],
            "executions": [dict(row) for row in conn.execute("SELECT * FROM executions ORDER BY timestamp DESC LIMIT 500")],
            "portfolios": [dict(row) for row in conn.execute("SELECT * FROM portfolios")],
        }
    return generate_monthly_prompt(payload)


def manual_input(app_config: AppConfig, strategy_name: str, content: str) -> Path:
    path = ROOT / app_config.manual["responses_dir"] / f"{strategy_name}_{date.today().isoformat()}.json"
    path.write_text(content, encoding="utf-8")
    if strategy_name != "manual":
        alias = ROOT / app_config.manual["responses_dir"] / f"manual_{date.today().isoformat()}.json"
        alias.write_text(content, encoding="utf-8")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="arena")
    sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("init")
    init.add_argument("--reset", action="store_true")
    sub.add_parser("status")
    sub.add_parser("init-portfolios")
    scan = sub.add_parser("scan")
    markets = sub.add_parser("markets")
    markets.add_argument("--category", default=None)
    sub.add_parser("recategorize-markets")
    run_once = sub.add_parser("run-once")
    run_once.add_argument("strategy_id")
    sub.add_parser("scheduler")
    sub.add_parser("export-dashboard")
    show_decision = sub.add_parser("show-decision")
    show_decision.add_argument("decision_id")
    last_decisions = sub.add_parser("last-decisions")
    last_decisions.add_argument("--strategy", default=None)
    last_decisions.add_argument("--limit", type=int, default=5)
    show_packet = sub.add_parser("show-info-packet")
    show_packet.add_argument("strategy_id")
    manual = sub.add_parser("manual-input")
    manual.add_argument("strategy_name")
    manual.add_argument("--file", default=None)
    sub.add_parser("daily-snapshot")
    sub.add_parser("weekly-retro")
    sub.add_parser("monthly-meta")
    return parser


def main(argv: list[str] | None = None) -> None:
    load_local_env()
    _provider_keys = {
        "MINIMAX_API_KEY": "MiniMax Direct API (M2.7 Token Plan)",
        "GEMINI_API_KEY": "Google AI Studio (Gemini 3 Flash)",
        "NVIDIA_API_KEY": "NVIDIA NIM (Nemotron-3 Super)",
        "OPENROUTER_API_KEY": "OpenRouter (legacy fallback)",
        "NEXUS_API_KEY": "Perplexia / Nexus Deep Research",
    }
    for key, desc in _provider_keys.items():
        if os.environ.get(key):
            logger.info(f"✓ {desc} — key found")
        else:
            if key == "NEXUS_API_KEY":
                logger.warning(f"✗ {desc} — {key} not set; this is fine if your local Nexus API is open")
            else:
                logger.warning(f"✗ {desc} — {key} not set, this provider will be unavailable")
    args = build_parser().parse_args(argv)
    app_config = load_app_config()

    # Execution mode validation
    exec_mode = app_config.execution.get("mode", "paper")
    if exec_mode == "live":
        env_gate = os.environ.get("ARENA_LIVE_TRADING")
        if env_gate == "YES_I_UNDERSTAND_THIS_IS_REAL_MONEY":
            logger.warning("EXECUTION MODE: LIVE — real orders will be placed on Polymarket CLOB")
        else:
            logger.info("Execution mode set to 'live' in config but ARENA_LIVE_TRADING env var not set — falling back to paper mode")
            exec_mode = "paper"
    else:
        logger.info(f"Execution mode: {exec_mode}")

    db = get_db(app_config)

    if args.command == "init":
        if args.reset and app_config.db_path.exists():
            app_config.db_path.unlink()
        db.initialize()
        ensure_portfolios(app_config, db)
        print_status(app_config, db)
        return
    if args.command == "status":
        db.initialize()
        print_status(app_config, db)
        return
    if args.command == "init-portfolios":
        db.initialize()
        init_portfolios(app_config, db)
        return
    if args.command == "scan":
        db.initialize()
        asyncio.run(scan_markets(app_config, db))
        print_status(app_config, db)
        return
    if args.command == "markets":
        db.initialize()
        print_markets(db, args.category)
        return
    if args.command == "recategorize-markets":
        db.initialize()
        recategorize_markets(db)
        return
    if args.command == "run-once":
        db.initialize()
        execution_services = build_execution_services(app_config, db)
        try:
            decision = asyncio.run(run_strategy_once(app_config, db, args.strategy_id, execution_services=execution_services))
        finally:
            asyncio.run(execution_services.close())
        print(json.dumps({"decision_id": decision.decision_id, "strategy_id": decision.strategy_id, "actions": len(decision.actions)}, indent=2))
        return
    if args.command == "show-decision":
        db.initialize()
        show_decision(db, args.decision_id)
        return
    if args.command == "last-decisions":
        db.initialize()
        list_decisions(db, args.strategy, args.limit)
        return
    if args.command == "show-info-packet":
        db.initialize()
        asyncio.run(show_info_packet(app_config, db, args.strategy_id))
        return
    if args.command == "daily-snapshot":
        db.initialize()
        capture_daily_snapshots(app_config, db)
        print("Daily snapshots captured.")
        return
    if args.command == "weekly-retro":
        db.initialize()
        path = run_weekly_retrospective(db)
        print(path)
        return
    if args.command == "monthly-meta":
        db.initialize()
        path = run_monthly_meta_prompt(db)
        print(path)
        return
    if args.command == "manual-input":
        db.initialize()
        if args.file:
            content = Path(args.file).read_text(encoding="utf-8")
        else:
            content = sys.stdin.read()
        path = manual_input(app_config, args.strategy_name, content)
        print(path)
        return
    if args.command == "export-dashboard":
        db.initialize()
        export_dashboard(app_config, db)
        print("Dashboard export complete.")
        return
    if args.command == "scheduler":
        from arena.scheduler import build_scheduler

        db.initialize()
        ensure_portfolios(app_config, db)
        execution_services = build_execution_services(app_config, db)
        scheduler = build_scheduler(
            app_config,
            jobs={
                "scan_markets": lambda: asyncio.run(scan_markets(app_config, db)),
                "poll_resolutions": lambda: asyncio.run(poll_resolutions(app_config, db)),
                "mark_to_market": lambda: asyncio.run(mark_to_market(app_config, db)),
                "monitor_limit_orders": lambda: asyncio.run(monitor_limit_orders(app_config, db, execution_services)),
                "manage_open_positions": lambda: asyncio.run(manage_open_positions(app_config, db)),
                "run_discovery_scout": lambda: asyncio.run(run_discovery_scout(app_config, db)),
                "export_dashboard": lambda: export_dashboard(app_config, db),
                "check_manual_responses": lambda: None,
                "run_strategy": lambda strategy_id: asyncio.run(
                    run_strategy_once(app_config, db, strategy_id, execution_services=execution_services)
                ),
                "monitor_intraday": lambda: asyncio.run(monitor_intraday_weather(app_config, db)),
                "poll_fourcastnet": lambda: asyncio.run(poll_fourcastnet_cache(app_config, db)),
                "capture_daily_snapshots": lambda: capture_daily_snapshots(app_config, db),
                "run_weekly_retrospective": lambda: run_weekly_retrospective(db),
                "run_monthly_meta_prompt": lambda: run_monthly_meta_prompt(db),
            },
        )
        shutdown_signal: dict[str, str | None] = {"name": None}

        def _handle_shutdown(signum, _frame) -> None:
            signame = signal.Signals(signum).name
            if shutdown_signal["name"] is not None:
                return
            shutdown_signal["name"] = signame
            db.record_event("graceful_shutdown_requested", {"signal": signame})
            logger.warning("Received %s, shutting down scheduler gracefully", signame)
            try:
                scheduler.shutdown(wait=False)
            except Exception:
                logger.exception("Scheduler shutdown request failed")

        signal.signal(signal.SIGTERM, _handle_shutdown)
        signal.signal(signal.SIGINT, _handle_shutdown)
        try:
            scheduler.start()
        finally:
            if shutdown_signal["name"] is not None:
                db.record_event("graceful_shutdown", {"signal": shutdown_signal["name"]})
            try:
                asyncio.run(execution_services.limit_order_manager.cancel_all())
            except Exception:
                logger.exception("Failed to cancel open limit orders during shutdown")
            asyncio.run(execution_services.close())


def _json_or_raw(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
    return value


def show_decision(db: ArenaDB, decision_id: str) -> None:
    row = db.get_decision(decision_id)
    if not row:
        print(f"Decision not found: {decision_id}")
        return
    payload = {key: _json_or_raw(row[key]) for key in row.keys()}
    print(json.dumps(payload, indent=2, default=str))


def list_decisions(db: ArenaDB, strategy_id: str | None, limit: int) -> None:
    rows = [
        {
            "decision_id": row["decision_id"],
            "strategy_id": row["strategy_id"],
            "timestamp": row["timestamp"],
            "expected_edge_bps": row["expected_edge_bps"],
            "confidence": row["confidence"],
            "actions": len(_json_or_raw(row["actions_json"]) or []),
            "no_action_reason": row["no_action_reason"],
        }
        for row in db.list_recent_decisions(strategy_id, limit=limit)
    ]
    print(render_table(rows))


async def show_info_packet(app_config: AppConfig, db: ArenaDB, strategy_id: str) -> None:
    from arena.adapters.weather_hko import HKOWeatherSource
    from arena.adapters.weather_openmeteo import OpenMeteoSource
    from arena.intelligence.info_packet import InfoPacketBuilder

    strategy_cfg = app_config.strategies[strategy_id].strategy
    builder = InfoPacketBuilder(
        db,
        search_client=build_search_client(app_config, strategy_cfg),
        weather_sources=[HKOWeatherSource(), OpenMeteoSource()],
    )
    packet = await builder.build(strategy_cfg, strategy_id)
    print(json.dumps(packet, indent=2, default=str))


def export_dashboard(app_config: AppConfig, db: ArenaDB) -> None:
    from arena.adapters.sheets import GoogleSheetsSink

    payloads = build_dashboard_payloads(db)
    sink = GoogleSheetsSink(
        spreadsheet_id=app_config.models["providers"]["google_sheets"].get("spreadsheet_id", "")
    )
    asyncio.run(sink.export_leaderboard(payloads["leaderboard"]))
    asyncio.run(sink.export_trade_log(payloads["trade_feed"]))
    asyncio.run(sink.export_reasoning_log(payloads["reasoning"]))
    asyncio.run(sink.export_calibration(payloads["calibration"]))
    asyncio.run(sink.export_costs(payloads["costs"]))


if __name__ == "__main__":
    main()
