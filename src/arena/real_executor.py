from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    pass


class RealExecutor:
    """Stub real executor for Polymarket CLOB API.

    This module is behind TWO safety gates:
    1. config/arena.toml must have: [execution] mode = "live"
    2. Environment variable ARENA_LIVE_TRADING must equal
       "YES_I_UNDERSTAND_THIS_IS_REAL_MONEY"

    If either is missing, fall back to paper executor.
    """

    def __init__(self, config: dict) -> None:
        self.api_key = os.environ.get("POLYMARKET_API_KEY")
        self.api_secret = os.environ.get("POLYMARKET_SECRET")
        self.base_url = "https://clob.polymarket.com"
        self.dry_run = config.get("dry_run", True)

        if not self.api_key or not self.api_secret:
            raise ConfigError("Polymarket credentials not set — cannot use live mode")

    async def execute_trade(
        self,
        decision: dict,
        position_size: dict,
        risk_check: dict,
        orderbook: dict,
    ) -> dict:
        if not risk_check.get("approved"):
            return {"status": "rejected", "reason": risk_check.get("reason", "Risk check failed")}

        market_id = decision.get("market_id", "unknown")
        side = decision.get("side", "BUY")
        amount = position_size.get("amount_usd", 0)

        if self.dry_run:
            logger.info(
                f"DRY RUN: Would place {side} order for ${amount:.2f} on {market_id}"
            )
            return {
                "status": "dry_run",
                "market_id": market_id,
                "side": side,
                "amount_usd": amount,
                "message": "Dry run — no real order placed",
            }

        # REAL ORDER PLACEMENT:
        # POST to Polymarket CLOB API
        # Log EVERYTHING: request payload, response, timestamps
        # Store the order ID in the executions table
        # Wait for fill confirmation (poll or websocket)
        # If no fill within 60 seconds, cancel the order
        #
        # NOT YET IMPLEMENTED — falling back to stub response
        logger.warning(
            "Real execution not yet fully implemented — "
            "returning stub response instead of placing real order"
        )
        return {
            "status": "not_implemented",
            "market_id": market_id,
            "side": side,
            "amount_usd": amount,
            "message": "Real CLOB API integration pending — no order placed",
        }


def get_executor(config: dict):
    """Return the appropriate executor based on configuration.

    Returns RealExecutor if live mode is fully enabled, otherwise None
    (caller should use PaperExecutor).
    """
    execution_config = config.get("execution", {})
    mode = execution_config.get("mode", "paper")

    if mode != "live":
        return None

    env_gate = os.environ.get("ARENA_LIVE_TRADING")
    if env_gate != "YES_I_UNDERSTAND_THIS_IS_REAL_MONEY":
        logger.error(
            "LIVE MODE REQUESTED but ARENA_LIVE_TRADING env var not set correctly. "
            "Falling back to paper mode."
        )
        return None

    try:
        return RealExecutor(execution_config)
    except ConfigError as e:
        logger.error(f"Cannot initialize real executor: {e}. Falling back to paper mode.")
        return None
