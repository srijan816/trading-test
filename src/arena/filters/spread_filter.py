from __future__ import annotations

import logging
import os
from typing import Dict

logger = logging.getLogger("arena.spread_filter")


class SpreadFilter:
    """
    Rejects trades where the bid-ask spread exceeds our estimated edge.

    For a binary contract with model value v:
    - Estimated edge = v - market_mid_price
    - Spread cost = (ask - bid) / 2
    - Net edge at execution = v - ask
    - We only trade if net_edge > MIN_NET_EDGE

    In this codebase the orderbook is fetched for the specific outcome being
    traded, so `our_probability` should always be the fair value of that
    selected outcome contract.
    """

    MIN_NET_EDGE_CENTS = 2.0
    MAX_SPREAD_CENTS = 8.0
    MIN_VOLUME = 50

    @classmethod
    def _min_net_edge_cents(cls) -> float:
        return float(os.getenv("SPREAD_FILTER_MIN_NET_EDGE_CENTS", cls.MIN_NET_EDGE_CENTS))

    @classmethod
    def _max_spread_cents(cls) -> float:
        return float(os.getenv("SPREAD_FILTER_MAX_SPREAD_CENTS", cls.MAX_SPREAD_CENTS))

    @classmethod
    def _min_volume(cls) -> int:
        return int(os.getenv("SPREAD_FILTER_MIN_VOLUME", str(cls.MIN_VOLUME)))

    @classmethod
    def check(
        cls,
        our_probability: float,
        best_bid: float,
        best_ask: float,
        volume: int | None = None,
        side: str = "yes",
    ) -> Dict:
        """
        Args:
            our_probability: fair value for the selected contract on a 0-1 scale
            best_bid: best bid price on a 0-1 scale
            best_ask: best ask price on a 0-1 scale
            volume: optional market volume proxy
            side: "yes" or "no" label for logging
        """
        result: Dict[str, object] = {
            "side": side.lower(),
            "volume": volume,
        }

        if best_bid is None or best_ask is None or best_bid < 0 or best_ask < 0:
            result["pass"] = False
            result["reason"] = "missing bid/ask quotes"
            logger.info("SPREAD FILTER REJECT: %s", result["reason"])
            return result

        if best_ask < best_bid:
            result["pass"] = False
            result["reason"] = f"crossed market ask {best_ask:.4f} < bid {best_bid:.4f}"
            logger.info("SPREAD FILTER REJECT: %s", result["reason"])
            return result

        spread = best_ask - best_bid
        spread_cents = round(spread * 100, 1)
        half_spread = spread / 2
        mid_price = (best_bid + best_ask) / 2
        estimated_edge = our_probability - mid_price
        execution_price = best_ask
        edge_after_execution = our_probability - best_ask
        edge_after_cents = round(edge_after_execution * 100, 1)

        result.update(
            {
                "spread_cents": spread_cents,
                "half_spread_cents": round(half_spread * 100, 1),
                "mid_price": round(mid_price, 4),
                "estimated_edge_cents": round(estimated_edge * 100, 1),
                "edge_after_execution_cents": edge_after_cents,
                "execution_price": round(execution_price, 4),
                "best_bid": round(best_bid, 4),
                "best_ask": round(best_ask, 4),
                "our_probability": round(our_probability, 4),
            }
        )

        max_spread_cents = cls._max_spread_cents()
        min_net_edge_cents = cls._min_net_edge_cents()
        min_volume = cls._min_volume()

        if spread_cents > max_spread_cents:
            result["pass"] = False
            result["reason"] = f"spread {spread_cents}c > max {max_spread_cents:g}c"
            logger.info("SPREAD FILTER REJECT: %s", result["reason"])
            return result

        if edge_after_cents < min_net_edge_cents:
            result["pass"] = False
            result["reason"] = f"net edge {edge_after_cents}c < min {min_net_edge_cents:g}c after spread"
            logger.info("SPREAD FILTER REJECT: %s", result["reason"])
            return result

        if volume is not None and volume < min_volume:
            result["pass"] = False
            result["reason"] = f"volume {volume} < min {min_volume}"
            logger.info("SPREAD FILTER REJECT: %s", result["reason"])
            return result

        result["pass"] = True
        result["reason"] = "OK"
        logger.info(
            "SPREAD FILTER PASS: edge=%sc, spread=%sc, vol=%s",
            edge_after_cents,
            spread_cents,
            volume,
        )
        return result
