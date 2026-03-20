from __future__ import annotations

"""
Kalshi exchange adapter using PyKalshi.
Mirrors the interface of the existing Polymarket adapter.
"""

import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger("arena.kalshi")

try:
    from pykalshi import (
        Action,
        Feed,
        KalshiAPIError,
        KalshiClient,
        MarketStatus,
        OrderStatus,
        OrderbookManager,
        OrderbookSnapshotMessage,
        RateLimitError,
        Side,
        TickerMessage,
        InsufficientFundsError,
    )

    PYKALSHI_AVAILABLE = True
except ImportError:
    PYKALSHI_AVAILABLE = False
    KalshiClient = None
    MarketStatus = None
    Action = None
    Side = None
    OrderStatus = None
    Feed = None
    OrderbookManager = None
    TickerMessage = None
    OrderbookSnapshotMessage = None
    InsufficientFundsError = None
    RateLimitError = None
    KalshiAPIError = Exception
    logger.warning("pykalshi not installed. Kalshi adapter disabled.")


class KalshiAdapter:
    CITY_CODES = {
        "new york": "NY",
        "chicago": "CHI",
        "los angeles": "LA",
        "miami": "MIA",
        "denver": "DEN",
        "houston": "HOU",
        "phoenix": "PHX",
        "seattle": "SEA",
        "boston": "BOS",
        "atlanta": "ATL",
        "dallas": "DAL",
        "minneapolis": "MIN",
    }

    def __init__(self) -> None:
        self.enabled = os.getenv("KALSHI_ENABLED", "false").lower() == "true"
        self.trade_enabled = os.getenv("KALSHI_TRADE_ENABLED", "false").lower() == "true"
        self.client = None
        if self.enabled and PYKALSHI_AVAILABLE:
            api_key_id = os.getenv("KALSHI_API_KEY_ID")
            private_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH")
            if not api_key_id or not private_key_path:
                logger.warning("Kalshi enabled but credentials are incomplete. Adapter disabled.")
                self.enabled = False
                return
            try:
                self.client = KalshiClient(
                    api_key_id=api_key_id,
                    private_key_path=private_key_path,
                )
                balance = self.client.portfolio.get_balance()
                logger.info("Kalshi connected. Balance: $%.2f", float(balance.balance))
            except Exception as exc:
                logger.error("Kalshi connection failed: %s", exc)
                self.enabled = False

    def find_weather_markets(self, city: str, target_date: str) -> List[Dict]:
        """
        Find active Kalshi weather/temperature markets for a city and date.
        Searches broadly by Kalshi city series and then filters to the target date.
        """
        if not self.enabled or not self.client:
            return []

        code = self.CITY_CODES.get(city.lower())
        if not code:
            return []

        try:
            date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
            series_ticker = f"HIGH{code}"
            markets = self.client.get_markets(
                series_ticker=series_ticker,
                status=MarketStatus.OPEN,
                limit=50,
                fetch_all=True,
            )
            result: List[Dict] = []
            for market in markets:
                close_time = getattr(market, "close_time", None)
                market_date = close_time.date() if close_time else None
                if market_date and market_date != date_obj:
                    continue
                result.append(self._normalize_market(market))
            return result
        except KalshiAPIError as exc:
            logger.error("Kalshi API error finding markets: %s", exc)
            return []
        except Exception as exc:
            logger.error("Kalshi error finding weather markets: %s", exc)
            return []

    def get_orderbook(self, ticker: str) -> Optional[Dict]:
        if not self.enabled or not self.client:
            return None
        try:
            market = self.client.get_market(ticker)
            orderbook = market.get_orderbook()
            yes_levels = [
                {"price": float(price), "size": float(size)}
                for price, size in (getattr(orderbook.orderbook, "yes_dollars", None) or [])
            ]
            no_levels = [
                {"price": float(price), "size": float(size)}
                for price, size in (getattr(orderbook.orderbook, "no_dollars", None) or [])
            ]
            return {
                "ticker": ticker,
                "yes_bids": yes_levels,
                "no_bids": no_levels,
                "best_yes_bid": self._safe_float(getattr(market, "yes_bid_dollars", None)),
                "best_yes_ask": self._safe_float(getattr(market, "yes_ask_dollars", None)),
                "best_no_bid": self._safe_float(getattr(market, "no_bid_dollars", None)),
                "best_no_ask": self._safe_float(getattr(market, "no_ask_dollars", None)),
            }
        except Exception as exc:
            logger.error("Kalshi orderbook error for %s: %s", ticker, exc)
            return None

    def place_order(self, ticker: str, side: str, yes_price_cents: int, count: int = 1) -> Optional[Dict]:
        """
        Place an order on Kalshi.
        side: "yes" or "no"
        yes_price_cents: price in cents (1-99)
        """
        if not self.enabled or not self.trade_enabled or not self.client:
            return None

        try:
            action = Action.BUY
            order_side = Side.YES if side.lower() == "yes" else Side.NO
            price_dollars = f"{yes_price_cents / 100:.2f}"
            kwargs = {"yes_price_dollars": price_dollars} if order_side == Side.YES else {"no_price_dollars": price_dollars}
            order = self.client.portfolio.place_order(
                ticker,
                action,
                order_side,
                count_fp=str(count),
                **kwargs,
            )
            order = order.wait_until_terminal()
            return {
                "order_id": getattr(order, "order_id", None),
                "status": str(getattr(order, "status", "unknown")),
                "ticker": ticker,
                "side": side,
                "price_cents": yes_price_cents,
                "count": count,
            }
        except InsufficientFundsError:
            logger.error("Kalshi: insufficient funds")
            return None
        except RateLimitError:
            logger.warning("Kalshi: rate limited (auto-retry in progress)")
            return None
        except KalshiAPIError as exc:
            logger.error("Kalshi order error: %s", exc)
            return None

    def get_balance(self) -> Optional[float]:
        if not self.enabled or not self.client:
            return None
        try:
            bal = self.client.portfolio.get_balance()
            return float(bal.balance)
        except Exception:
            return None

    def get_positions(self) -> List[Dict]:
        if not self.enabled or not self.client:
            return []
        try:
            positions = self.client.portfolio.get_positions(fetch_all=True)
            return [
                {
                    "ticker": p.ticker,
                    "count": getattr(p, "position_fp", None),
                    "side": "yes" if float(getattr(p, "position_fp", 0.0) or 0.0) >= 0 else "no",
                }
                for p in positions
            ]
        except Exception:
            return []

    def compare_market_prices(
        self,
        city: str,
        target_date: str,
        polymarket_question: str,
        polymarket_yes_ask: float,
        polymarket_no_ask: float,
    ) -> Optional[Dict]:
        markets = self.find_weather_markets(city, target_date)
        if not markets:
            return None

        scored = [(self._match_score(polymarket_question, market), market) for market in markets]
        scored.sort(key=lambda item: item[0], reverse=True)
        best_score, best_market = scored[0]
        preferred_yes = "kalshi" if self._safe_float(best_market.get("yes_ask")) is not None and best_market["yes_ask"] < polymarket_yes_ask else "polymarket"
        preferred_no = "kalshi" if self._safe_float(best_market.get("no_ask")) is not None and best_market["no_ask"] < polymarket_no_ask else "polymarket"
        return {
            "city": city,
            "target_date": target_date,
            "match_score": best_score,
            "reference_question": polymarket_question,
            "kalshi_market_count": len(markets),
            "kalshi_best_ticker": best_market["ticker"],
            "kalshi_best_title": best_market["title"],
            "polymarket_yes_ask": round(polymarket_yes_ask, 4),
            "polymarket_no_ask": round(polymarket_no_ask, 4),
            "kalshi_yes_ask": best_market.get("yes_ask"),
            "kalshi_no_ask": best_market.get("no_ask"),
            "preferred_yes_platform": preferred_yes,
            "preferred_no_platform": preferred_no,
            "trade_enabled": self.trade_enabled,
        }

    def create_feed(self):
        if not self.enabled or not self.client or Feed is None:
            return None
        try:
            return Feed(self.client)
        except Exception as exc:
            logger.warning("Kalshi feed unavailable: %s", exc)
            return None

    def create_orderbook_manager(self, ticker: str):
        if not self.enabled or OrderbookManager is None:
            return None
        return OrderbookManager(ticker)

    def _normalize_market(self, market) -> Dict:
        return {
            "ticker": getattr(market, "ticker", None),
            "title": getattr(market, "title", "") or getattr(market, "subtitle", ""),
            "yes_bid": self._safe_float(getattr(market, "yes_bid_dollars", None)),
            "yes_ask": self._safe_float(getattr(market, "yes_ask_dollars", None)),
            "no_bid": self._safe_float(getattr(market, "no_bid_dollars", None)),
            "no_ask": self._safe_float(getattr(market, "no_ask_dollars", None)),
            "volume": self._safe_float(getattr(market, "volume_fp", None)) or 0.0,
            "close_time": getattr(market, "close_time", None).isoformat() if getattr(market, "close_time", None) else None,
            "platform": "kalshi",
        }

    def _match_score(self, reference_question: str, market: Dict) -> int:
        title = str(market.get("title", "")).lower()
        ref = reference_question.lower()
        ref_numbers = set(re.findall(r"\d+\.?\d*", ref))
        title_numbers = set(re.findall(r"\d+\.?\d*", title))
        score = len(ref_numbers & title_numbers)
        if any(token in title for token in ("high", "temperature", "temp")):
            score += 2
        if any(city_code.lower() in title for city_code in self.CITY_CODES.values()):
            score += 1
        return score

    @staticmethod
    def _safe_float(value) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
