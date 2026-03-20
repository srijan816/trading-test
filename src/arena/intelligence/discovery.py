"""Discovery query builder — generates targeted search queries to find
information asymmetries rather than synthesize probabilities.

The key insight: we don't ask the LLM "what is the probability?" (the market
already knows). We ask "what NEW information exists that the market might
not have priced in yet?"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from os import getenv
from pathlib import Path
import re
import sqlite3
from urllib.parse import urlparse

from arena.adapters.weather_openmeteo import CITY_COORDS


class SignalType(str, Enum):
    BREAKING_NEWS = "breaking_news"
    WEATHER_ALERT = "weather_alert"
    DATA_RELEASE = "data_release"
    REGULATORY_CHANGE = "regulatory"
    SOURCE_DISAGREEMENT = "source_disagree"
    STALE_MARKET = "stale_market"
    MARKET_EXPANSION = "market_expansion"
    CROSS_VENUE = "cross_venue"
    NO_SIGNAL = "no_signal"


@dataclass
class DiscoveryQuery:
    """A query designed to find new information, not synthesize probabilities."""

    market_id: str
    market_question: str
    category: str
    query_text: str
    focus: str
    recency_hours: int = 6
    max_sources: int = 3
    skip_probability: bool = True


@dataclass
class DiscoverySignal:
    """A piece of new information that might create a trading opportunity."""

    signal_type: SignalType
    headline: str
    detail: str
    source_url: str
    source_name: str
    recency_minutes: int
    relevance_score: float
    market_id: str
    direction: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


_STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "be",
    "by",
    "for",
    "from",
    "if",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "the",
    "this",
    "to",
    "what",
    "when",
    "will",
    "with",
}

_TEMPORAL_MARKERS = (
    "breaking",
    "just announced",
    "today",
    "this morning",
    "this afternoon",
    "this evening",
    "hours ago",
    "minutes ago",
    "issued alert",
    "filed today",
    "newly released",
)

_AUTHORITY_MARKERS = (
    "according to",
    "official statement",
    "press release",
    "court ruled",
    "judge ordered",
    "sec filing",
    "cftc",
    "nws",
    "noaa",
)

_MAGNITUDE_MARKERS = (
    "record",
    "significant shift",
    "surprise",
    "first time",
    "unprecedented",
    "warning",
    "emergency",
)


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _tokenize(text: str) -> list[str]:
    return [token for token in re.findall(r"[A-Za-z0-9']+", text.lower()) if token and token not in _STOP_WORDS]


def _extract_city(question: str) -> str | None:
    lowered = question.lower()
    for city in sorted(CITY_COORDS.keys(), key=len, reverse=True):
        if city in lowered:
            return city.title()
    for pattern in (
        r"\bin ([a-z .'-]+?) be\b",
        r"\bin ([a-z .'-]+?) on\b",
        r"\bfor ([a-z .'-]+?) on\b",
    ):
        match = re.search(pattern, lowered)
        if match:
            return match.group(1).strip().title()
    return None


def _extract_core_topic(question: str) -> str:
    text = _normalize_text(question.rstrip(" ?"))
    text = re.sub(r"^(will|can|does|is|are)\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(on|by|before|during)\b.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(today|tomorrow|this week|this month)\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" -:,")
    return text or question.strip()


def _source_name_from_url(url: str) -> str:
    host = urlparse(url).netloc.lower().removeprefix("www.")
    if not host:
        return "Unknown source"
    parts = host.split(".")
    if len(parts) >= 2:
        return parts[-2].upper() if len(parts[-2]) <= 4 else parts[-2].title()
    return host.title()


def _summarize_detail(text: str, limit: int = 320) -> str:
    compact = _normalize_text(text)
    if len(compact) <= limit:
        return compact
    clipped = compact[:limit].rsplit(" ", 1)[0]
    return f"{clipped}..."


def _trim_sentence(sentence: str, limit: int = 140) -> str:
    compact = _normalize_text(sentence)
    if len(compact) <= limit:
        return compact
    clipped = compact[:limit].rsplit(" ", 1)[0]
    return f"{clipped}..."


def _extract_direction(signal_text: str, market_question: str, category: str) -> str:
    lowered_signal = signal_text.lower()
    lowered_question = market_question.lower()

    warm_markers = ("warmer", "heat", "hotter", "above normal", "record high")
    cool_markers = ("colder", "cold", "freeze", "snow", "record low", "below normal")
    positive_markers = ("approved", "won", "passed", "signed", "support surged", "leads")
    negative_markers = ("blocked", "lost", "denied", "rejected", "fell", "withdrawn")

    if category == "weather":
        wants_low = any(marker in lowered_question for marker in ("below", "or below", "under", "at most"))
        wants_high = any(marker in lowered_question for marker in ("above", "or above", "over", "at least"))
        if wants_low:
            if any(marker in lowered_signal for marker in cool_markers):
                return "bullish_yes"
            if any(marker in lowered_signal for marker in warm_markers):
                return "bullish_no"
        if wants_high:
            if any(marker in lowered_signal for marker in warm_markers):
                return "bullish_yes"
            if any(marker in lowered_signal for marker in cool_markers):
                return "bullish_no"
        return "ambiguous"

    if any(marker in lowered_signal for marker in positive_markers):
        return "bullish_yes"
    if any(marker in lowered_signal for marker in negative_markers):
        return "bullish_no"
    return "ambiguous"


class DiscoveryQueryBuilder:
    """Builds discovery queries tailored to market category."""

    def build_query(
        self,
        market_id: str,
        question: str,
        category: str,
        ensemble_data: dict | None = None,
        market_data: dict | None = None,
    ) -> DiscoveryQuery:
        if category == "weather":
            return self._build_weather_query(market_id, question, ensemble_data, market_data)
        if category in ("politics", "legal", "geopolitics"):
            return self._build_political_query(market_id, question, market_data)
        if category == "crypto":
            return self._build_crypto_query(market_id, question, market_data)
        return self._build_generic_query(market_id, question, category, market_data)

    def _build_weather_query(
        self,
        market_id: str,
        question: str,
        ensemble_data: dict | None = None,
        market_data: dict | None = None,
    ) -> DiscoveryQuery:
        """Weather discovery focuses on new alerts, sudden model shifts, and station issues."""

        city = _extract_city(question) or _extract_core_topic(question)
        sigma = float((ensemble_data or {}).get("sigma") or 0.0)
        focus = "breaking weather alerts, forecast shifts, and station anomalies"
        recency_hours = 6

        if sigma and sigma < 1.0:
            focus = "official weather alerts only because forecast models already agree tightly"
            query_text = (
                f"\"{city}\" weather alert warning advisory issued today "
                "site:weather.gov OR site:alerts.weather.gov OR site:noaa.gov"
            )
        elif sigma and sigma > 3.0:
            focus = "weather alerts, unusual model disagreement, and station outage notices"
            recency_hours = 12
            query_text = (
                f"\"{city}\" weather alert OR forecast shift OR model disagreement OR station outage "
                "today site:weather.gov OR site:noaa.gov OR site:aviationweather.gov"
            )
        else:
            query_text = (
                f"\"{city}\" weather warning advisory alert forecast shift last 6 hours "
                "site:weather.gov OR site:noaa.gov"
            )

        return DiscoveryQuery(
            market_id=market_id,
            market_question=question,
            category="weather",
            query_text=query_text,
            focus=focus,
            recency_hours=recency_hours,
            max_sources=3,
            skip_probability=True,
        )

    def _build_political_query(
        self,
        market_id: str,
        question: str,
        market_data: dict | None = None,
    ) -> DiscoveryQuery:
        """Political discovery focuses on very recent primary reporting and official releases."""

        topic = _extract_core_topic(question)
        return DiscoveryQuery(
            market_id=market_id,
            market_question=question,
            category=str((market_data or {}).get("category") or "politics"),
            query_text=(
                f"\"{topic}\" breaking news today Reuters OR AP OR Bloomberg OR official statement "
                "OR press release OR court filing"
            ),
            focus="breaking news, official statements, court filings, and fresh polls",
            recency_hours=2,
            max_sources=3,
            skip_probability=True,
        )

    def _build_crypto_query(
        self,
        market_id: str,
        question: str,
        market_data: dict | None = None,
    ) -> DiscoveryQuery:
        """Crypto discovery focuses on exchange, regulatory, and on-chain changes."""

        topic = _extract_core_topic(question)
        return DiscoveryQuery(
            market_id=market_id,
            market_question=question,
            category="crypto",
            query_text=(
                f"\"{topic}\" exchange announcement OR sec filing OR cftc OR listing OR delisting "
                "OR hack OR whale transfer last hour"
            ),
            focus="exchange announcements, regulatory actions, and large on-chain transactions",
            recency_hours=1,
            max_sources=3,
            skip_probability=True,
        )

    def _build_generic_query(
        self,
        market_id: str,
        question: str,
        category: str,
        market_data: dict | None = None,
    ) -> DiscoveryQuery:
        """Fallback discovery for unknown categories."""

        topic = _extract_core_topic(question)
        return DiscoveryQuery(
            market_id=market_id,
            market_question=question,
            category=category or str((market_data or {}).get("category") or "event"),
            query_text=(
                f"\"{topic}\" new information today breaking latest official update last 4 hours"
            ),
            focus="new developments that could move the market",
            recency_hours=4,
            max_sources=3,
            skip_probability=True,
        )


class SignalClassifier:
    """Classifies Nexus research responses into actionable signals."""

    def classify(
        self,
        research_text: str,
        sources: list,
        market_question: str,
        category: str,
    ) -> list[DiscoverySignal]:
        """Extract signals from research text."""

        text = _normalize_text(research_text)
        lowered = text.lower()
        if not text:
            return [
                DiscoverySignal(
                    signal_type=SignalType.NO_SIGNAL,
                    headline="No research text returned",
                    detail="The discovery query completed without usable report text.",
                    source_url="",
                    source_name="Unknown source",
                    recency_minutes=-1,
                    relevance_score=0.0,
                    market_id="",
                    direction="none",
                )
            ]

        source_url = ""
        source_name = "Unknown source"
        if sources:
            first_source = sources[0] if isinstance(sources[0], dict) else {"url": str(sources[0])}
            source_url = str(first_source.get("url", "") or "")
            source_name = str(first_source.get("title", "") or _source_name_from_url(source_url))

        recency = self._extract_recency(text)
        relevance = self._estimate_relevance(text, market_question)
        direction = _extract_direction(text, market_question, category)
        signals: list[DiscoverySignal] = []

        def add_signal(signal_type: SignalType, headline_prefix: str) -> None:
            headline = _trim_sentence(f"{headline_prefix}: {text}", limit=140)
            signals.append(
                DiscoverySignal(
                    signal_type=signal_type,
                    headline=headline,
                    detail=_summarize_detail(text),
                    source_url=source_url,
                    source_name=source_name,
                    recency_minutes=recency,
                    relevance_score=relevance,
                    market_id="",
                    direction=direction if signal_type != SignalType.NO_SIGNAL else "none",
                )
            )

        has_novelty = any(marker in lowered for marker in _TEMPORAL_MARKERS)
        has_authority = any(marker in lowered for marker in _AUTHORITY_MARKERS)
        has_magnitude = any(marker in lowered for marker in _MAGNITUDE_MARKERS)

        if category == "weather":
            if any(term in lowered for term in ("warning", "advisory", "watch", "heat advisory", "storm warning", "issued alert", "record high", "record low")):
                add_signal(SignalType.WEATHER_ALERT, "Weather alert")
            if any(term in lowered for term in ("models disagree", "forecast shifted", "forecast shift", "ensemble spread", "model disagreement")):
                add_signal(SignalType.SOURCE_DISAGREEMENT, "Forecast disagreement")
        else:
            if ("breaking" in lowered or has_novelty) and relevance >= 0.2:
                add_signal(SignalType.BREAKING_NEWS, "Breaking news")
            if any(term in lowered for term in ("court ruled", "judge ordered", "executive order", "regulator", "sec filing", "cftc", "law signed", "policy change")):
                add_signal(SignalType.REGULATORY_CHANGE, "Regulatory change")
            if any(term in lowered for term in ("poll released", "survey shows", "earnings", "cpi", "jobs report", "economic data", "data released", "official stats")):
                add_signal(SignalType.DATA_RELEASE, "Data release")

        if "market hasn't moved" in lowered or "price has not moved" in lowered or "still trading near" in lowered:
            add_signal(SignalType.STALE_MARKET, "Potential stale market")

        if not signals and not (has_novelty or has_authority or has_magnitude):
            return [
                DiscoverySignal(
                    signal_type=SignalType.NO_SIGNAL,
                    headline="No new signals detected",
                    detail="The research report mostly contained background context rather than fresh market-moving information.",
                    source_url=source_url,
                    source_name=source_name,
                    recency_minutes=recency,
                    relevance_score=relevance,
                    market_id="",
                    direction="none",
                )
            ]

        if not signals:
            add_signal(SignalType.BREAKING_NEWS, "Fresh information")

        deduped: list[DiscoverySignal] = []
        seen_types: set[str] = set()
        for signal in signals:
            if signal.signal_type.value in seen_types:
                continue
            seen_types.add(signal.signal_type.value)
            deduped.append(signal)
        return deduped

    def _extract_recency(self, text: str) -> int:
        """Estimate how old the information is in minutes."""

        lowered = text.lower()
        match = re.search(r"(\d+)\s*(minute|min|hour|hr|day)s?\s+ago", lowered)
        if match:
            value = int(match.group(1))
            unit = match.group(2)
            if unit.startswith(("minute", "min")):
                return value
            if unit.startswith(("hour", "hr")):
                return value * 60
            return value * 1440
        if "just announced" in lowered or "breaking" in lowered:
            return 15
        if "this morning" in lowered:
            return 240
        if "this afternoon" in lowered:
            return 180
        if "this evening" in lowered:
            return 120
        if "today" in lowered or "filed today" in lowered or "issued alert" in lowered:
            return 360
        if "yesterday" in lowered:
            return 1440
        return -1

    def _estimate_relevance(self, signal_text: str, market_question: str) -> float:
        """Score 0.0-1.0 for how relevant a signal is to the market question."""

        signal_tokens = set(_tokenize(signal_text))
        question_tokens = set(_tokenize(market_question))
        if not signal_tokens or not question_tokens:
            return 0.0

        overlap = signal_tokens & question_tokens
        entity_tokens = {
            token.lower()
            for token in re.findall(r"\b[A-Z][a-zA-Z0-9.-]+\b", market_question)
            if token.lower() not in _STOP_WORDS
        }
        entity_overlap = signal_tokens & entity_tokens

        overlap_score = len(overlap) / max(len(question_tokens), 1)
        entity_score = len(entity_overlap) / max(len(entity_tokens), 1) if entity_tokens else 0.0
        return round(min(1.0, (overlap_score * 0.65) + (entity_score * 0.35)), 3)


def should_spend_on_research(
    market_data: dict,
    estimated_call_cost_usd: float = 0.05,
) -> tuple[bool, str]:
    """Check if research spend is justified by potential profit."""

    volume_usd = float(market_data.get("volume_usd") or market_data.get("volume") or 0.0)
    max_position_size = float(
        market_data.get("max_position_size")
        or market_data.get("max_order_usd")
        or market_data.get("max_trade_size_usd")
        or 0.0
    )
    edge_estimate = market_data.get("edge_estimate")
    if edge_estimate is None:
        edge_bps = float(market_data.get("expected_edge_bps") or 0.0)
        edge_estimate = edge_bps / 10000.0
    edge_estimate = abs(float(edge_estimate or 0.0))
    resolution_hours = float(market_data.get("resolution_hours") or market_data.get("time_remaining_hours") or 0.0)
    breaking_news_candidate = bool(market_data.get("breaking_news_candidate")) or str(market_data.get("category") or "").lower() == "breaking_news"

    if volume_usd < 100:
        return False, f"skip research: market volume ${volume_usd:.2f} is below the $100 floor"
    if resolution_hours < 1.0 and not breaking_news_candidate:
        return False, f"skip research: market resolves in {resolution_hours:.2f}h and is not a breaking-news candidate"
    expected_profit = max_position_size * edge_estimate
    min_justified = estimated_call_cost_usd * 3.0
    if expected_profit < min_justified:
        return False, f"skip research: expected profit ${expected_profit:.2f} is below 3x call cost (${min_justified:.2f})"
    return True, f"research justified: expected profit ${expected_profit:.2f} exceeds cost gate ${min_justified:.2f}"


def compute_research_roi(db_path: str | Path, hours: int = 24 * 7) -> dict:
    """Compute discovery research return on investment from the SQLite database."""

    estimated_call_cost = float(getenv("NEXUS_ESTIMATED_CALL_COST_USD", "0.05"))
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cutoff = f"-{int(hours)} hours"
        total_calls = conn.execute(
            """
            SELECT COUNT(*)
            FROM research_log
            WHERE endpoint IS NOT NULL
              AND timestamp >= datetime('now', ?)
              AND mode = 'discovery'
            """,
            (cutoff,),
        ).fetchone()[0]
        signals_found = conn.execute(
            """
            SELECT COUNT(*)
            FROM discovery_alerts
            WHERE created_at >= datetime('now', ?)
              AND signal_type != ?
            """,
            (cutoff, SignalType.NO_SIGNAL.value),
        ).fetchone()[0]
        signals_acted_on = conn.execute(
            """
            SELECT COUNT(*)
            FROM discovery_alerts
            WHERE created_at >= datetime('now', ?)
              AND signal_type != ?
              AND acted_on = 1
            """,
            (cutoff, SignalType.NO_SIGNAL.value),
        ).fetchone()[0]
        pnl_row = conn.execute(
            """
            WITH acted_alerts AS (
                SELECT DISTINCT market_id, COALESCE(strategy_id, '') AS strategy_id
                FROM discovery_alerts
                WHERE created_at >= datetime('now', ?)
                  AND signal_type != ?
                  AND acted_on = 1
            )
            SELECT COALESCE(SUM(p.realized_pnl + p.unrealized_pnl), 0.0) AS pnl
            FROM positions p
            JOIN acted_alerts a
              ON a.market_id = p.market_id
             AND (a.strategy_id = '' OR a.strategy_id = p.strategy_id)
            """,
            (cutoff, SignalType.NO_SIGNAL.value),
        ).fetchone()
        estimated_pnl = float((pnl_row["pnl"] if pnl_row else 0.0) or 0.0)
        total_cost = total_calls * estimated_call_cost
        roi = (estimated_pnl / total_cost) if total_cost > 0 else 0.0
        return {
            "total_research_cost_usd": round(total_cost, 4),
            "total_calls": int(total_calls),
            "signals_found": int(signals_found),
            "signals_acted_on": int(signals_acted_on),
            "estimated_pnl_from_signals": round(estimated_pnl, 4),
            "roi": round(roi, 4),
        }
    finally:
        conn.close()
