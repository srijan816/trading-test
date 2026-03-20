"""Research assistant integration for market-level pre-trade context."""
from __future__ import annotations

from datetime import datetime, timezone
import logging
import re

from arena.adapters.search_perplexia import PerplexiaSearchClient

logger = logging.getLogger(__name__)


def _build_query(market_question: str) -> str:
    lowered = market_question.lower()
    if any(term in lowered for term in ("temperature", "rain", "precipitation", "weather")):
        return f"{market_question} official forecast and latest weather observations"
    if any(term in lowered for term in ("bitcoin", "btc", "ethereum", "eth", "crypto")):
        return f"{market_question} latest spot price, catalysts, and market consensus"
    return f"{market_question} latest developments, consensus view, and market-moving evidence"


def _extract_findings(summary: str, fallback_snippets: list[str]) -> list[str]:
    findings: list[str] = []
    for raw in re.split(r"(?:\n+|(?<=[.!?])\s+)", summary):
        text = raw.strip(" -*\t\r\n")
        if len(text) < 25:
            continue
        findings.append(text)
        if len(findings) >= 5:
            break
    if findings:
        return findings
    for snippet in fallback_snippets:
        text = snippet.strip()
        if not text:
            continue
        findings.append(text[:220])
        if len(findings) >= 5:
            break
    return findings


def _build_market_findings(result: dict) -> list[str]:
    findings: list[str] = []
    probability = result.get("probability")
    confidence = result.get("confidence")
    edge = result.get("edge_assessment") or {}
    reasoning = str(result.get("reasoning", "") or "").strip()
    if probability is not None:
        findings.append(f"Reference probability: {float(probability):.3f} ({confidence or 'unknown'} confidence)")
    recommendation = edge.get("recommendation")
    if recommendation:
        findings.append(f"Edge recommendation: {recommendation} at {int(edge.get('raw_edge_bps', 0))} bps raw edge")
    if result.get("ensemble_override_triggered"):
        ensemble_probability = result.get("ensemble_probability")
        findings.append(f"Ensemble override triggered: use ensemble probability {float(ensemble_probability):.3f} as the weather anchor")
    if reasoning:
        findings.extend(_extract_findings(reasoning, []))
    risk_factors = result.get("edge_assessment", {}).get("risk_factors") or []
    for risk in risk_factors[:2]:
        findings.append(f"Risk: {risk}")
    return findings[:5]


async def research_market(
    market_question: str,
    mode: str = "standard",
    *,
    market_type: str = "auto",
    market_data: dict | None = None,
    ensemble_data: dict | None = None,
    calibration_data: dict | None = None,
    model: str | None = None,
) -> dict | None:
    """
    Query the research assistant for market-relevant information.

    Returns a dict with keys:
    query, findings, sources, summary, provider, mode, session_id, retrieved_at
    """
    client = PerplexiaSearchClient(
        mode=mode,
        output_length="short",
        max_sources=5,
    )
    query = _build_query(market_question)

    market_payload = {
        "question": market_question,
        "market_type": market_type,
        "market_data": market_data or {},
        "ensemble_data": ensemble_data,
        "calibration_data": calibration_data,
        "model": model,
        "search_depth": mode,
    }

    try:
        market_result = await client.market_research(market_payload)
    except Exception as exc:
        logger.warning("Structured market research unavailable for '%s': %s", market_question, exc)
        try:
            results = await client.search(query, num_results=5)
        except Exception as fallback_exc:
            logger.warning("Research assistant unavailable for '%s': %s", market_question, fallback_exc)
            return None
        if not results:
            return None

        primary = results[0]
        metadata = primary.metadata if isinstance(primary.metadata, dict) else {}
        resolved_mode = str(metadata.get("mode", client.mode) or client.mode)
        summary = str(metadata.get("report", "") or primary.snippet or "").strip()
        findings = _extract_findings(summary, [item.snippet for item in results[1:]])
        sources = [item.url for item in results if item.url and not item.url.startswith("perplexia://")]
        if not findings and not sources and not summary:
            return None
        return {
            "provider": "perplexia",
            "query": query,
            "mode": resolved_mode,
            "summary": summary[:1800],
            "findings": findings[:5],
            "sources": sources[:5],
            "sources_detail": [
                {"url": item.url, "title": item.title}
                for item in results
                if item.url and not item.url.startswith("perplexia://")
            ][:5],
            "session_id": metadata.get("session_id"),
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "full_report": summary[:6000],
            "model_used": metadata.get("model_used"),
            "endpoint": "/api/v1/research",
            "fallback_used": True,
        }

    sources = [str(item.get("url")) for item in (market_result.get("sources") or []) if item.get("url")]
    findings = _build_market_findings(market_result)
    reasoning = str(market_result.get("reasoning", "") or "").strip()
    edge = market_result.get("edge_assessment") or {}
    summary_parts = []
    probability = market_result.get("probability")
    if probability is not None:
        summary_parts.append(f"Probability YES: {float(probability):.3f}")
    if edge.get("recommendation"):
        summary_parts.append(f"Recommendation: {edge['recommendation']}")
    if reasoning:
        summary_parts.append(reasoning)
    summary = " | ".join(summary_parts).strip()[:1800]
    if not findings and not sources and not summary:
        return None
    return {
        "provider": "perplexia-market",
        "query": market_question,
        "mode": mode,
        "summary": summary,
        "findings": findings,
        "sources": sources[:5],
        "sources_detail": [
            {
                "url": str(item.get("url", "") or ""),
                "title": str(item.get("title", "") or "Untitled source"),
            }
            for item in (market_result.get("sources") or [])
            if item.get("url")
        ][:5],
        "session_id": None,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "probability": market_result.get("probability"),
        "confidence_label": market_result.get("confidence"),
        "edge_assessment": edge,
        "ensemble_probability": market_result.get("ensemble_probability"),
        "llm_probability": market_result.get("llm_probability"),
        "ensemble_override_triggered": bool(market_result.get("ensemble_override_triggered")),
        "search_queries_used": market_result.get("search_queries_used", []),
        "market_type": market_result.get("market_type"),
        "from_cache": bool(market_result.get("from_cache")),
        "model_used": market_result.get("model_used"),
        "tokens_used": market_result.get("tokens_used", {}),
        "reasoning_trace": market_result.get("reasoning_trace"),
        "full_report": reasoning or summary,
        "endpoint": "/api/v1/market-research",
        "structured": market_result,
        "fallback_used": False,
    }


def format_research_for_packet(research_result: dict | None) -> str:
    """Format research results for inclusion in an info packet."""
    if not research_result:
        return ""

    findings = research_result.get("findings", [])
    if not findings:
        return ""

    lines = [
        "=== RESEARCH CONTEXT ===",
        f"Source: {research_result.get('provider', 'research assistant')}",
        f"Query: \"{research_result.get('query', 'N/A')}\"",
        f"Mode: {research_result.get('mode', 'quick')}",
        "Use this as reference context, not a blind override.",
        "Key findings:",
    ]
    probability = research_result.get("probability")
    if probability is not None:
        confidence = research_result.get("confidence_label") or research_result.get("confidence")
        lines.append(f"  Reference probability (YES): {float(probability):.3f} [{confidence}]")
    edge = research_result.get("edge_assessment") or {}
    if edge.get("recommendation"):
        lines.append(
            f"  Market edge view: {edge.get('recommendation')} "
            f"(raw edge {int(edge.get('raw_edge_bps', 0))} bps, adjusted {int(edge.get('adjusted_edge_bps', 0))} bps)"
        )
    if research_result.get("ensemble_override_triggered"):
        lines.append(
            f"  Ensemble override flag: YES — use ensemble probability {float(research_result.get('ensemble_probability', 0.0)):.3f}"
        )
    for i, finding in enumerate(findings[:5], 1):
        lines.append(f"  {i}. {finding}")

    sources = research_result.get("sources", [])
    if sources:
        lines.append(f"Sources: {', '.join(sources[:3])}")

    return "\n".join(lines)
