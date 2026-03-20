from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any

from arena.models import Decision, EvidenceItem, ProposedAction, SearchRecord, new_id


DECISION_JSON_SCHEMA = {
    "type": "object",
    "required": [
        "timestamp",
        "strategy_id",
        "markets_considered",
        "evidence_items",
        "risk_notes",
        "exit_plan",
        "thinking",
        "web_searches_used",
        "actions",
    ],
    "properties": {
        "timestamp": {"type": "string"},
        "strategy_id": {"type": "string"},
        "markets_considered": {"type": "array", "items": {"type": "string"}},
        "predicted_probability": {"type": ["number", "null"]},
        "market_implied_probability": {"type": ["number", "null"]},
        "expected_edge_bps": {"type": ["integer", "null"]},
        "confidence": {"type": ["number", "null"]},
        "evidence_items": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["source", "content"],
                "properties": {
                    "source": {"type": "string"},
                    "content": {"type": "string"},
                },
            },
        },
        "risk_notes": {"type": "string"},
        "exit_plan": {"type": "string"},
        "thinking": {"type": "string"},
        "web_searches_used": {"type": "array"},
        "actions": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["action_type", "market_id", "venue", "outcome_id", "outcome_label", "amount_usd", "reasoning_summary"],
                "properties": {
                    "action_type": {"type": "string"},
                    "market_id": {"type": "string"},
                    "venue": {"type": "string"},
                    "outcome_id": {"type": "string"},
                    "outcome_label": {"type": "string"},
                    "amount_usd": {"type": "number"},
                    "limit_price": {"type": ["number", "null"]},
                    "reasoning_summary": {"type": "string"},
                },
            },
        },
        "no_action_reason": {"type": ["string", "null"]},
    },
}


def _strip_reasoning_traces(value: Any) -> Any:
    if isinstance(value, str):
        return re.sub(r"<think>.*?</think>", "", value, flags=re.DOTALL).strip()
    if isinstance(value, list):
        return [_strip_reasoning_traces(item) for item in value]
    if isinstance(value, dict):
        return {key: _strip_reasoning_traces(item) for key, item in value.items()}
    return value


def _normalize_action_type(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized in {"BUY", "SELL", "HOLD"}:
        return normalized
    if "_" in normalized:
        prefix = normalized.split("_", 1)[0]
        if prefix in {"BUY", "SELL", "HOLD"}:
            return prefix
    return normalized


def normalize_evidence_items(raw: Any) -> list[dict]:
    if not raw:
        return []
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    items: list[dict] = []
    for item in raw:
        if isinstance(item, str):
            items.append({"source": "llm_stated", "content": item})
        elif isinstance(item, dict):
            content = item.get("content") or item.get("detail") or item.get("description") or item.get("value")
            source = item.get("source") or "llm_stated"
            if content is not None:
                items.append({"source": str(source), "content": str(content)})
    return items


def normalize_scalar_probability(raw: Any, market_id: str | None = None) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        value = float(raw)
    elif isinstance(raw, str):
        cleaned = raw.strip().rstrip("%")
        try:
            value = float(cleaned)
            if "%" in raw:
                value /= 100.0
        except ValueError:
            return None
    elif isinstance(raw, dict):
        if market_id and market_id in raw:
            return normalize_scalar_probability(raw[market_id], None)
        for value in raw.values():
            normalized = normalize_scalar_probability(value, None)
            if normalized is not None:
                return normalized
        return None
    else:
        return None
    if value > 1.0 and value <= 100.0:
        value /= 100.0
    return max(0.0, min(1.0, value))


def normalize_confidence(raw: Any, market_id: str | None = None) -> float:
    if isinstance(raw, dict):
        if market_id and market_id in raw:
            normalized = normalize_confidence(raw[market_id], None)
            return normalized
        values = [normalize_confidence(value, None) for value in raw.values()]
        values = [value for value in values if value is not None]
        return max(values) if values else 0.5
    normalized = normalize_scalar_probability(raw, market_id)
    return normalized if normalized is not None else 0.5


def normalize_expected_edge_bps(raw: Any, market_id: str | None = None) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        if market_id and market_id in raw:
            return normalize_expected_edge_bps(raw[market_id], None)
        for value in raw.values():
            normalized = normalize_expected_edge_bps(value, None)
            if normalized is not None:
                return normalized
        return None
    if isinstance(raw, (int, float)):
        return int(float(raw))
    if isinstance(raw, str):
        match = re.search(r"-?\d+(?:\.\d+)?", raw.replace(",", ""))
        if match:
            return int(float(match.group(0)))
    return None


def _normalize_market_ids(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, (int, float)):
        return [str(raw)]
    if isinstance(raw, dict):
        return [str(key) for key in raw.keys()]
    if isinstance(raw, list):
        result = []
        for item in raw:
            if isinstance(item, (str, int, float)):
                result.append(str(item))
            elif isinstance(item, dict):
                value = item.get("market_id") or item.get("id")
                if value is not None:
                    result.append(str(value))
        return result
    return []


def _normalize_web_searches(raw: Any) -> list[dict]:
    if not raw:
        return []
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    searches: list[dict] = []
    for item in raw:
        if isinstance(item, str):
            searches.append({"query": "", "results_summary": item, "source_urls": []})
        elif isinstance(item, dict):
            searches.append(
                {
                    "query": str(item.get("query", "")),
                    "results_summary": str(item.get("results_summary", item.get("key_findings", item.get("summary", "")))),
                    "source_urls": item.get("source_urls", []) if isinstance(item.get("source_urls", []), list) else [str(item.get("source_urls", ""))],
                }
            )
    return searches


def _normalize_reasoning_text(payload: dict) -> str:
    return str(payload.get("thinking") or payload.get("reasoning") or payload.get("analysis") or "")


def _normalize_text_list(raw: Any) -> str:
    if isinstance(raw, list):
        return " ".join(str(item) for item in raw if item)
    if raw is None:
        return ""
    return str(raw)


def _normalize_amount(raw: Any, default: float = 50.0) -> float:
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str):
        match = re.search(r"-?\d+(?:\.\d+)?", raw.replace(",", ""))
        if match:
            return float(match.group(0))
    return default


def _normalize_action_label(action: dict, market: dict) -> tuple[str | None, str | None]:
    outcome_id = action.get("outcome_id")
    outcome_label = action.get("outcome_label")
    outcomes = market.get("outcomes", [])
    if outcome_id:
        for outcome in outcomes:
            if str(outcome.get("outcome_id")) == str(outcome_id):
                return str(outcome.get("outcome_id")), str(outcome.get("label"))
    if outcome_label:
        normalized_target = re.sub(r"[^a-z0-9]+", "", str(outcome_label).lower())
        for outcome in outcomes:
            label = str(outcome.get("label", ""))
            if re.sub(r"[^a-z0-9]+", "", label.lower()) == normalized_target:
                return str(outcome.get("outcome_id")), label
        question_norm = re.sub(r"[^a-z0-9]+", "", str(market.get("question", "")).lower())
        if normalized_target and normalized_target in question_norm:
            yes_outcome = next((item for item in outcomes if str(item.get("label", "")).lower() == "yes"), None)
            if yes_outcome:
                return str(yes_outcome.get("outcome_id")), str(yes_outcome.get("label"))
    return None, None


def normalize_llm_output(raw_dict: dict, markets_in_packet: list[dict], max_order_usd: float = 100.0) -> dict:
    normalized = dict(_strip_reasoning_traces(raw_dict))
    reasoning = _normalize_reasoning_text(normalized)
    normalized["thinking"] = reasoning
    normalized["risk_notes"] = _normalize_text_list(normalized.get("risk_notes"))
    normalized["exit_plan"] = _normalize_text_list(normalized.get("exit_plan"))
    normalized["markets_considered"] = _normalize_market_ids(normalized.get("markets_considered"))
    lead_market_id = normalized["markets_considered"][0] if normalized["markets_considered"] else None
    normalized["predicted_probability"] = normalize_scalar_probability(normalized.get("predicted_probability"), lead_market_id)
    normalized["market_implied_probability"] = normalize_scalar_probability(normalized.get("market_implied_probability"), lead_market_id)
    normalized["expected_edge_bps"] = normalize_expected_edge_bps(normalized.get("expected_edge_bps"), lead_market_id)
    normalized["confidence"] = normalize_confidence(normalized.get("confidence"), lead_market_id)
    normalized["evidence_items"] = normalize_evidence_items(normalized.get("evidence_items"))
    normalized["web_searches_used"] = _normalize_web_searches(normalized.get("web_searches_used"))

    packet_markets = {str(market.get("market_id")): market for market in markets_in_packet}
    raw_actions = normalized.get("actions", [])
    if isinstance(raw_actions, dict):
        raw_actions = [raw_actions]
    elif not isinstance(raw_actions, list):
        raw_actions = []
    normalized_actions: list[dict] = []
    for raw_action in raw_actions:
        if not isinstance(raw_action, dict):
            continue
        action = dict(raw_action)
        action["action_type"] = _normalize_action_type(
            action.get("action_type") or action.get("type") or action.get("trade_type") or action.get("side") or "BUY"
        )
        action["market_id"] = str(action.get("market_id") or action.get("market") or action.get("marketId") or "")
        action["amount_usd"] = min(max(_normalize_amount(action.get("amount_usd") or action.get("amount") or action.get("usd"), 50.0), 10.0), max_order_usd)
        market = packet_markets.get(action["market_id"])
        if market:
            action.setdefault("venue", market.get("venue"))
            action["outcome_id"] = action.get("outcome_id") or action.get("token_id") or action.get("contract_id")
            action["outcome_label"] = action.get("outcome_label") or action.get("outcome") or action.get("label") or action.get("bucket")
            outcome_id, outcome_label = _normalize_action_label(action, market)
            action["outcome_id"] = outcome_id
            action["outcome_label"] = outcome_label
            action.setdefault("limit_price", None)
            action.setdefault("reasoning_summary", "LLM-generated trade idea.")
            required = ("action_type", "market_id", "venue", "outcome_id", "outcome_label", "amount_usd")
            if all(action.get(field) not in {None, ""} for field in required):
                normalized_actions.append(action)
    normalized["actions"] = normalized_actions
    if not normalized_actions and not normalized.get("no_action_reason"):
        normalized["no_action_reason"] = "No valid trade actions remained after payload normalization."
    return _normalize_payload(normalized)


def _normalize_payload(payload: dict) -> dict:
    normalized = dict(payload)
    if "thinking" not in normalized and "reasoning" in normalized:
        normalized["thinking"] = normalized["reasoning"]
    list_fields = ("markets_considered", "evidence_items", "web_searches_used", "actions")
    for field in list_fields:
        value = normalized.get(field, [])
        if isinstance(value, list):
            continue
        if isinstance(value, dict):
            normalized[field] = [value]
        elif field == "markets_considered" and isinstance(value, str):
            normalized[field] = [value]
        else:
            normalized[field] = []
    normalized_actions = []
    for item in normalized.get("actions", []):
        action = dict(item)
        if "action_type" not in action and "type" in action:
            action["action_type"] = action["type"]
        action["action_type"] = _normalize_action_type(action.get("action_type", ""))
        normalized_actions.append(action)
    normalized["actions"] = normalized_actions
    return normalized


def validate_decision_payload(payload: dict) -> None:
    payload = _normalize_payload(payload)
    missing = [field for field in DECISION_JSON_SCHEMA["required"] if field not in payload]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    for action in payload.get("actions", []):
        if action["action_type"] not in {"BUY", "SELL", "HOLD"}:
            raise ValueError(f"Invalid action_type {action['action_type']}")


def parse_decision_payload(payload: dict, strategy_type: str, llm_model_used: str | None = None, llm_input_tokens: int | None = None, llm_output_tokens: int | None = None, llm_cost_usd: float | None = None, search_cost_usd: float = 0.0) -> Decision:
    payload = _normalize_payload(payload)
    validate_decision_payload(payload)
    return Decision(
        decision_id=new_id("decision"),
        strategy_id=payload["strategy_id"],
        strategy_type=strategy_type,
        timestamp=datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00")) if isinstance(payload["timestamp"], str) else datetime.now(timezone.utc),
        markets_considered=list(payload.get("markets_considered", [])),
        predicted_probability=payload.get("predicted_probability"),
        market_implied_probability=payload.get("market_implied_probability"),
        expected_edge_bps=payload.get("expected_edge_bps"),
        confidence=payload.get("confidence"),
        evidence_items=[
            EvidenceItem(source=item["source"], content=item.get("content", item.get("detail", "")), retrieved_at=datetime.now(timezone.utc))
            for item in payload.get("evidence_items", [])
        ],
        risk_notes=payload.get("risk_notes", ""),
        exit_plan=payload.get("exit_plan", ""),
        thinking=payload.get("thinking", ""),
        web_searches_used=[
            SearchRecord(
                query=item.get("query", ""),
                results_summary=item.get("results_summary", item.get("key_findings", "")),
                source_urls=item.get("source_urls", []) if isinstance(item.get("source_urls", []), list) else [str(item.get("source_urls", ""))],
                retrieved_at=datetime.now(timezone.utc),
            )
            for item in payload.get("web_searches_used", [])
        ],
        actions=[
            ProposedAction(
                action_type=item["action_type"],
                market_id=item["market_id"],
                venue=item["venue"],
                outcome_id=item["outcome_id"],
                outcome_label=item["outcome_label"],
                amount_usd=float(item["amount_usd"]),
                limit_price=item.get("limit_price"),
                reasoning_summary=item["reasoning_summary"],
            )
            for item in payload.get("actions", [])
        ],
        no_action_reason=payload.get("no_action_reason"),
        llm_model_used=llm_model_used,
        llm_input_tokens=llm_input_tokens,
        llm_output_tokens=llm_output_tokens,
        llm_cost_usd=llm_cost_usd,
        search_queries_count=len(payload.get("web_searches_used", [])),
        search_cost_usd=search_cost_usd,
    )
