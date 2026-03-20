from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
import os
import re

from arena.adapters import llm_google, llm_minimax, llm_nvidia, llm_openrouter
from arena.adapters.base import LLMClient, SearchClient
from arena.adapters.weather_hko import HKOWeatherSource
from arena.adapters.weather_openmeteo import OpenMeteoSource
from arena.db import ArenaDB
from arena.intelligence.info_packet import InfoPacketBuilder
from arena.intelligence.output_parser import DECISION_JSON_SCHEMA, normalize_llm_output, parse_decision_payload
from arena.models import Decision, EvidenceItem, utc_now
from arena.risk.kelly import compute_position_size
from arena.strategies.base import Strategy

logger = logging.getLogger(__name__)

PROVIDER_MAP = {
    "minimax": llm_minimax,
    "openrouter": llm_openrouter,
    "google_ai_studio": llm_google,
    "nvidia_nim": llm_nvidia,
}

PROVIDER_ENV_KEYS = {
    "minimax": "MINIMAX_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
    "google_ai_studio": "GEMINI_API_KEY",
    "nvidia_nim": "NVIDIA_API_KEY",
}


def _extract_response_content(response) -> str | None:
    if not response or not getattr(response, "choices", None):
        return None
    message = response.choices[0].message
    content = getattr(message, "content", None)
    if isinstance(content, list):
        parts = []
        for item in content:
            text = getattr(item, "text", None) if not isinstance(item, dict) else item.get("text")
            if text:
                parts.append(text)
        content = "".join(parts)
    return content if isinstance(content, str) else None


def _extract_finish_reason(response) -> str | None:
    if not response or not getattr(response, "choices", None):
        return None
    return getattr(response.choices[0], "finish_reason", None)


def _extract_usage(response) -> tuple[int, int]:
    usage = getattr(response, "usage", None)
    if usage is None:
        return 0, 0
    prompt_tokens = getattr(usage, "prompt_tokens", 0) or 0
    completion_tokens = getattr(usage, "completion_tokens", 0) or 0
    return int(prompt_tokens), int(completion_tokens)


def call_llm(messages: list[dict], model_config: dict) -> tuple[str | None, list[dict], dict]:
    """
    Try LLM providers in order: primary → fallback → last_resort.
    model_config is the [model] section from the strategy TOML.
    Returns (content, attempts, result) tuple — no shared mutable state.
    """
    attempts_list: list[dict] = []
    result_meta: dict = {
        "provider": None,
        "model": None,
        "response": None,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "cost": 0.0,
    }

    provider_attempts = [
        (model_config.get("provider"), model_config.get("model_id")),
        (model_config.get("fallback_provider"), model_config.get("fallback_model") or model_config.get("fallback_model_id")),
        (model_config.get("last_resort_provider"), model_config.get("last_resort_model")),
    ]

    max_tokens = model_config.get("max_output_tokens", 4096)
    temperature = model_config.get("temperature", 0.2)

    for provider_name, model_id in provider_attempts:
        if not provider_name or not model_id:
            continue
        adapter = PROVIDER_MAP.get(provider_name)
        if not adapter:
            logger.warning(f"Unknown provider '{provider_name}', skipping")
            attempts_list.append({"provider": provider_name, "model": model_id, "status": "unknown_provider"})
            continue
        env_key = PROVIDER_ENV_KEYS.get(provider_name)
        if env_key and not os.environ.get(env_key):
            logger.warning(f"Skipping {provider_name}/{model_id}: missing {env_key}")
            attempts_list.append({"provider": provider_name, "model": model_id, "status": "missing_key", "env_key": env_key})
            continue
        try:
            logger.info(f"LLM call: {provider_name} / {model_id}")
            response = adapter.chat_completion(
                messages=messages,
                model=model_id,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            content = _extract_response_content(response)
            # Strip MiniMax reasoning traces — M2.7 is a thinking model
            # and sometimes includes <think>...</think> in content
            if content:
                content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
            finish_reason = _extract_finish_reason(response)
            if content and len(content.strip()) > 10:
                prompt_tokens, completion_tokens = _extract_usage(response)
                logger.info(
                    f"LLM success: {provider_name}/{model_id} "
                    f"({len(content)} chars, finish_reason={finish_reason})"
                )
                attempts_list.append(
                    {
                        "provider": provider_name,
                        "model": model_id,
                        "status": "success",
                        "finish_reason": finish_reason,
                    }
                )
                result_meta = {
                    "provider": provider_name,
                    "model": model_id,
                    "response": response,
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "cost": 0.0,
                }
                return content, attempts_list, result_meta
            logger.warning(
                f"Empty/short response from {provider_name}/{model_id}: "
                f"'{content[:50] if content else '(None)'}'"
            )
            attempts_list.append(
                {
                    "provider": provider_name,
                    "model": model_id,
                    "status": "empty",
                    "finish_reason": finish_reason,
                }
            )
        except Exception as e:
            logger.error(f"LLM error from {provider_name}/{model_id}: {e}")
            attempts_list.append(
                {
                    "provider": provider_name,
                    "model": model_id,
                    "status": "error",
                    "error": str(e),
                }
            )

    logger.error("ALL LLM providers failed for this call")
    return None, attempts_list, result_meta


class LLMStrategy(Strategy):
    def __init__(
        self,
        db: ArenaDB,
        strategy_config: dict,
        llm_client: LLMClient,
        search_client: SearchClient | None = None,
        fallback_client: LLMClient | None = None,
        fallback_model_id: str | None = None,
    ) -> None:
        super().__init__(db, strategy_config)
        self.supported_formats = (
            strategy_config.get("scope", {}).get("supported_formats")
            or strategy_config.get("supported_formats")
            or ["binary"]
        )
        self.llm_client = llm_client
        self.search_client = search_client
        self.fallback_client = fallback_client
        self.fallback_model_id = fallback_model_id
        self.packet_builder = InfoPacketBuilder(
            db,
            search_client=search_client,
            weather_sources=[HKOWeatherSource(), OpenMeteoSource()],
        )

    async def generate_decision(self):
        packet = await self.packet_builder.build(self.strategy_config, self.strategy_id)
        prompt_body, searches = self.packet_builder.render_packet(packet)
        system_prompt = self.strategy_config.get("persona", {}).get("system_prompt", "")
        parsed = None
        try:
            parsed, input_tokens, output_tokens, cost, model_used = await self._request_decision(system_prompt, prompt_body)
            parsed = normalize_llm_output(
                parsed,
                packet.get("opportunities", []),
                max_order_usd=float(self.strategy_config.get("risk", {}).get("max_order_usd", 100.0)),
            )
            parsed["timestamp"] = datetime.now(timezone.utc).isoformat()
            parsed["strategy_id"] = self.strategy_id
            is_valid, parsed, validation_error = self._validate_decision(parsed, packet)
            if not is_valid:
                self.db.record_event(
                    "llm_validation_warning",
                    {
                        "strategy_id": self.strategy_id,
                        "error": validation_error,
                        "normalized_payload": parsed,
                    },
                    strategy_id=self.strategy_id,
                )
            if parsed.get("actions") and not self.should_execute_trade():
                logger.info(
                    "LLM strategy %s generated signal but trade_enabled=false, recording as research-only",
                    self.strategy_id,
                )
                self.db.record_event(
                    "llm_signal_research_only",
                    {
                        "strategy_id": self.strategy_id,
                        "actions_blocked": len(parsed.get("actions", [])),
                        "predicted_probability": parsed.get("predicted_probability"),
                        "market_implied_probability": parsed.get("market_implied_probability"),
                        "expected_edge_bps": parsed.get("expected_edge_bps"),
                    },
                    strategy_id=self.strategy_id,
                )
                parsed["no_action_reason"] = "Research-only mode: trade_enabled=false"
                parsed["actions"] = []
            self.db.record_event(
                "llm_response_served",
                {
                    "strategy_id": self.strategy_id,
                    "served_by": model_used,
                },
                strategy_id=self.strategy_id,
            )
            return parse_decision_payload(
                parsed,
                strategy_type="llm",
                llm_model_used=model_used,
                llm_input_tokens=input_tokens,
                llm_output_tokens=output_tokens,
                llm_cost_usd=cost,
                search_cost_usd=0.0,
            )
        except Exception as exc:
            self.db.record_event(
                "llm_decision_error",
                {"strategy_id": self.strategy_id, "error": str(exc), "raw_payload": parsed},
                strategy_id=self.strategy_id,
            )
            return Decision(
                decision_id=f"decision_error_{utc_now().timestamp()}_{self.strategy_id}",
                strategy_id=self.strategy_id,
                strategy_type="llm",
                timestamp=utc_now(),
                markets_considered=[],
                predicted_probability=None,
                market_implied_probability=None,
                expected_edge_bps=None,
                confidence=0.0,
                evidence_items=[
                    EvidenceItem(
                        source="llm_error",
                        content=str(exc),
                        retrieved_at=utc_now(),
                    )
                ],
                risk_notes="LLM output parsing failed.",
                exit_plan="No action.",
                thinking=f"LLM decision failed: {exc}",
                web_searches_used=[],
                actions=[],
                no_action_reason="LLM output error; recorded as no-action for resilience.",
                llm_model_used=self.strategy_config.get("model", {}).get("model_id"),
                search_cost_usd=0.0,
            )

    async def _request_decision(self, system_prompt: str, prompt_body: str) -> tuple[dict, int, int, float, str | None]:
        model_cfg = self.strategy_config.get("model", {})
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt_body},
        ]
        content, attempts, result = await asyncio.to_thread(call_llm, messages, model_cfg)
        if content:
            cleaned = self._clean_json_text(content)
            try:
                parsed = json.loads(cleaned)
            except json.JSONDecodeError:
                repair_messages = [
                    {
                        "role": "system",
                        "content": (
                            "Convert malformed trading-decision output into one strict valid JSON object. "
                            "Return only JSON with double-quoted keys and strings."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            "Repair this output without changing its intent. It must match this schema:\n"
                            f"{json.dumps(DECISION_JSON_SCHEMA, ensure_ascii=False)}\n\n"
                            "Malformed output:\n"
                            f"{cleaned}"
                        ),
                    },
                ]
                repaired_content, repair_attempts, _repair_result = await asyncio.to_thread(call_llm, repair_messages, model_cfg)
                attempts.extend(repair_attempts)
                if repaired_content:
                    content = repaired_content
        for attempt in attempts:
            self.db.record_event(
                "llm_provider_attempt",
                {"strategy_id": self.strategy_id, "provider": attempt.get("provider"), "model": attempt.get("model")},
                strategy_id=self.strategy_id,
            )
            if attempt.get("status") in {"error", "empty", "missing_key", "unknown_provider"}:
                self.db.record_event(
                    "llm_provider_failure",
                    {
                        "strategy_id": self.strategy_id,
                        "provider": attempt.get("provider"),
                        "model": attempt.get("model"),
                        "error": attempt.get("error", attempt.get("status", "failed")),
                    },
                    strategy_id=self.strategy_id,
                )
        if not content:
            raise RuntimeError("ALL LLM providers failed for this call")
        parsed = json.loads(self._clean_json_text(content))
        return (
            parsed,
            int(result.get("prompt_tokens", 0) or 0),
            int(result.get("completion_tokens", 0) or 0),
            float(result.get("cost", 0.0) or 0.0),
            result.get("model"),
        )

    def _clean_json_text(self, content: str) -> str:
        text = content.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            text = "\n".join(lines).strip()
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end >= start:
            return text[start : end + 1]
        return text

    def _validate_decision(self, decision: dict, packet: dict) -> tuple[bool, dict, str | None]:
        errors: list[str] = []
        opportunity_map = {
            str(item.get("market_id")): item
            for item in packet.get("opportunities", [])
            if item.get("market_id")
        }
        market_categories = {
            market_id: str(item.get("category"))
            for market_id, item in opportunity_map.items()
        }
        for key in ("predicted_probability", "market_implied_probability", "confidence"):
            value = decision.get(key)
            if value is None:
                continue
            if not isinstance(value, (int, float)) or not (0.0 <= float(value) <= 1.0):
                errors.append(f"{key} out of range")
        for action in decision.get("actions", []):
            amount = action.get("amount_usd")
            if amount is None:
                continue
            action["amount_usd"] = min(max(float(amount), 10.0), float(self.strategy_config.get("risk", {}).get("max_order_usd", 100.0)))
            if not action.get("market_id"):
                errors.append("action missing market_id")
        kelly_error = self._apply_kelly_sizing(decision, opportunity_map)
        if kelly_error:
            errors.append(kelly_error)
        expected_edge_bps = int(decision.get("expected_edge_bps") or 0)
        if expected_edge_bps > 3000:
            logger.warning("Clamping %s edge from %s bps to 3000", self.strategy_id, expected_edge_bps)
            expected_edge_bps = 3000
            decision["expected_edge_bps"] = expected_edge_bps
        action_market_ids = [str(action.get("market_id")) for action in decision.get("actions", []) if action.get("market_id")]
        weather_trade = any(market_categories.get(market_id) == "weather" for market_id in action_market_ids)
        if not weather_trade:
            weather_trade = any(market_categories.get(str(market_id)) == "weather" for market_id in decision.get("markets_considered", []))
        self._anchor_weather_probability_to_ensemble(decision, opportunity_map)
        if weather_trade and expected_edge_bps > 3000:
            decision["actions"] = []
            decision["no_action_reason"] = f"Edge {expected_edge_bps} bps exceeds sanity limit for weather markets"
            errors.append("weather edge exceeds sanity limit")
        for market_id in action_market_ids:
            item = opportunity_map.get(market_id)
            question = item.get("question") if item else None
            category = market_categories.get(market_id)
            if category != "weather" or not question:
                continue
            contract = self.packet_builder._forecast_strategy._parse_weather_contract(question)
            if contract and contract.get("dated", True) and contract["forecast_date"] <= utc_now().date():
                decision["actions"] = []
                decision["no_action_reason"] = f"Weather market {market_id} is same-day or stale; forecast-only trading disabled"
                errors.append("weather market no longer eligible for forecast-based trading")
                break
        if not decision.get("actions") and (decision.get("expected_edge_bps") or 0) > 200:
            errors.append("edge_bps > 200 but no action")
        if errors:
            if not decision.get("no_action_reason"):
                decision["no_action_reason"] = "; ".join(errors)
            return False, decision, "; ".join(errors)
        return True, decision, None

    def _apply_kelly_sizing(self, decision: dict, opportunity_map: dict[str, dict]) -> str | None:
        if not decision.get("actions"):
            return None
        portfolio = self.db.get_portfolio(self.strategy_id)
        bankroll = portfolio.cash if portfolio else float(self.strategy_config.get("starting_balance", 10000.0))
        sized_actions: list[dict] = []
        rejected_reasons: list[str] = []
        sizing_cfg = self.strategy_config.get("position_sizing", {})
        yes_probability = decision.get("predicted_probability")

        if yes_probability is None:
            decision["actions"] = []
            return "Kelly sizing rejected: missing predicted_probability"

        for action in decision.get("actions", []):
            market_id = str(action.get("market_id") or "")
            opportunity = opportunity_map.get(market_id) or {}
            outcomes = opportunity.get("outcomes", []) if isinstance(opportunity.get("outcomes"), list) else []
            target_outcome = next(
                (
                    outcome for outcome in outcomes
                    if str(outcome.get("outcome_id")) == str(action.get("outcome_id"))
                ),
                None,
            )
            market_ask_price = None
            if target_outcome is not None:
                ask = target_outcome.get("best_ask")
                mid = target_outcome.get("mid_price")
                market_ask_price = float(ask if ask is not None else mid) if (ask is not None or mid is not None) else None
            if market_ask_price is None:
                rejected_reasons.append(f"missing market price for {market_id}")
                continue

            action_probability = float(yes_probability)
            if str(action.get("outcome_label") or "").lower() != "yes":
                action_probability = 1.0 - float(yes_probability)

            kelly_result = compute_position_size(
                predicted_probability=action_probability,
                market_ask_price=market_ask_price,
                bankroll=bankroll,
                kelly_fraction=float(sizing_cfg.get("kelly_fraction", os.getenv("RISK_KELLY_FRACTION_MULTIPLIER", "0.5"))),
                max_position_pct=float(sizing_cfg.get("max_position_pct", self.strategy_config.get("risk", {}).get("max_position_pct", 0.02))),
                min_position_usd=float(sizing_cfg.get("min_position_usd", os.getenv("RISK_MIN_TRADE_SIZE", "5"))),
                max_position_usd=float(sizing_cfg.get("max_position_usd", os.getenv("RISK_MAX_SINGLE_TRADE_SIZE", "50"))),
                fee_rate=float(sizing_cfg.get("fee_rate", 0.02)),
                yes_side_probability=float(yes_probability),
            )
            if kelly_result["action"] != "trade":
                rejected_reasons.append(f"{market_id}: {kelly_result['reason']}")
                continue

            sized_action = dict(action)
            sized_action["amount_usd"] = float(kelly_result["amount_usd"])
            sized_actions.append(sized_action)

        decision["actions"] = sized_actions
        if sized_actions:
            return None
        decision["no_action_reason"] = f"Kelly sizing rejected: {'; '.join(rejected_reasons) or 'no valid actions'}"
        return decision["no_action_reason"]

    def _anchor_weather_probability_to_ensemble(self, decision: dict, opportunity_map: dict[str, dict]) -> None:
        if self.strategy_id != "llm_analyst":
            return

        weather_market_id = None
        weather_outcome_label = None
        for action in decision.get("actions", []):
            market_id = str(action.get("market_id") or "")
            opportunity = opportunity_map.get(market_id)
            if opportunity and opportunity.get("category") == "weather":
                weather_market_id = market_id
                weather_outcome_label = str(action.get("outcome_label") or "").lower()
                break
        if weather_market_id is None:
            return

        signal = (opportunity_map.get(weather_market_id) or {}).get("algo_forecast_signal") or {}
        if not isinstance(signal, dict):
            return

        research_context = (opportunity_map.get(weather_market_id) or {}).get("research_context") or {}
        if isinstance(research_context, dict) and research_context.get("ensemble_override_triggered"):
            ensemble_probability = research_context.get("ensemble_probability")
            if ensemble_probability is not None:
                ensemble_probability = float(ensemble_probability)
                decision["predicted_probability"] = ensemble_probability
                market_implied = decision.get("market_implied_probability")
                if market_implied is not None:
                    decision["expected_edge_bps"] = int((ensemble_probability - float(market_implied)) * 10000)
                self.db.record_event(
                    "llm_probability_override",
                    {
                        "strategy_id": self.strategy_id,
                        "market_id": weather_market_id,
                        "ensemble_probability": round(ensemble_probability, 4),
                        "reason": "Nexus market research flagged ensemble override",
                    },
                    strategy_id=self.strategy_id,
                )
                return

        if weather_outcome_label == "no":
            ensemble_probability = signal.get("forecast_implied_no_probability")
        else:
            ensemble_probability = signal.get("forecast_implied_yes_probability")

        llm_probability = decision.get("predicted_probability")
        if ensemble_probability is None or llm_probability is None:
            return

        ensemble_probability = float(ensemble_probability)
        llm_probability = float(llm_probability)
        gap = abs(llm_probability - ensemble_probability)
        if gap <= 0.20:
            return

        logger.warning(
            "LLM probability %.2f overridden by ensemble %.2f (gap %.0fpp > 20pp max)",
            llm_probability,
            ensemble_probability,
            gap * 100.0,
        )
        self.db.record_event(
            "llm_probability_override",
            {
                "strategy_id": self.strategy_id,
                "market_id": weather_market_id,
                "llm_probability": round(llm_probability, 4),
                "ensemble_probability": round(ensemble_probability, 4),
                "gap_pp": round(gap * 100.0, 1),
                "reason": "Weather probability anchored to ensemble",
            },
            strategy_id=self.strategy_id,
        )
        decision["predicted_probability"] = ensemble_probability
        market_implied = decision.get("market_implied_probability")
        if market_implied is not None:
            decision["expected_edge_bps"] = int((ensemble_probability - float(market_implied)) * 10000)
