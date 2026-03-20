from __future__ import annotations

import asyncio
import logging
from os import getenv
import json
from types import SimpleNamespace

import httpx

from arena.adapters.base import LLMClient

logger = logging.getLogger(__name__)


def _clean_json_text(content: str) -> str:
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


def _extract_content(payload: dict) -> str:
    if "choices" not in payload:
        raise RuntimeError(f"OpenRouter response missing choices: {json.dumps(payload)[:1000]}")
    message = payload["choices"][0]["message"]
    content = message.get("content", "")
    if content is None:
        raise RuntimeError(
            "OpenRouter returned no message content. "
            f"finish_reason={payload['choices'][0].get('finish_reason')} "
            f"reasoning_present={bool(message.get('reasoning'))}"
        )
    if isinstance(content, list):
        text_parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text_parts.append(item.get("text", ""))
        content = "".join(text_parts)
    if not isinstance(content, str):
        raise RuntimeError(f"Unexpected OpenRouter content shape: {json.dumps(payload)[:1000]}")
    return content


def _to_response(payload: dict):
    content = _extract_content(payload)
    choice = payload["choices"][0]
    usage = payload.get("usage", {})
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(content=content),
                finish_reason=choice.get("finish_reason"),
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=usage.get("prompt_tokens", 0) or 0,
            completion_tokens=usage.get("completion_tokens", 0) or 0,
        ),
        _payload=payload,
    )


def chat_completion(
    messages: list[dict],
    model: str = "minimax/minimax-m2.7",
    max_tokens: int = 4096,
    temperature: float = 0.2,
):
    api_key = getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENROUTER_API_KEY")
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "reasoning": {
            "exclude": True,
            "effort": "medium",
        },
    }
    with httpx.Client(timeout=45.0) as client:
        response = client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
        )
        response.raise_for_status()
        raw = response.json()
    return _to_response(raw)


class OpenRouterLLMClient(LLMClient):
    def __init__(self, base_url: str, api_key_env: str = "OPENROUTER_API_KEY", timeout: float = 45.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key_env = api_key_env
        self.timeout = timeout

    async def complete_json(
        self,
        system_prompt: str,
        user_content: str,
        json_schema: dict,
        max_tokens: int = 4096,
        temperature: float = 0.3,
        model_id: str | None = None,
    ) -> tuple[dict, int, int, float]:
        schema_hint = (
            "Return only a valid JSON object. Do not use markdown fences. "
            "Use these top-level keys exactly: "
            "timestamp, strategy_id, markets_considered, predicted_probability, market_implied_probability, "
            "expected_edge_bps, confidence, evidence_items, risk_notes, exit_plan, thinking, "
            "web_searches_used, actions, no_action_reason. "
            "For each action, use action_type as BUY, SELL, or HOLD. "
            "Keep string values concise and properly escaped."
        )
        try:
            payload = await self._complete(
                system_prompt=system_prompt,
                user_content=f"{user_content}\n\n{schema_hint}",
                max_tokens=max_tokens,
                temperature=temperature,
                model_id=model_id,
                response_format={"type": "json_object"},
            )
            content = _extract_content(payload)
            parsed = json.loads(_clean_json_text(content))
            usage = payload.get("usage", {})
            return parsed, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0), payload.get("usage", {}).get("cost", 0.0)
        except Exception:
            fallback_prompt = (
                f"{user_content}\n\n"
                f"{schema_hint}\n"
                "If there are no actions, return an empty actions array and a no_action_reason."
            )
            payload = await self._complete(
                system_prompt=system_prompt,
                user_content=fallback_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
                model_id=model_id,
                response_format=None,
            )
            content = _extract_content(payload)
            parsed = json.loads(_clean_json_text(content))
            usage = payload.get("usage", {})
            return parsed, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0), payload.get("usage", {}).get("cost", 0.0)

    async def complete_text(
        self,
        system_prompt: str,
        user_content: str,
        max_tokens: int = 4096,
        model_id: str | None = None,
    ) -> tuple[str, int, int, float]:
        payload = await self._complete(system_prompt, user_content, max_tokens, 0.3, model_id, None)
        content = _extract_content(payload)
        usage = payload.get("usage", {})
        return content, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0), payload.get("usage", {}).get("cost", 0.0)

    async def _complete(
        self,
        system_prompt: str,
        user_content: str,
        max_tokens: int,
        temperature: float,
        model_id: str | None,
        response_format: dict | None,
    ) -> dict:
        api_key = getenv(self.api_key_env)
        if not api_key:
            raise RuntimeError(f"Missing {self.api_key_env}")
        effective_max_tokens = max(int(max_tokens), 16000)
        payload = {
            "model": model_id or "minimax/minimax-m2.7",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            "temperature": temperature,
            "max_tokens": effective_max_tokens,
            "reasoning": {
                "exclude": True,
                "effort": "medium",
            },
        }
        if response_format:
            payload["response_format"] = response_format
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
            )
            response.raise_for_status()
            return response.json()
