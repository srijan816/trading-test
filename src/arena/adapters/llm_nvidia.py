from __future__ import annotations

import asyncio
import logging
from os import getenv
import json
import sys
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


def chat_completion(
    messages: list[dict],
    model: str = "nvidia/nemotron-3-super-120b-a12b",
    max_tokens: int = 4096,
    temperature: float = 0.2,
):
    api_key = getenv("NVIDIA_API_KEY")
    if not api_key:
        raise RuntimeError("Missing NVIDIA_API_KEY")
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max(max_tokens, 4096),
    }
    with httpx.Client(timeout=60.0) as client:
        response = client.post(
            "https://integrate.api.nvidia.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
        )
        raw_text = response.text
    print(
        f"NVIDIA raw status={response.status_code}, body_len={len(raw_text)}, first_500={raw_text[:500]!r}",
        file=sys.stderr,
        flush=True,
    )
    if response.status_code != 200:
        raise RuntimeError(f"NVIDIA returned {response.status_code}: {raw_text[:300]}")
    try:
        raw = response.json()
    except Exception as exc:
        raise RuntimeError(f"NVIDIA returned non-JSON body: {exc}") from exc
    usage = raw.get("usage", {})
    choices = raw.get("choices") or []
    if not choices:
        raise RuntimeError("NVIDIA returned no choices")
    choice = choices[0]
    message = choice.get("message", {})
    content = message.get("content", "")
    reasoning = message.get("reasoning_content", "")
    if isinstance(content, list):
        text_parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text_parts.append(item.get("text", ""))
        content = "".join(text_parts)
    if isinstance(reasoning, list):
        reasoning_parts = []
        for item in reasoning:
            if isinstance(item, dict) and item.get("type") == "text":
                reasoning_parts.append(item.get("text", ""))
            elif isinstance(item, str):
                reasoning_parts.append(item)
        reasoning = "".join(reasoning_parts)
    if not content or not str(content).strip():
        finish_reason = choice.get("finish_reason")
        reasoning_len = len(reasoning) if isinstance(reasoning, str) else 0
        print(
            f"NVIDIA empty content warning: finish_reason={finish_reason}, reasoning_len={reasoning_len}",
            file=sys.stderr,
            flush=True,
        )
        raise RuntimeError(f"NVIDIA returned empty content (finish_reason={finish_reason})")
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(content=str(content)),
                finish_reason=choice.get("finish_reason"),
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=usage.get("prompt_tokens", 0) or 0,
            completion_tokens=usage.get("completion_tokens", 0) or 0,
        ),
        _payload=raw,
    )


class NvidiaLLMClient(LLMClient):
    def __init__(self, base_url: str, api_key_env: str = "NVIDIA_API_KEY", timeout: float = 60.0) -> None:
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
        text, prompt_tokens, completion_tokens, cost = await self.complete_text(
            system_prompt=system_prompt,
            user_content=user_content,
            max_tokens=max(max_tokens, 4096),
            temperature=temperature,
            model_id=model_id,
        )
        return json.loads(_clean_json_text(text)), prompt_tokens, completion_tokens, cost

    async def complete_text(
        self,
        system_prompt: str,
        user_content: str,
        max_tokens: int = 4096,
        temperature: float = 0.3,
        model_id: str | None = None,
    ) -> tuple[str, int, int, float]:
        response = await asyncio.to_thread(
            chat_completion,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            model=model_id or "nvidia/nemotron-3-super-120b-a12b",
            max_tokens=max_tokens,
            temperature=temperature,
        )
        usage = getattr(response, "usage", None)
        prompt_tokens = getattr(usage, "prompt_tokens", 0) if usage else 0
        completion_tokens = getattr(usage, "completion_tokens", 0) if usage else 0
        content = response.choices[0].message.content or ""
        return str(content), int(prompt_tokens or 0), int(completion_tokens or 0), 0.0
