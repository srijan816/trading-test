"""MiniMax direct API adapter — M2.7 via OpenAI-compatible endpoint.

Uses the MiniMax Token Plan ($10/mo flat rate).
Base URL: https://api.minimax.io/v1
Auth: Bearer token from MINIMAX_API_KEY env var.
One API call = one request on the Token Plan regardless of token count.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os

from openai import OpenAI

from arena.adapters.base import LLMClient

logger = logging.getLogger(__name__)


def get_client():
    api_key = os.environ.get("MINIMAX_API_KEY")
    if not api_key:
        raise ValueError("MINIMAX_API_KEY environment variable not set")
    return OpenAI(
        api_key=api_key,
        base_url="https://api.minimax.io/v1",
        timeout=90,
    )


def chat_completion(
    messages: list[dict],
    model: str = "MiniMax-M2.7",
    max_tokens: int = 4096,
    temperature: float = 0.2,
):
    """
    Call MiniMax M2.7 via direct API.

    Note: MiniMax M2.7 is a thinking/reasoning model. The response may include
    reasoning_details if reasoning_split=True is passed. For our trading use case
    we only need the final content, so we don't enable reasoning_split.
    """
    client = get_client()
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return response


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


def _usage_tokens(response) -> tuple[int, int]:
    usage = getattr(response, "usage", None)
    if usage is None:
        return 0, 0
    prompt_tokens = getattr(usage, "prompt_tokens", 0) or 0
    completion_tokens = getattr(usage, "completion_tokens", 0) or 0
    return int(prompt_tokens), int(completion_tokens)


class MiniMaxLLMClient(LLMClient):
    def __init__(self, base_url: str | None = None, api_key_env: str = "MINIMAX_API_KEY", timeout: float = 90.0) -> None:
        self.base_url = (base_url or "https://api.minimax.io/v1").rstrip("/")
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
            max_tokens=max_tokens,
            model_id=model_id,
            temperature=temperature,
        )
        return json.loads(_clean_json_text(text)), prompt_tokens, completion_tokens, cost

    async def complete_text(
        self,
        system_prompt: str,
        user_content: str,
        max_tokens: int = 4096,
        model_id: str | None = None,
        temperature: float = 0.3,
    ) -> tuple[str, int, int, float]:
        response = await asyncio.to_thread(
            chat_completion,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            model=model_id or "MiniMax-M2.7",
            max_tokens=max_tokens,
            temperature=temperature,
        )
        content = response.choices[0].message.content or ""
        prompt_tokens, completion_tokens = _usage_tokens(response)
        return content, prompt_tokens, completion_tokens, 0.0
