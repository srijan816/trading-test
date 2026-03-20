"""Google AI Studio adapter — Gemini models via OpenAI-compatible API."""
from __future__ import annotations

import asyncio
import json
import logging
import os

from openai import OpenAI

from arena.adapters.base import LLMClient

logger = logging.getLogger(__name__)


def get_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")
    return OpenAI(
        api_key=api_key,
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        timeout=60,
    )


def chat_completion(
    messages: list[dict],
    model: str = "gemini-3-flash-preview",
    max_tokens: int = 4096,
    temperature: float = 0.2,
):
    """Call Gemini via Google AI Studio OpenAI-compatible endpoint."""
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


class GoogleAILLMClient(LLMClient):
    def __init__(self, base_url: str | None = None, api_key_env: str = "GEMINI_API_KEY", timeout: float = 60.0) -> None:
        self.base_url = (base_url or "https://generativelanguage.googleapis.com/v1beta/openai/").rstrip("/") + "/"
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
            model=model_id or "gemini-3-flash-preview",
            max_tokens=max_tokens,
            temperature=temperature,
        )
        content = response.choices[0].message.content or ""
        prompt_tokens, completion_tokens = _usage_tokens(response)
        return content, prompt_tokens, completion_tokens, 0.0
