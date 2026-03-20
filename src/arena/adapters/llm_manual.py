from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json

from arena.adapters.base import LLMClient


class ManualLLMClient(LLMClient):
    def __init__(self, pending_dir: Path, responses_dir: Path) -> None:
        self.pending_dir = pending_dir
        self.responses_dir = responses_dir
        self.pending_dir.mkdir(parents=True, exist_ok=True)
        self.responses_dir.mkdir(parents=True, exist_ok=True)

    async def complete_json(
        self,
        system_prompt: str,
        user_content: str,
        json_schema: dict,
        max_tokens: int = 4096,
        temperature: float = 0.3,
        model_id: str | None = None,
    ) -> tuple[dict, int, int, float]:
        key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        prefix = self._prefix_from_model(model_id)
        prompt_path = self.pending_dir / f"{prefix}_{key}.md"
        response_path = self.responses_dir / f"{prefix}_{key}.json"
        prompt_path.write_text(f"# System\n\n{system_prompt}\n\n# User\n\n{user_content}\n", encoding="utf-8")
        if not response_path.exists():
            raise FileNotFoundError(f"Manual response file not found: {response_path}")
        return json.loads(response_path.read_text(encoding="utf-8")), 0, 0, 0.0

    async def complete_text(
        self,
        system_prompt: str,
        user_content: str,
        max_tokens: int = 4096,
        model_id: str | None = None,
    ) -> tuple[str, int, int, float]:
        payload, prompt_tokens, completion_tokens, cost = await self.complete_json(
            system_prompt=system_prompt,
            user_content=user_content,
            json_schema={},
            max_tokens=max_tokens,
            model_id=model_id,
        )
        return json.dumps(payload), prompt_tokens, completion_tokens, cost

    def _prefix_from_model(self, model_id: str | None) -> str:
        if not model_id:
            return "manual"
        if "frontier" in model_id:
            return "strategist"
        return model_id.split("/")[-1].replace("-", "_")
