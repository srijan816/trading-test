from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json

from arena.config import ROOT
from arena.intelligence.prompts import render_prompt


def generate_monthly_prompt(payload: dict, prompts_dir: Path | None = None) -> Path:
    rendered = render_prompt("monthly_meta.md.j2", month=datetime.now(timezone.utc).strftime("%Y-%m"), payload=json.dumps(payload, indent=2, sort_keys=True))
    directory = prompts_dir or (ROOT / "prompts" / "pending")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"meta_{datetime.now(timezone.utc).strftime('%Y-%m')}.md"
    path.write_text(rendered, encoding="utf-8")
    return path
