from __future__ import annotations

from pathlib import Path
import os

from arena.config import ROOT


def load_local_env(root: Path | None = None) -> None:
    base = root or ROOT
    env_path = base / ".env"
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            # Use setdefault so previously-set env vars (e.g. from uvicorn --env)
            # are not overridden, but fresh starts get the .env values.
            os.environ.setdefault(key, value)
    _apply_aliases(base)


def _apply_aliases(base: Path) -> None:
    google_json = base / "google.json"
    if "GOOGLE_OAUTH_CLIENT_FILE" not in os.environ and google_json.exists():
        os.environ["GOOGLE_OAUTH_CLIENT_FILE"] = str(google_json)
