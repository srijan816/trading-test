from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from arena.config import ROOT


def render_prompt(template_name: str, **context: object) -> str:
    env = Environment(
        loader=FileSystemLoader(str(ROOT / "prompts")),
        autoescape=select_autoescape(default_for_string=False),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template(template_name)
    return template.render(**context)
