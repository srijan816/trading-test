from __future__ import annotations

from typing import Iterable


def render_table(rows: list[dict]) -> str:
    if not rows:
        return "No rows."
    headers = list(rows[0].keys())
    widths = {header: max(len(header), *(len(str(row.get(header, ""))) for row in rows)) for header in headers}
    lines = [" | ".join(header.ljust(widths[header]) for header in headers)]
    lines.append("-+-".join("-" * widths[header] for header in headers))
    for row in rows:
        lines.append(" | ".join(str(row.get(header, "")).ljust(widths[header]) for header in headers))
    return "\n".join(lines)
