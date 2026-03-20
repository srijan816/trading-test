#!/usr/bin/env python3
"""Probe a local Perplexia / Nexus research API."""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from arena.adapters.search_perplexia import PerplexiaSearchClient
from arena.env import load_local_env


async def main() -> int:
    load_local_env()
    client = PerplexiaSearchClient(base_url=os.environ.get("PERPLEXIA_BASE_URL", "http://localhost:3001/api/v1"))
    query = os.environ.get(
        "PERPLEXIA_PROBE_QUERY",
        "What are the three most important developments in prediction markets this week?",
    )
    print("Perplexia probe")
    print(f"base_url={client.base_url}")
    print(f"query={query}")
    try:
        results = await client.search(query, num_results=3)
    except Exception as exc:
        print(f"FAILED: {exc}")
        return 1
    print(f"OK: {len(results)} result(s)")
    for idx, result in enumerate(results, start=1):
        print(f"{idx}. {result.title}")
        print(f"   url: {result.url}")
        print(f"   snippet: {result.snippet[:180]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
