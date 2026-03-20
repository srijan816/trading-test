#!/usr/bin/env python3
"""
Verify that the Arena trading system can reach Nexus and get useful research.
Tests the actual code path that strategies use.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import sys
import time

REPO_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"
if (
    VENV_PYTHON.exists()
    and Path(sys.executable).resolve() != VENV_PYTHON.resolve()
    and os.environ.get("ARENA_VERIFY_NEXUS_REEXEC") != "1"
):
    os.environ["ARENA_VERIFY_NEXUS_REEXEC"] = "1"
    os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), __file__, *sys.argv[1:]])

SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
for site_packages in sorted((REPO_ROOT / ".venv" / "lib").glob("python*/site-packages")):
    if str(site_packages) not in sys.path:
        sys.path.insert(0, str(site_packages))


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


async def main() -> None:
    load_env(REPO_ROOT / ".env")
    print("=== Arena -> Nexus Integration Test ===\n")

    try:
        from arena.adapters.search_perplexia import PerplexiaSearchClient

        print("✓ PerplexiaSearchClient imports successfully")
    except Exception as exc:
        print(f"✗ search_perplexia import failed: {exc}")
        return

    try:
        from arena.intelligence.info_packet import InfoPacketBuilder

        print("✓ InfoPacketBuilder imports successfully")
    except Exception as exc:
        print(f"✗ InfoPacketBuilder import failed: {exc}")

    try:
        from arena.intelligence.nexus_types import MarketResearchRequest, MarketResearchResponse

        req = MarketResearchRequest(
            question="Will Chicago high be above 60F tomorrow?",
            market_type="weather",
        )
        _ = MarketResearchResponse(probability=0.5, confidence="medium", reasoning="ok")
        print(f"✓ nexus_types works: {req.question[:50]}...")
    except Exception as exc:
        print(f"✗ nexus_types import failed: {exc}")

    try:
        import httpx

        nexus_url = os.getenv("NEXUS_URL", f"http://localhost:{os.getenv('NEXUS_PORT', '3001')}")
        timeout = httpx.Timeout(90.0, connect=5.0)
        headers = {}
        if os.getenv("NEXUS_API_KEY"):
            headers["Authorization"] = f"Bearer {os.environ['NEXUS_API_KEY']}"

        async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
            health_resp = await client.get(f"{nexus_url}/api/v1/health")
            health_resp.raise_for_status()
            health = health_resp.json()
            print(f"✓ Nexus health: {health.get('status', 'unknown')}")

            start = time.time()
            research_resp = await client.post(
                f"{nexus_url}/api/v1/research",
                json={
                    "query": "Chicago weather forecast high temperature tomorrow",
                    "mode": "quick",
                },
            )
            research_resp.raise_for_status()
            result = research_resp.json()
            elapsed = time.time() - start

            report = result.get("report", result.get("output", ""))
            sources = result.get("sources", [])
            print(f"✓ Research completed in {elapsed:.1f}s")
            print(f"  Report length: {len(str(report))} chars")
            print(f"  Sources: {len(sources)}")
            print(f"  First 200 chars: {str(report)[:200]}...")

            try:
                adapter = PerplexiaSearchClient(mode="quick", output_length="short", max_sources=5, timeout=180.0)
                market_start = time.time()
                market_result = await adapter.market_research(
                    {
                        "question": "Will Chicago high be above 60F tomorrow?",
                        "market_type": "weather",
                        "market_data": {"current_price_yes": 0.42, "current_price_no": 0.58},
                        "ensemble_data": {"mu": 16.5, "sigma": 1.8},
                        "search_depth": "quick",
                    }
                )
                market_elapsed = time.time() - market_start
                print(f"✓ Arena adapter market_research completed in {market_elapsed:.1f}s")
                print(
                    "  Structured keys: "
                    + ", ".join(sorted(k for k in market_result.keys() if k in {"probability", "confidence", "reasoning", "edge_assessment", "sources", "model_used", "tokens_used"}))
                )
                print(f"  Probability: {market_result.get('probability')}")
                print(f"  Model used: {market_result.get('model_used', 'unknown')}")
                print(f"  Tokens used: {json.dumps(market_result.get('tokens_used', {}))}")
            except Exception as structured_exc:
                print(f"✗ Arena adapter market_research failed: {structured_exc!r}")

    except Exception as exc:
        print(f"✗ Nexus HTTP call failed: {exc!r}")

    print("\n=== Done ===")


if __name__ == "__main__":
    asyncio.run(main())
