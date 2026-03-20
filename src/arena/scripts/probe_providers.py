#!/usr/bin/env python3
"""Probe all LLM providers to verify connectivity and response quality."""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from arena.adapters import llm_google, llm_minimax, llm_nvidia, llm_openrouter
from arena.env import load_local_env

load_local_env()

TEST_MSG = [{"role": "user", "content": 'Reply with exactly this JSON and nothing else: {"status":"ok","model":"your-name"}'}]

PROVIDERS = [
    ("MiniMax Direct → M2.7", llm_minimax, "MiniMax-M2.7"),
    ("Google AI Studio → Gemini 3 Flash", llm_google, "gemini-3-flash-preview"),
    ("NVIDIA NIM → Nemotron-3 Super", llm_nvidia, "nvidia/nemotron-3-super-120b-a12b"),
    ("OpenRouter → M2.7 (fallback)", llm_openrouter, "minimax/minimax-m2.7"),
]

print("=" * 60)
print("ARENA LLM PROVIDER PROBE")
print("=" * 60)

results = []
for name, adapter, model_id in PROVIDERS:
    print(f"\n--- {name} ---")
    print(f"    Model: {model_id}")
    start = time.time()
    try:
        resp = adapter.chat_completion(
            messages=TEST_MSG,
            model=model_id,
            max_tokens=100,
            temperature=0.0,
        )
        content = resp.choices[0].message.content
        elapsed = time.time() - start
        finish = resp.choices[0].finish_reason
        print(f"    ✓ OK  ({elapsed:.2f}s, finish_reason={finish})")
        print(f"    Response: {content[:200]}")
        results.append((name, "OK", elapsed))
    except Exception as e:
        elapsed = time.time() - start
        print(f"    ✗ FAILED  ({elapsed:.2f}s)")
        print(f"    Error: {e}")
        results.append((name, "FAIL", elapsed))

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
for name, status, elapsed in results:
    icon = "✓" if status == "OK" else "✗"
    print(f"  {icon} {name}: {status} ({elapsed:.2f}s)")

failed = [r for r in results if r[1] != "OK"]
if failed:
    print(f"\n⚠ {len(failed)} provider(s) failed. Check API keys.")
else:
    print(f"\n✓ All {len(results)} providers responding. Ready to trade.")
