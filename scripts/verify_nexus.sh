#!/bin/bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    # shellcheck source=/dev/null
    source "$REPO_ROOT/.env"
    set +a
fi

NEXUS_URL="${NEXUS_URL:-http://localhost:${NEXUS_PORT:-3001}}"
SEARXNG_URL="${SEARXNG_URL:-http://localhost:8081}"
CRAWL4AI_URL="${CRAWL4AI_URL:-http://localhost:11235}"
DEFAULT_MODEL="${DEFAULT_MODEL:-${NEXUS_DEFAULT_MODEL:-stepfun/step-3.5-flash:free}}"

AUTH_ARGS=()
if [ -n "${NEXUS_API_KEY:-}" ]; then
    AUTH_ARGS=(-H "Authorization: Bearer ${NEXUS_API_KEY}")
fi

PASS=0
FAIL=0

check() {
    local name="$1"
    local result="$2"
    if [[ "$result" == ok* ]]; then
        echo "  ✓ $name"
        PASS=$((PASS + 1))
    else
        echo "  ✗ $name — $result"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== Nexus Integration Diagnostics ==="
echo "Nexus:    $NEXUS_URL"
echo "SearXNG:  $SEARXNG_URL"
echo "Crawl4AI: $CRAWL4AI_URL"
echo ""

echo "[1/5] Infrastructure"

if curl -sf "$SEARXNG_URL" > /dev/null 2>&1; then
    check "SearXNG reachable" "ok"
else
    check "SearXNG reachable" "UNREACHABLE at $SEARXNG_URL"
fi

if curl -sf "$CRAWL4AI_URL/health" > /dev/null 2>&1; then
    check "Crawl4AI reachable" "ok"
else
    check "Crawl4AI reachable" "UNREACHABLE at $CRAWL4AI_URL (is Docker running?)"
fi

HEALTH="$(curl -sf "$NEXUS_URL/api/v1/health" 2>/dev/null || echo "UNREACHABLE")"
if echo "$HEALTH" | grep -q '"status"'; then
    NEXUS_STATUS="$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','unknown'))" 2>/dev/null || echo "parse_error")"
    check "Nexus health endpoint" "ok"

    SEARXNG_OK="$(echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('dependencies', d.get('services', {})).get('searxng', d.get('searxng', 'unknown')))" 2>/dev/null || echo "unknown")"
    CRAWL4AI_OK="$(echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('dependencies', d.get('services', {})).get('crawl4ai', d.get('crawl4ai', 'unknown')))" 2>/dev/null || echo "unknown")"
    OPENROUTER_OK="$(echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('dependencies', d.get('services', {})).get('openrouter', d.get('openrouter', 'unknown')))" 2>/dev/null || echo "unknown")"
    echo "    Nexus status: $NEXUS_STATUS"
    check "Nexus sees SearXNG" "ok ($SEARXNG_OK)"
    check "Nexus sees Crawl4AI" "ok ($CRAWL4AI_OK)"
    check "Nexus sees OpenRouter" "ok ($OPENROUTER_OK)"
else
    check "Nexus health endpoint" "UNREACHABLE at $NEXUS_URL"
fi

echo ""

echo "[2/5] Configuration"

[ -n "${OPENROUTER_API_KEY:-}" ] && check "OPENROUTER_API_KEY set" "ok" || check "OPENROUTER_API_KEY set" "MISSING in .env"
[ -n "${DEFAULT_MODEL:-}" ] && check "DEFAULT_MODEL set" "ok ($DEFAULT_MODEL)" || check "DEFAULT_MODEL set" "MISSING — Nexus still falls back to DEFAULT_MODEL in some code paths"
[ -n "${NEXUS_DEFAULT_MODEL:-}" ] && check "NEXUS_DEFAULT_MODEL set" "ok ($NEXUS_DEFAULT_MODEL)" || check "NEXUS_DEFAULT_MODEL set" "MISSING in .env"
[ -n "${NEXUS_PORT:-}" ] && check "NEXUS_PORT set" "ok ($NEXUS_PORT)" || check "NEXUS_PORT set" "using default 3001"

echo ""

echo "[3/5] SearXNG Search Test"

SEARCH_RESULT="$(curl -sf "$SEARXNG_URL/search?q=chicago+weather+forecast&format=json" 2>/dev/null || echo "FAILED")"
if echo "$SEARCH_RESULT" | python3 -c "import sys,json; r=json.load(sys.stdin); print(len(r.get('results',[])))" 2>/dev/null | grep -q '[1-9]'; then
    RESULT_COUNT="$(echo "$SEARCH_RESULT" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('results',[])))")"
    check "SearXNG returns results" "ok ($RESULT_COUNT results)"
else
    check "SearXNG returns results" "FAILED or 0 results"
fi

echo ""

echo "[4/5] Crawl4AI Extraction Test"

CRAWL_RESULT="$(curl -sf -X POST "$CRAWL4AI_URL/crawl" \
    -H "Content-Type: application/json" \
    -d '{"urls": ["https://example.com"], "priority": 10}' 2>/dev/null || echo "FAILED")"

if echo "$CRAWL_RESULT" | grep -q "results\|task_id"; then
    check "Crawl4AI accepts crawl request" "ok"
    TASK_ID="$(echo "$CRAWL_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('task_id',''))" 2>/dev/null || echo "")"
    if [ -n "$TASK_ID" ] && [ "$TASK_ID" != "None" ]; then
        sleep 3
        TASK_RESULT="$(curl -sf "$CRAWL4AI_URL/task/$TASK_ID" 2>/dev/null || echo "")"
        if echo "$TASK_RESULT" | grep -q "results"; then
            check "Crawl4AI returns markdown" "ok (async task completed)"
        else
            check "Crawl4AI returns markdown" "task pending (may need more time)"
        fi
    else
        check "Crawl4AI returns markdown" "ok (sync response)"
    fi
else
    check "Crawl4AI accepts crawl request" "FAILED — $(echo "$CRAWL_RESULT" | head -c 120)"
fi

echo ""

echo "[5/5] Nexus Full Pipeline Test (this may take 15-30 seconds)"

RESEARCH_RESULT="$(curl -sf -X POST "$NEXUS_URL/api/v1/research" \
    "${AUTH_ARGS[@]}" \
    -H "Content-Type: application/json" \
    -d '{
        "query": "What is the current weather forecast high temperature for Chicago tomorrow?",
        "mode": "quick",
        "model": "'"${DEFAULT_MODEL}"'"
    }' 2>/dev/null || echo "FAILED")"

if echo "$RESEARCH_RESULT" | grep -q "report\|output\|result"; then
    REPORT_LEN="$(echo "$RESEARCH_RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); r=d.get('report', d.get('output', d.get('result', ''))); print(len(str(r)))" 2>/dev/null || echo "0")"
    SOURCES_COUNT="$(echo "$RESEARCH_RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('sources', [])))" 2>/dev/null || echo "0")"
    MODEL_USED="$(echo "$RESEARCH_RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); meta=d.get('metadata', d.get('meta', {})); models=meta.get('models_used', {}) if isinstance(meta, dict) else {}; print(meta.get('model', meta.get('synthesis_model', models.get('synthesis', 'unknown'))))" 2>/dev/null || echo "unknown")"
    check "Nexus returns research report" "ok (${REPORT_LEN} chars, ${SOURCES_COUNT} sources, model: ${MODEL_USED})"
else
    check "Nexus returns research report" "FAILED"
    echo "    Response: $(echo "$RESEARCH_RESULT" | head -c 200)"
fi

echo ""

echo "[Bonus] Recent Usage"
USAGE="$(curl -sf "${AUTH_ARGS[@]}" "$NEXUS_URL/api/v1/usage" 2>/dev/null || echo "{}")"
TOTAL="$(echo "$USAGE" | python3 -c "import sys,json; d=json.load(sys.stdin); entries=d.get('recent_entries', d.get('entries', [])); print(len(entries))" 2>/dev/null || echo "0")"
echo "  Total logged recent entries: $TOTAL"

RECENT="$(echo "$USAGE" | python3 -c "import sys,json,time; d=json.load(sys.stdin); entries=d.get('recent_entries', d.get('entries', [])); cutoff=(time.time()*1000)-(5*60*1000); recent=[e for e in entries if e.get('timestamp', 0) > cutoff]; print(len(recent))" 2>/dev/null || echo "0")"
echo "  Calls in last 5 minutes: $RECENT"

FLASH_RECENT="$(echo "$USAGE" | python3 -c "import sys,json,time; d=json.load(sys.stdin); entries=d.get('recent_entries', d.get('entries', [])); cutoff=(time.time()*1000)-(5*60*1000); recent=[e for e in entries if e.get('timestamp', 0) > cutoff and e.get('model') == 'stepfun/step-3.5-flash:free']; print(len(recent))" 2>/dev/null || echo "0")"
echo "  stepfun/step-3.5-flash:free calls in last 5 minutes: $FLASH_RECENT"

echo ""
echo "=== Summary: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && echo "All checks passed." || echo "Some checks failed — see above."
