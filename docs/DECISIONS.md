# Decisions

This file records architecture and implementation decisions as they exist in the current codebase. When git history is available, entries should cite the actual change set; in this workspace, the repository history was not available, so the notes below are summarized from the checked-in code state on 2026-03-20.

## 2026-03-21 — Phase 3A: weather question parsing regression coverage locked to live market shapes
Changed: `src/tests/test_strategies.py`
What: Audited all active weather markets in SQLite against `parse_weather_question()` and confirmed zero live parse misses across 528 active rows. Since the parser already covered the current production shapes, the improvement for this phase was to add direct regression coverage for the contract patterns that matter most to paper trading: Fahrenheit and Celsius temperatures, exact thresholds, `or higher`, `or below`, bracket ranges, rain markets, and lowest-temperature wording. This turns the parser from “currently working” into “guarded against silent regressions” before the maker-pricing work proceeds.
Status: Working. `py_compile` passed and the regression cases were executed through a direct assertion harness; `pytest` is configured in project metadata but not installed in the current venv.
Next: Improve `compute_limit_price()` so maker quotes adapt to book spread, edge strength, and displayed size instead of relying on a fixed offset.

## 2026-03-21 — Phase 3B: maker quote placement now adapts to spread, edge, and displayed size
Changed: `src/arena/engine/limit_order_manager.py`, `src/tests/test_limit_order_manager.py`
What: Replaced the old fixed-offset maker quote rule with a spread-aware placement policy. Quotes now start 30% of the way into the spread from our side, then become more aggressive when the model’s edge over the current midpoint is stronger and when top-of-book displayed size is deeper. The implementation still respects tick size, never crosses the spread, and preserves the post-fee minimum edge floor before allowing an order. Added direct regression coverage for empty books, crossed quotes, narrow spreads, size-driven aggressiveness, edge-driven aggressiveness, and edge-floor rejection.
Status: Working. `py_compile` passed and the limit-price regression cases were exercised through a direct assertion harness; `pytest` remains unavailable in the active venv.
Next: Extend the order-monitor loop with richer lifecycle handling and metrics, especially repricing visibility, fill-rate tracking, and midpoint-vs-fill slippage.

## 2026-03-21 — Phase 3C: partial fills now merge cleanly and the monitor logs fill-quality metrics
Changed: `src/arena/engine/limit_order_manager.py`, `src/arena/db.py`, `src/arena/main.py`, `src/tests/test_limit_order_manager.py`
What: Hardened the maker-order lifecycle in three places. First, repeated partial fills on the same order now merge into a single open position with a weighted-average entry price instead of creating duplicate position rows. Second, fill booking records midpoint-at-fill context and stores midpoint-vs-fill improvement so the monitor can report actual maker quality instead of just raw fills. Third, the 30-second monitor loop now emits summary metrics for recent fill rate, average time to fill, and midpoint slippage alongside status transitions and reprices. Reprice operations also write an explicit `repriced` order event that links the original order to its replacement price.
Status: Working. `py_compile` passed and a direct temp-DB regression harness confirmed that two partial fills collapse into one position with the correct quantity, weighted average, and portfolio cash impact.
Next: Start `3D` by defining paper-only exit logic so the system can manage open positions instead of only accumulating them into resolution.

## 2026-03-21 — Phase 3D: paper-only position manager closes weather trades on reversal, stop loss, and late-stage decay
Changed: `src/arena/main.py`, `src/arena/scheduler.py`, `src/tests/test_position_management.py`
What: Added a paper-only weather position manager that scans open positions every 15 minutes and closes them when one of three conditions hits: the hold edge reverses materially against the position, the mark-to-bid loss breaches a stop-loss threshold, or time-to-resolution is short and little edge remains. This is implemented as a separate management loop instead of sell-side maker execution so the system can start managing risk immediately without pretending the long-only maker ledger is already a full two-sided execution engine. Exit signals are unit-tested directly, and closures are written as `SELL` executions plus `position_exit` events.
Status: Working at the paper-management layer. `py_compile` passed and the exit-signal cases were exercised through a direct assertion harness.
Next: Move into `3E` to accelerate calibration feedback, especially city/metric-specific sigma updates after each settlement.

## 2026-03-21 — Paper-only maker execution wired into the scheduler with public Polymarket orderbooks
Changed: `src/arena/main.py`, `src/arena/scheduler.py`, `src/arena/engine/paper_limit_executor.py`, `src/arena/engine/limit_order_manager.py`, `src/arena/exchanges/polymarket_limit.py`, `src/arena/strategies/llm_strategy.py`, `src/arena/dashboard/app.py`, `.env.example`, `pyproject.toml`, `requirements.txt`
What: Integrated the existing maker-order scaffold into the live scheduler path without enabling authenticated trading. `EXECUTION_MODE=paper_limit` now routes Polymarket actions into the resting-order engine, while `live_limit` is explicitly downgraded to paper mode with a logged guard. Added a 30-second `monitor_limit_orders` scheduler job that runs fill checks, books fills, and reprices stale orders. Weather executions now pass through `ConfidenceGate` before any order is submitted, and LLM strategies apply a small post-parse confidence/edge boost when discovery packets include breaking signals. Replaced the old placeholder Polymarket limit client with a read-only public CLOB reader that fetches real orderbooks and tick sizes for paper simulation only. Both `main.py` and the dashboard now call `load_dotenv()` when available, while falling back to the repo’s existing `.env` loader if `python-dotenv` is not installed in the runtime.
Status: Working in paper-only mode. Public orderbook and tick-size reads verified against Polymarket; full source tree `py_compile` passed.
Next: Improve maker quote selection, expand question parsing/test coverage, and add integration tests around the new order-monitor loop before considering any live execution work again.

## 2026-03-21 — Maker limit-order engine scaffolded for Polymarket paper trading
Changed: `src/arena/engine/limit_order_manager.py` (new), `src/arena/engine/paper_limit_executor.py` (new), `src/arena/engine/order_types.py` (new), `src/arena/engine/order_schema.py` (new), `src/arena/exchanges/polymarket_limit.py` (new), `src/arena/db.py`, `config/arena.toml`, `.env.example`
What: Added a maker-oriented limit order lifecycle alongside the existing taker-only paper executor. The new manager persists `limit_orders` and `order_events`, computes inside-spread maker prices with a post-fee edge floor, tracks status transitions (`pending/open/stale/partial/filled/cancelled/expired`), and books buy-side paper fills back through the existing portfolio/execution tables. Paper mode now has a realistic resting-order simulator with TTL, stale detection, randomized fill delays, and partial-fill support. Live Polymarket maker execution remains explicitly disabled behind a placeholder CLOB client until authenticated order submission is implemented and verified.
Status: Working as a standalone engine component. Not yet wired into `main.py`, which remains on the existing taker path by design.
Next: Integrate the manager into the scheduler/execution flow, add sell-side portfolio accounting if short inventory becomes part of the strategy set, and replace the live CLOB placeholder with a signed Polymarket implementation.

## 2026-03-21 — Dashboard upgraded for cross-system operational visibility
Changed: `src/arena/dashboard/app.py`, `src/arena/dashboard/queries.py`, `src/arena/dashboard/templates/base.html`, `src/arena/dashboard/templates/partials/overview_page.html`, `src/arena/dashboard/templates/partials/performance_page.html`, `src/arena/dashboard/templates/partials/calibration_page.html`, `src/arena/dashboard/templates/partials/research_pipeline_page.html`, `src/arena/dashboard/templates/partials/execution_funnel_page.html` (new), `src/arena/dashboard/templates/partials/orders_page.html` (new), `src/arena/dashboard/templates/partials/discovery_page.html` (new), `src/arena/dashboard/static/style.css`
What: Reworked the dashboard around a trader-facing operating view. The overview now surfaces real-time P&L, weather-strategy status, execution-funnel readiness, research spend, and recent activity in one screen. Added dedicated `/execution-funnel`, `/orders`, and `/discovery` pages with graceful fallback handling when agent-owned tables are missing. The calibration view now reads CRPS/Brier JSONL history per city and renders inline SVG ratio charts with a hard tradeability cutoff. Strategy performance now compares enabled strategies side by side with weather-only and maker-readiness context, and the research pipeline now estimates model spend and ROI directly from `research_log`.
Status: Working in read-only mode against SQLite/JSONL inputs. Order and discovery pages intentionally degrade to “not yet active” messaging until their upstream agents begin persisting rows.
Next: Once `execution_gate_*`, `limit_orders`, `order_events`, and `discovery_alerts` are flowing in production, tighten the funnel math and signal ROI joins around the final upstream schemas instead of fallback heuristics.

## 2026-03-21 — Strategy layer re-centered on weather edge
Changed: `src/arena/strategies/algo_forecast.py`, `src/arena/strategies/algo_partition.py`, `src/arena/strategies/llm_strategy.py`, `src/arena/strategies/base.py`, `src/arena/data_sources/weather_constants.py`, `config/strategies/algo_forecast.toml`, `config/strategies/algo_harvester.toml`, `config/strategies/algo_partition.toml`, `config/strategies/llm_analyst.toml`, `config/strategies/llm_news_trader.toml`
What: Reworked the strategy layer around the trading review conclusion that weather is the only durable edge. `algo_forecast` now parses broader weather market wording, resolves more city names, ranks opportunities by edge weighted by liquidity, enforces a dedicated minimum market-volume gate, and discounts edge as resolution approaches. `algo_partition` stays active for weather numeric brackets but now groups contracts by parsed weather dimensions, rejects malformed or stale baskets before execution, and ranks bracket opportunities with the same time-decay logic. `algo_harvester` was disabled because a 30-minute late-stage scan cannot compete with colocated bots. `llm_analyst` and `llm_news_trader` remain enabled for research visibility, but `trade_enabled=false` now converts any generated trade signal into research-only output so no capital is deployed from LLM probability synthesis.
Status: Working pending compile/config verification in the current workspace.
Next: Monitor whether the broader city coverage materially increases weather opportunity throughput, and consider adding precipitation-specific ensemble support before activating rain-market trading.

## 2026-03-20 — FIX 1: Settlement pipeline now resolves weather markets locally
Changed: `src/arena/main.py` (`poll_resolutions`)
What: `poll_resolutions()` now queries both `status='active'` AND `status='resolved'` markets with `end_time < now()`. Weather markets are resolved locally using observed temperatures from `station_observations.py` + `CITY_COORDS`, comparing actual highs against contract thresholds. Non-weather markets still use Polymarket/Kalshi API. Markets overdue by 24h+ without API resolution are logged as warnings. Settlement fires the existing calibration hook (`resolution_hook.py`).
Status: Working.
Next: Grow resolved sample size; monitor city-level calibration drift.

## 2026-03-20 — FIX 2: Categorization rewrite with scoring system
Changed: `src/arena/categorization.py`, `src/arena/db.py`, `scripts/recategorize_markets.py`
What: Replaced first-match keyword lookup with a scoring system that counts keyword hits per category and picks the highest scorer. Added 9 expanded keyword dictionaries (150+ sports terms including team names, prop-bet regex patterns), `secondary_category` column, and `categorize_market_detailed()` returning `(primary, secondary)`. Created `scripts/recategorize_markets.py` for batch re-categorization with before/after distribution reporting.
Status: Working.
Next: Monitor categorization accuracy on new market ingestion.

## 2026-03-20 — FIX 3: Research enabled for LLM Analyst strategy
Changed: `config/strategies/llm_analyst.toml`
What: Set `search.enabled = true`, added `research_assistant_enabled = true` and `research_mode = "standard"`. The LLM Analyst now calls Nexus/Perplexia research for weather markets, incorporating ensemble data into the synthesis layer.
Status: Working.
Next: Monitor research log entries and Nexus call volume.

## 2026-03-20 — MiniMax direct synthesis fix
Changed: `services/nexus/src/lib/minimax-client.ts` (new), `services/nexus/src/market-research.ts`  
What: Direct MiniMax API bypasses OpenAI SDK parsing bug.  
Status: Working. Live test confirmed.  
Next: Add reasoning trace capture.

## 2026-03-20 — Research Pipeline dashboard
Changed: `src/arena/dashboard/app.py`, `src/arena/dashboard/queries.py`, research templates  
What: New `/research-pipeline` page, `research_log` table, live logging.  
Status: Working. Integrated in nav.

## 2026-03-20 — System audit
What: Full diagnostic found 5 critical, 8 high, 7 medium issues.  
Status: Fixes in progress.

## 2026-03-20 — Structured market research moved behind Nexus
Changed: `src/arena/intelligence/research.py`, `src/arena/intelligence/info_packet.py`, `services/nexus/src/market-research.ts`  
What: Arena now sends a structured market payload to Nexus instead of relying only on ad hoc search summaries. The response includes probability, confidence, edge assessment, sources, and model metadata.  
Status: Working for the main research path.  
Next: Keep Python and TypeScript response types aligned.

## 2026-03-20 — Weather ensemble anchored the weather research path
Changed: `src/arena/data_sources/weather_ensemble.py`, `src/arena/intelligence/info_packet.py`, `services/nexus/src/market-research.ts`  
What: Weather markets pass ensemble mean/sigma/threshold context into Nexus, and the synthesis layer clamps large deviations back toward the ensemble probability.  
Status: Working with override protection enabled.  
Next: Continue validating calibration against resolved weather markets.

## 2026-03-20 — Settlement now feeds calibration scoring
Changed: `src/arena/engine/settlement.py`, `src/arena/calibration/resolution_hook.py`, `src/arena/calibration/crps_tracker.py`  
What: Settlement hooks compute real resolved outcomes and feed both Brier scoring and weather-market CRPS tracking.  
Status: Working after resolution metadata fixes.  
Next: Grow the resolved sample size and watch city-level sigma recommendations.

## 2026-03-20 — Duplicate dashboard/weather constants were consolidated
Changed: `src/arena/dashboard/queries.py`, `src/arena/data_sources/weather_constants.py`, `src/arena/data_sources/weather_ensemble.py`  
What: Canonical weather bias values were moved to a shared source so the dashboard and ensemble layer stop drifting.  
Status: Working.  
Next: Keep any future station/source-specific adjustments in the shared constants module.

## 2026-03-20 — Tavily path retired in favor of Nexus/Perplexia
Changed: `src/arena/main.py`, strategy configs, search adapter wiring
What: The old Tavily search path was removed so Arena consistently uses the Nexus/Perplexia research stack.
Status: Working.
Next: Remove any stale operational docs that still mention Tavily.

## 2026-03-20 — Market format classification + strategy filtering
Changed: `src/arena/categorization.py`, `src/arena/db.py`, `src/arena/strategies/base.py`, `src/arena/strategies/algo_forecast.py`, `src/arena/strategies/algo_harvester.py`, `src/arena/strategies/algo_partition.py`, `src/arena/strategies/llm_strategy.py`, `src/arena/intelligence/info_packet.py`, `scripts/recategorize_markets.py`, all strategy TOML configs
What: Added `detect_market_format()` returning binary/multi_outcome/numeric_bracket/unknown. Added `market_format TEXT` column via migration. Multi-outcome format heuristic (3+ comma-separated yes/no items) now infers sports category, reducing uncategorized "event" markets from 1401 to 102 (99.7% categorized). Crypto disambiguation logic fixes "Solana Sierra" and "COL Avalanche" false positives (crypto 17→4). Strategy base class gains `supported_formats` and `supported_categories` with `is_market_eligible()` gate. No strategy supports `multi_outcome`. Strategy configs updated: llm_news_trader fixed mismatched category names (events→event, regulation→legal, tech→science_tech), algo_harvester scoped to politics/entertainment/legal/sports, algo_partition scoped to weather only. Info packet builder also filters by `supported_formats`.
Status: Working. All py_compile checks pass.
Next: Monitor remaining 102 "event" markets; consider LLM-based fallback for truly ambiguous questions.

## 2026-03-20 — FIX: Settlement pipeline activated and hardened
Changed: `src/arena/engine/settlement.py`, `src/arena/main.py`
What: Settlement pipeline was registered in the scheduler but never produced results because weather observation failures were silently swallowed. Added per-market try/except error handling with `settled_count`/`skipped_count`/`error_count` tracking, `settlement_error` event logging on failure, and a summary log line. Added `market_settled` event recording in `SettlementEngine.settle_market()` with market_id, venue, winning outcome, positions settled count, total realized PnL, PnL by strategy, and resolution source. Manual trigger confirmed: 121 markets settled, 64 skipped, 0 errors.
Status: Working. 121 resolutions and 121 `market_settled` events recorded.
Next: Monitor ongoing settlement cycles and grow resolved sample size.

## 2026-03-20 — FIX: Central Kelly sizing enforcement in execute_decision
Changed: `src/arena/main.py`
What: The `llm_analyst` placed a $100.50 position (1415 shares × $0.071) on a 7% probability market, bypassing the $50 RISK_MAX_SINGLE_TRADE_SIZE cap. Root cause: individual strategies ran their own Kelly sizing but `execute_decision()` had no central enforcement. Added a single enforcement point in `execute_decision()` that ALL strategies pass through — recomputes `compute_position_size()` from scratch using orderbook ask price, applies the full Kelly chain (raw → half_kelly → capped → extreme prob reduction), and enforces a hard dollar cap via `RISK_MAX_SINGLE_TRADE_SIZE`. Logs a `trade_sizing` event with the full chain for every trade. The $100.50 trade scenario now returns "no_trade: no edge" (7% prob vs 7.1% ask).
Status: Working. All py_compile checks pass.
Next: Monitor `trade_sizing` events to confirm all trades are properly sized.

## 2026-03-20 — FIX: Dashboard reset button for paper trading
Changed: `src/arena/engine/paper_reset.py` (new), `src/arena/dashboard/app.py`, `src/arena/dashboard/templates/partials/positions_page.html`
What: Extracted reset logic from `scripts/reset_paper_trading.py` into a reusable `reset_paper_trading()` function in `src/arena/engine/paper_reset.py`. Added `POST /api/reset-paper-trading` endpoint to the dashboard. Added a red "Reset Paper Trading" button in the positions page summary strip with HTMX confirm dialog. Reset closes open positions (status='reset_cancelled'), cancels pending orders, resets all portfolios to $10,000, backs up state files, and records a `paper_reset` event.
Status: Working. Test confirmed: 1 position closed, all balances reset to $10,000.
Next: Consider adding a reset history view to the dashboard.

## 2026-03-20 — FIX 5-pipeline: Research pipeline, CRPS calibration, settlement logging, env vars, algo_partition edge
Changed: `src/arena/intelligence/rate_limiter.py`, `src/arena/intelligence/info_packet.py`, `src/arena/calibration/crps_tracker.py`, `src/arena/calibration/resolution_hook.py`, `src/arena/engine/settlement.py`, `src/arena/strategies/algo_partition.py`, `.env`, `.env.example`

What:
1. **Research pipeline**: `NexusRateLimiter` is now configurable via `NEXUS_RATE_LIMIT_CALLS` (20), `NEXUS_RATE_LIMIT_WINDOW_SECONDS` (1800), `NEXUS_COOLDOWN_RESET_MINUTES` (5). Added `is_in_cooldown()`, `set_cooldown()`, `cooldown_expires_in()` methods. After a Nexus error, cooldown is set for 5 minutes (configurable), then auto-resets so the next scan cycle retries. `_should_search()` now returns a tuple `(should_search, reason)` and logs `research_call_attempted` / `research_call_blocked` events to the events table with reason tags: `search_disabled`, `cooldown`, `rate_limit`, `trigger_conditions_not_met`. `_maybe_research_market()` removed the instance-level `_research_assistant_available` flag; cooldown is now solely managed by the module-level rate limiter. Build() no longer double-calls `_should_search()` for the same market.

2. **CRPS calibration**: `parameter_adjustments` table uses `current_value`/`recommended_value` column names (verified correct in both DB schema and hook INSERT). Added try/except around the INSERT with error logging so silent failures no longer go unnoticed. CRPS `record()` now deduplicates: skips insert if an identical record (same city, target_date, mu±0.01, sigma±0.01) exists in the last hour.

3. **Settlement**: `settle_market()` now fetches all open positions into a list first and logs `found N total open positions, filtering for market match...` plus uses `str()` coercion on both sides of the market_id/venue comparison to prevent type-mismatch silent failures.

4. **Env vars**: Added to `.env`: `RISK_MAX_SINGLE_TRADE_SIZE=50`, `RISK_KELLY_FRACTION_MULTIPLIER=0.25`, `RISK_MIN_TRADE_SIZE=5`, `RISK_REENTRY_PRICE_DELTA_CENTS=5`, `RISK_MAX_EXPOSURE_PER_MARKET=75`, `RISK_MAX_POSITIONS_PER_MARKET=2`, `NEXUS_RATE_LIMIT_CALLS=20`, `NEXUS_RATE_LIMIT_WINDOW_SECONDS=1800`, `NEXUS_COOLDOWN_RESET_MINUTES=5`. Also added to `.env.example` with explanatory comments.

5. **algo_partition edge**: HOLD decisions now report `expected_edge_bps = 0` instead of the computed deviation value. Only BUY decisions carry a non-zero edge.

Status: All py_compile checks pass. All diagnostic queries confirmed.

## 2026-03-20 — FIX 6-pipeline-2: Research pipeline, env loading, execution gates, CRPS feedback loop, restart script
Changed: `src/arena/env.py`, `src/arena/intelligence/rate_limiter.py`, `src/arena/intelligence/info_packet.py`, `src/arena/main.py`, `src/arena/dashboard/app.py`, `src/arena/data_sources/weather_ensemble.py`, `src/arena/calibration/resolution_hook.py`, `config/strategies/algo_forecast.toml`, `src/arena/scripts/run_burnin.sh`, `scripts/restart_arena.sh` (new)

What:
1. **Env loading**: `load_local_env()` now uses `setdefault` (was overwriting) so uvicorn-launched processes don't override with blank values. Added belt-and-suspenders `load_local_env()` call to `src/arena/dashboard/app.py` before all other imports.

2. **NexusRateLimiter** env vars: Moved `NEXUS_RATE_LIMIT_CALLS`, `NEXUS_RATE_LIMIT_WINDOW_SECONDS`, `NEXUS_COOLDOWN_RESET_MINUTES` from class-level attributes (read at import time) to module-level getter functions `_max_calls()`, `_window_seconds()`, `_cooldown_minutes()` that are called at runtime. This ensures that when the scheduler process starts fresh, `load_local_env()` has already set the env vars before any research cycle runs.

3. **Missing `_research_assistant_available`**: `InfoPacketBuilder.__init__` now initializes `self._research_assistant_available = True`. Without this, the attribute was referenced in `build()` but never set, causing `AttributeError` on every info packet build.

4. **algo_forecast research**: Added `[strategy.search]` section to `algo_forecast.toml` with `enabled=true`, `max_searches_per_cycle=3`, `trigger_conditions=["always"]`, `research_assistant_enabled=true`. Previously this section was entirely absent, making `search_budget = 0` and disabling all research calls for the most active strategy.

5. **llm_analyst trigger_conditions**: Changed from `["near_resolution", "high_volatility"]` to `["always"]` so research is attempted on every cycle (not blocked by trigger conditions).

6. **Execution gate logging**: Added 6 structured gate-level event logs to `execute_decision()` replacing opaque `execution_skip` events: `execution_gate_market_active` (gate 1), `execution_gate_risk_approval` (gate 2), `execution_gate_orderbook` (gate 3), `execution_gate_reentry` (gate 4), `execution_gate_spread_filter` (gate 5), `execution_gate_kelly_sizing` (gate 6). Each includes strategy_id, market_id, outcome_id, venue, pass/fail, and gate-specific details (spread value, risk reason, Kelly computed size, etc.).

7. **CRPS feedback loop**: `weather_ensemble.py` now reads `parameter_adjustments` table via `_load_sigma_adjustment_from_db()` at the start of `get_ensemble_forecast()`. If a valid `ensemble_sigma` adjustment exists for the city, it overrides `sigma_mult`. This closes the feedback loop — adjustments written to the DB by `resolution_hook` are now consumed by forecast computation.

8. **restart_arena.sh**: New script at `scripts/restart_arena.sh` that kills the dashboard (port 8050) and scheduler processes, waits 2s, sources `.env`, then restarts both processes with stdout/stderr redirected to `/tmp/arena-dashboard.log` and `/tmp/arena-scheduler.log`. Made executable.

9. **run_burnin.sh**: Added `set -a; source .env; set +a` before starting the scheduler so all env vars are available in the burn-in loop.

Status: All py_compile checks pass. Diagnostic queries confirmed RISK_KELLY_FRACTION_MULTIPLIER=0.25, RISK_MAX_SINGLE_TRADE_SIZE=50, NEXUS_RATE_LIMIT_CALLS=20 all loaded. 147 parameter_adjustments exist in DB (confirming INSERT works). research_log has 2 entries (stale from before fix — new research calls should start after restart). execution_skip analysis shows: 14 stale Polymarket orderbooks (algo_partition), 8 daily loss limit hits (algo_harvester/llm_analyst), 2 spread filter rejections — no execution bugs, just business logic gates. Settlement shows 121 markets settled with 0 positions_settled — no open positions overlap with settling markets (Ankara weather markets have no positions; system traded Chicago/London). Parameter adjustments INSERT confirmed working (147 rows). NexusRateLimiter now reads env vars at call time, ensuring fresh scheduler processes use .env values.


## 2026-03-20 — FIX 6-pipeline: Env loading, research pipeline, restart script, gate logging, CRPS feedback

Changed: `src/arena/env.py`, `src/arena/intelligence/rate_limiter.py`, `src/arena/intelligence/info_packet.py`, `src/arena/dashboard/app.py`, `src/arena/main.py`, `src/arena/data_sources/weather_ensemble.py`, `config/strategies/algo_forecast.toml`, `config/strategies/llm_analyst.toml`, `scripts/restart_arena.sh`, `src/arena/scripts/run_burnin.sh`

What:

1. **Env loading**: `load_local_env()` already parses `.env` via `os.environ.setdefault`. The real fix was ensuring `NexusRateLimiter` reads env vars at **call time** (via module-level accessor functions) rather than at class-definition or instantiation time. Also added missing `_research_assistant_available = True` to `InfoPacketBuilder.__init__`.

2. **Research pipeline**: `algo_forecast.toml` was missing the `[strategy.search]` section entirely — `max_searches_per_cycle = 0` → no research ever called. Added the full `[strategy.search]` section with `enabled = true`, `max_searches_per_cycle = 3`, `trigger_conditions = ["always"]`, `research_assistant_enabled = true`, `research_mode = "standard"`. Also changed `llm_analyst.toml` `trigger_conditions` from `["near_resolution", "high_volatility"]` to `["always"]` so research fires on every cycle. `algo_harvester` and `algo_partition` intentionally have no research (algorithmic-only).

3. **Restart script**: Created `scripts/restart_arena.sh` (executable) that kills the uvicorn dashboard (port 8050) and scheduler processes, waits 2s, sources `.env`, then restarts both with output redirected to `/tmp/arena-dashboard.log` and `/tmp/arena-scheduler.log`. Updated `run_burnin.sh` to `source .env` before starting the scheduler.

4. **Dashboard env**: Added `load_local_env()` call to top of `src/arena/dashboard/app.py` so the uvicorn process also loads env vars.

5. **Gate-level execution logging**: Added 6 new event types in `execute_decision()` for precise visibility into where trades die:
   - `execution_gate_market_active`: pass/fail + market status
   - `execution_gate_risk_approval`: pass/fail + reason
   - `execution_gate_orderbook`: pass/fail + best_bid, best_ask, spread, error
   - `execution_gate_spread_filter`: pass/fail + spread_value, threshold, bid, ask
   - `execution_gate_reentry`: pass/fail + price_delta, threshold
   - `execution_gate_kelly_sizing`: pass/fail + computed_size, min_threshold, hard_cap, reason

6. **CRPS feedback loop**: `weather_ensemble.py` now calls `_load_sigma_adjustment_from_db()` at the start of forecast computation to read the most recent `ensemble_sigma` adjustment from the `parameter_adjustments` table and apply it as the sigma multiplier. This closes the loop so calibration suggestions written by `resolution_hook.py` are actually consumed. Falls back to the static `sigma_calibration.json` if no DB adjustment exists.

Status: All py_compile checks pass.

## 2026-03-21 — Nexus research pipeline repurposed from probability synthesis to discovery alerts
Changed: `src/arena/intelligence/discovery.py` (new), `src/arena/intelligence/discovery_logger.py` (new), `src/arena/intelligence/info_packet.py`, `src/arena/intelligence/research.py`, `src/arena/intelligence/nexus_types.py`, `src/arena/db.py`, `config/strategies/algo_forecast.toml`, `config/strategies/llm_analyst.toml`, `config/strategies/llm_news_trader.toml`, `.env.example`
What: Added a Python-side discovery layer that uses Nexus `/api/v1/research` to look for fresh information asymmetries instead of trusting an LLM probability estimate on liquid markets. `InfoPacketBuilder` now supports `research_mode = "discovery" | "probability" | "both" | "off"`, classifies reports into typed signals (`breaking_news`, `weather_alert`, `data_release`, `regulatory`, `source_disagree`, `stale_market`, `no_signal`), logs them to a new `discovery_alerts` table, and attaches `discovery_signals`, `has_breaking_signal`, and `signal_direction` to packet opportunities. Discovery calls share the existing Nexus rate limiter and `research_log`, while a new cost gate blocks low-value calls whose expected profit does not justify token spend.
Status: Working at the Python integration layer; no TypeScript/Nexus endpoint changes required.
Next: Let the strategy refactor consume `discovery_signals` directly for trade decisions and use `discovery_alerts.acted_on` to measure signal-to-PnL conversion.

## 2026-03-21 — Calibration hardening: CRPS/Brier audit trail, city-scoped sigma feedback, confidence gate
Changed: `src/arena/calibration/crps_tracker.py`, `src/arena/calibration/resolution_hook.py`, `src/arena/calibration/confidence_gate.py`, `src/arena/data_sources/weather_ensemble.py`, `src/arena/data_sources/station_observations.py`, `src/arena/data_sources/weather_constants.py`, `src/arena/db.py`
What: Hardened the weather calibration loop so settlement now records one city/date CRPS entry with file-tail dedup, records binary-market Brier scores in `data/brier_history.jsonl`, and attaches observation provenance (`open_meteo_archive` plus a METAR secondary check when available). Added city-scoped `parameter_adjustments.city` so sigma recommendations are written for the affected city, with structured insert logging and a CRPS-ratio-based `compute_sigma_adjustment()` policy. `weather_ensemble.py` now treats DB recommendations as absolute sigma values in degrees C and applies them with priority `parameter_adjustments > sigma_calibration.json > inverse-variance default`, marking consumed rows `auto_applied=1`. Added `ConfidenceGate` for read-only tradeability checks based on resolved sample size, recent CRPS ratio, and recent Brier score.
Status: Working in code; verification run pending on the current workspace data.
Next: Let fresh settled weather markets populate city-tagged sigma adjustments, then wire `ConfidenceGate` into the weather strategy execution path after the parallel strategy work lands.
