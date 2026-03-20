# Arena Trading System — Implementation Summary

## System Overview

Arena is a paper-trading simulation system for prediction markets (Polymarket, Kalshi). It focuses on weather markets — temperature forecasts that resolve to binary outcomes like "Will Chicago's high exceed 60F on March 21?"

The system runs 10 strategies: 5 algorithmic (`algo_forecast`, `algo_harvester`, `algo_partition`, `algo_meanrev`, `algo_momentum`) and 5 LLM-based (`llm_generalist`, `llm_analyst`, `llm_contrarian`, `llm_strategist`, `llm_news_trader`). Two algo strategies (`algo_meanrev`, `algo_momentum`) are disabled due to invalid model parameters.

### Architecture

```
Market Adapters (Polymarket/Kalshi)
  -> Market Scanner (discovers active markets)
  -> Strategy Engine (generates decisions)
    -> Info Packet Builder (assembles research context)
    -> Weather Ensemble (multi-source forecasts)
    -> Station Observations (real-time conditions)
    -> Intraday Probability (blends forecast + observations)
    -> Kelly Criterion (position sizing)
    -> Risk Manager (pre-trade checks)
  -> Paper Executor (simulates fills)
  -> Settlement Engine (resolves positions)
    -> Resolution Hook (calibration feedback)
  -> Dashboard Export (Google Sheets)
```

---

## Bug Fixes (Phase 1)

10 bugs were identified and fixed before feature work:

1. **Race condition in `call_llm()`** — Mutable function attributes (`call_llm.last_attempts`, `call_llm.last_result`) caused shared state bugs. Fixed by returning a tuple `(content, attempts, result)`.

2. **Phantom implicit action extraction** — `_extract_implicit_action()` in `output_parser.py` manufactured trades when the LLM explicitly said "no action." Deleted entirely.

3. **`algo_meanrev`/`algo_momentum` disabled** — Both strategies referenced nonexistent model code. Added RuntimeError guard to prevent execution.

4. **Import cleanup** — Removed dead imports for the disabled strategies.

5. **`algo_partition` position sizing** — Was using `item["yes_ask"]` as the dollar amount (a probability, not a dollar value). Fixed to `min(portfolio.cash * 0.02, 25.0)`.

6. **Datetime serialization** — `info_packet.py` crashed on `datetime` objects in JSON. Added recursive `_serialize_dates()` method.

7. **Paper executor side mapping** — Side was set to `"buy"` for all trades. Fixed to map `BUY -> "long"`, `SELL -> "short"`.

8. **Weather fee basis points** — `default_weather_bps = 0` meant zero fees on weather trades. Fixed to `200`.

9. **Win rate calculation** — `calibration.py` and `sheets_sync.py` counted all executions as wins. Fixed to join with `resolutions` table.

10. **Daily snapshot over-counting** — `capture_daily_snapshots` used cumulative portfolio counters. Fixed to query today-only executions.

---

## Week 1 — Multi-Source Weather Ensemble

### Files Created
- `src/arena/data_sources/__init__.py`
- `src/arena/data_sources/weather_ensemble.py`
- `src/arena/data_sources/weather_bias.py`

### What It Does

Four weather forecast sources run concurrently via `asyncio.gather`:

| Source | API | Coverage |
|--------|-----|----------|
| Open-Meteo (default model) | `api.open-meteo.com/v1/forecast` | Global |
| GFS (US model) | `api.open-meteo.com/v1/gfs` | Global |
| ECMWF (European model) | `api.open-meteo.com/v1/ecmwf` | Global |
| HKO (Hong Kong Observatory) | `data.weather.gov.hk` | Hong Kong only |

The ensemble computes:
- **Mean high/low** across all successful sources
- **Sigma** (standard deviation, floored at 1.0C) — used as uncertainty in probability calculations
- **Bias correction** — if historical forecast errors are available (5+ samples), the systematic bias is subtracted

### Bias Tracking (`weather_bias.py`)

Every forecast is recorded to `forecast_history` table. When a market resolves, `backfill_actuals()` writes the true temperature and computes per-source error. Over time, `get_bias_correction()` returns the mean error for a location, enabling automatic debiasing.

### Integration

`algo_forecast.py` was rewired from single-source to ensemble:
- `_forecast_high_c()` -> `_get_ensemble()` (returns full ensemble dict)
- `_estimate_probability()` now accepts `sigma_override` from ensemble spread
- Evidence items include ensemble details (source count, names, sigma, bias correction)

---

## Intraday Observations (Pre-Week 2)

### Files Created
- `src/arena/data_sources/station_observations.py`

### What It Does

Three real-time observation sources:

| Source | API | Data |
|--------|-----|------|
| Open-Meteo Current | Same API, `current=` params | Live temperature, humidity, wind |
| METAR (aviation weather) | `aviationweather.gov/api/data/metar` | Airport observations (13 ICAO codes) |
| Open-Meteo Hourly Trajectory | Same API, `past_hours=12` | Last 12 hours of hourly temps |

`get_current_observations()` returns:
- `current_temp_c` — median of all current readings
- `max_temp_so_far_c` / `min_temp_so_far_c` — from today's trajectory
- `trending` — "warming", "cooling", or "stable" (based on last 3 hours)
- `hours_remaining` — estimated daylight remaining
- `temp_trajectory` — hourly temperature history

### Intraday Probability (`algo_forecast.py`)

`_compute_intraday_probability()` overrides the standard CDF when markets resolve today:

1. **Threshold already hit** (max_so_far >= threshold): return 0.98
2. **Very unlikely** (< 1 hour left, 2+ degrees away): return 0.05
3. **Otherwise, blend**:
   - If **cooling** and below threshold: reduce the effective forecast (only 30% of remaining warming potential)
   - If **warming**: use ensemble forecast with tighter sigma (0.7x — real-time confirms the trend)
   - If **stable**: use ensemble forecast as-is

### Observation-Triggered Monitoring

`monitor_intraday_weather()` runs every 15 minutes via the scheduler:
- Scans same-day weather markets
- Fetches current observations
- Logs `THRESHOLD HIT` when max_so_far >= threshold
- Logs `LIKELY MISS` when cooling, below threshold, and < 3 hours remaining
- Records events to the `events` table for analysis

This is a monitoring layer, not auto-trading — it identifies when to re-run `algo_forecast`.

### Info Packet Integration

For same-day weather markets, the info packet now includes a `current_conditions` section with live temperature, trend, and daylight remaining. This gives LLM strategies real-time context.

---

## Week 2 — Calibration Feedback Loop

### Files Created
- `src/arena/calibration/__init__.py`
- `src/arena/calibration/resolution_hook.py`
- `src/arena/scripts/calibration_report.py`

### What It Does

When a market resolves, `on_market_resolved()` fires four stages:

1. **Backfill forecast actuals** — Fetches historical weather from Open-Meteo and updates `forecast_history` with real temperatures
2. **Score every decision** — Computes Brier score `(predicted - actual)^2` for each decision on the market
3. **Compute rolling health** — 50-decision rolling window: mean Brier, calibration error (max deviation across 5 probability buckets), overconfidence rate
4. **Generate adjustments** — Rules-based parameter recommendations:
   - Brier > 0.25: recommend increasing sigma
   - Calibration error > 0.15: recommend sigma adjustment
   - Overconfidence > 40%: recommend reducing confidence
   - Brier < 0.15 and calibration < 0.10: recommend increasing Kelly fraction

### DB Tables
- `decision_scores` — per-decision Brier scores
- `strategy_health` — rolling strategy metrics over time
- `parameter_adjustments` — recommended config changes (not auto-applied)
- `station_observations` — real-time observation history

### Calibration Report

`python -m arena.scripts.calibration_report` prints:
- Strategy health dashboard
- Calibration curve (predicted vs actual by bucket)
- Forecast bias by source
- Pending parameter adjustments
- Recent decision accuracy

---

## Week 3 — Risk Management & Execution

### Files Created
- `src/arena/risk/__init__.py`
- `src/arena/risk/kelly.py`
- `src/arena/risk/risk_manager.py`
- `src/arena/real_executor.py`

### Kelly Criterion Position Sizing (`kelly.py`)

`compute_position_size()` implements quarter-Kelly:

1. Compute edge: `predicted_probability - market_ask_price`
2. Compute full Kelly fraction: `(p * b - q) / b` where `b = payout_ratio`
3. Apply fractional Kelly (default 0.25x)
4. Cap at `max_position_pct * bankroll` and `max_position_usd`
5. Reject if below `min_position_usd`
6. Reject if expected profit is consumed by fees

### Risk Manager (`risk_manager.py`)

`RiskManager.check_trade()` runs 6 pre-trade checks:

| Check | Default Limit |
|-------|--------------|
| Daily trade count | 20 trades/day |
| Daily P&L (spend) | $50/day |
| Open positions | 10 max |
| Per-market exposure | $50/market |
| Total exposure | $200 |
| Loss streak cooldown | 3 consecutive failures -> 60min pause |

### Real Executor (`real_executor.py`)

Stub for future Polymarket CLOB API integration, behind **two safety gates**:
1. `config/arena.toml` must have `[execution] mode = "live"`
2. Environment variable `ARENA_LIVE_TRADING` must equal `"YES_I_UNDERSTAND_THIS_IS_REAL_MONEY"`

If either is missing, the system falls back to paper mode.

### Pipeline Wiring

`algo_forecast.py` now flows:
```
Parse market -> Fetch ensemble -> (Intraday observations if same-day)
  -> Estimate probability -> Kelly sizing -> Risk check -> Build action
```

### Configuration

`config/arena.toml` now includes:

```toml
[execution]
mode = "paper"

[risk]
max_daily_trades = 20
max_daily_loss_usd = 50.0
max_open_positions = 10
max_exposure_per_market_usd = 50.0
max_total_exposure_usd = 200.0
cooldown_after_loss_streak = 3
cooldown_minutes = 60

[position_sizing]
kelly_fraction = 0.25
max_position_pct = 0.02
min_position_usd = 1.0
max_position_usd = 25.0
fee_rate = 0.02
```

Startup now logs execution mode validation.

---

## Current State

### What Works End-to-End
- `arena init` — creates DB with all tables (including new ones)
- `arena scan` — discovers weather markets from Polymarket/Kalshi
- `arena run-once algo_forecast` — full pipeline: ensemble forecast -> intraday observations -> Kelly sizing -> risk check -> paper execution
- `arena status` — portfolio dashboard
- `arena show-info-packet <strategy>` — includes current conditions for same-day markets
- Scheduler runs all jobs on intervals including 15-minute intraday monitoring
- Resolution triggers calibration scoring and strategy health computation

### DB Schema (new tables)
- `forecast_history` — per-source forecast records with bias tracking
- `decision_scores` — Brier scores per resolved decision
- `strategy_health` — rolling calibration metrics
- `parameter_adjustments` — recommended config changes
- `station_observations` — real-time weather observations

### Files Modified
- `src/arena/strategies/algo_forecast.py` — ensemble, intraday, Kelly, risk wiring
- `src/arena/intelligence/info_packet.py` — ensemble signals, current conditions
- `src/arena/intelligence/output_parser.py` — removed phantom action extraction
- `src/arena/engine/paper_executor.py` — fixed side mapping
- `src/arena/engine/settlement.py` — fires resolution hook
- `src/arena/analytics/calibration.py` — added health/scoring queries
- `src/arena/export/sheets_sync.py` — fixed win rate
- `src/arena/strategies/algo_partition.py` — fixed position sizing
- `src/arena/strategies/llm_strategy.py` — fixed call_llm race condition
- `src/arena/main.py` — disabled strategies, intraday monitor, execution mode
- `src/arena/scheduler.py` — added intraday monitoring job
- `src/arena/config.py` — added execution/risk/position_sizing fields
- `src/arena/db.py` — added 5 new tables in migrations
- `config/arena.toml` — fixed weather fees, added new config sections

### Files Created
- `src/arena/data_sources/weather_ensemble.py`
- `src/arena/data_sources/weather_bias.py`
- `src/arena/data_sources/station_observations.py`
- `src/arena/calibration/resolution_hook.py`
- `src/arena/scripts/calibration_report.py`
- `src/arena/risk/kelly.py`
- `src/arena/risk/risk_manager.py`
- `src/arena/real_executor.py`

### Not Modified (as required)
- `src/arena/engine/paper_executor.py` (post-bug-fix)
- `src/arena/strategies/llm_strategy.py` (post-bug-fix)
- LLM adapter files (`llm_google.py`, `llm_minimax.py`, etc.)
- `config/models.toml` model provider settings

### What's Not Yet Implemented
- Real CLOB API order placement (stub exists with safety gates)
- Auto-application of parameter adjustments (logged but manual)
- Lucknow/Madrid city coordinates (logged as warnings)
- `algo_meanrev`/`algo_momentum` (disabled, need model rewrite)
