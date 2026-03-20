# Arena Burn-In Checklist

## Pre-Launch (do once)
- [ ] Run `arena init` — DB created with all tables
- [ ] Run `arena scan` — markets discovered (target: 5+ weather markets)
- [ ] Test ensemble for 3 locations — all return 2+ sources
- [ ] Test observations for 3 locations — all return 1+ sources
- [ ] Run `algo_forecast` once — decision recorded with Kelly sizing
- [ ] Run calibration report — runs without errors (empty data OK)
- [ ] Verify execution mode = "paper" in arena.toml
- [ ] Verify ARENA_LIVE_TRADING env var is NOT set
- [ ] Start burn-in: `nohup bash src/arena/scripts/run_burnin.sh > logs/burnin.log 2>&1 &`

## Daily Checks (run daily_health.sh)
- [ ] Day 1: System running, decisions being recorded
- [ ] Day 3: forecast_history has 50+ rows per location
- [ ] Day 5: At least 1 market has resolved, decision_scores has rows
- [ ] Day 7: Calibration report shows early Brier scores
- [ ] Day 10: forecast_history has actuals backfilled for resolved markets
- [ ] Day 14: strategy_health has rolling metrics with 10+ sample size
- [ ] Day 21: Bias corrections becoming reliable (5+ resolved per source)
- [ ] Day 28: Calibration curve meaningful, parameter adjustments generated

## Go/No-Go for Real Trading (after 28 days)
- [ ] algo_forecast Brier score < 0.20 on 30+ resolved decisions
- [ ] Calibration error < 0.10 (predicted probabilities match actual rates)
- [ ] Mean forecast error < ±0.5C across all sources
- [ ] Kelly sizing produces reasonable positions ($1-$25 range)
- [ ] Risk manager has blocked at least 1 trade (proves it works)
- [ ] No silent failures in logs (check for uncaught exceptions)
- [ ] Daily P&L is net positive or flat (not consistently negative)
- [ ] Parameter adjustments are reasonable (not suggesting wild changes)

## If Go: Real Trading Setup
1. Create Polymarket API account and fund with $100-$200
2. Set POLYMARKET_API_KEY and POLYMARKET_SECRET in .env
3. Change arena.toml: execution.mode = "live", dry_run = true
4. Run for 3 days in dry-run mode (logs orders but doesn't place them)
5. Review dry-run logs — are the orders sensible?
6. Set dry_run = false, max_position_usd = 5.0 (start tiny)
7. Set ARENA_LIVE_TRADING="YES_I_UNDERSTAND_THIS_IS_REAL_MONEY"
8. Monitor hourly for the first day
9. After 1 week of live trading, review P&L and adjust Kelly fraction
