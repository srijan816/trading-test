#!/bin/bash
# Run daily to check system health during burn-in
# Intended for manual runs or cron: 0 9 * * * bash daily_health.sh

set -euo pipefail
cd /Volumes/SrijanExt/Code/finance/trading-test
export PYTHONPATH=src

DB="data/arena.db"

echo "======================================="
echo "ARENA DAILY HEALTH CHECK - $(date +%Y-%m-%d)"
echo "======================================="
echo ""

echo "--- Market Coverage ---"
sqlite3 -header -column "$DB" "
  SELECT category, COUNT(*) AS markets,
         SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS active
  FROM markets
  GROUP BY category
  ORDER BY category;
"

echo ""
echo "--- Decisions (last 24h) ---"
sqlite3 -header -column "$DB" "
  SELECT strategy_id,
         COUNT(*) AS decisions,
         SUM(CASE WHEN actions_json != '[]' THEN 1 ELSE 0 END) AS with_trades,
         ROUND(AVG(predicted_probability), 3) AS avg_pred,
         ROUND(AVG(expected_edge_bps), 0) AS avg_edge_bps
  FROM decisions
  WHERE timestamp >= datetime('now', '-1 day')
  GROUP BY strategy_id
  ORDER BY decisions DESC, strategy_id;
"

echo ""
echo "--- Executions (last 24h) ---"
sqlite3 -header -column "$DB" "
  SELECT strategy_id,
         COUNT(*) AS fills,
         ROUND(SUM(total_cost), 2) AS total_spent,
         ROUND(AVG(requested_amount_usd), 2) AS avg_position
  FROM executions
  WHERE timestamp >= datetime('now', '-1 day')
  GROUP BY strategy_id
  ORDER BY fills DESC, strategy_id;
"

echo ""
echo "--- Portfolio Status ---"
sqlite3 -header -column "$DB" "
  SELECT strategy_id,
         ROUND(cash, 2) AS cash,
         ROUND(total_value, 2) AS value,
         ROUND(realized_pnl, 2) AS realized,
         ROUND(unrealized_pnl, 2) AS unrealized,
         total_trades, winning_trades, losing_trades
  FROM portfolios
  ORDER BY strategy_id;
"

echo ""
echo "--- Forecast Ensemble Coverage ---"
sqlite3 -header -column "$DB" "
  SELECT location, source, COUNT(*) AS forecasts,
         ROUND(AVG(CASE WHEN actual_high_c IS NOT NULL
               THEN predicted_high_c - actual_high_c END), 2) AS avg_bias
  FROM forecast_history
  GROUP BY location, source
  ORDER BY location, source;
"

echo ""
echo "--- Calibration (if data exists) ---"
sqlite3 -header -column "$DB" "
  SELECT strategy_id, sample_size,
         ROUND(rolling_brier, 3) AS brier,
         ROUND(calibration_error, 3) AS cal_err,
         ROUND(overconfidence_rate, 3) AS overconf
  FROM strategy_health
  ORDER BY computed_at DESC
  LIMIT 5;
"

echo ""
echo "--- Pending Parameter Adjustments ---"
sqlite3 -header -column "$DB" "
  SELECT strategy_id, parameter_name,
         current_value, recommended_value, reason
  FROM parameter_adjustments
  WHERE auto_applied = 0
  ORDER BY created_at DESC
  LIMIT 10;
"

echo ""
echo "--- Observations (last 6h) ---"
sqlite3 -header -column "$DB" "
  SELECT location, COUNT(*) AS obs,
         ROUND(MIN(temperature_c), 1) AS min_c,
         ROUND(MAX(temperature_c), 1) AS max_c,
         MAX(observation_time) AS latest
  FROM station_observations
  WHERE created_at >= datetime('now', '-6 hours')
  GROUP BY location
  ORDER BY location;
"

echo ""
echo "--- Resolution Hook Activity ---"
sqlite3 -header -column "$DB" "
  SELECT COUNT(*) AS scored_decisions
  FROM decision_scores;
"
sqlite3 -header -column "$DB" "
  SELECT COUNT(*) AS resolved_forecasts
  FROM forecast_history
  WHERE actual_high_c IS NOT NULL;
"

echo ""
echo "======================================="
echo "END OF HEALTH CHECK"
echo "======================================="
