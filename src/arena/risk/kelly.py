from __future__ import annotations

import logging
import os


logger = logging.getLogger(__name__)


def compute_position_size(
    predicted_probability: float,
    market_ask_price: float,
    bankroll: float,
    kelly_fraction: float | None = None,
    max_position_pct: float = 0.02,
    min_position_usd: float = 1.0,
    max_position_usd: float = 25.0,
    fee_rate: float = 0.02,
    yes_side_probability: float | None = None,
) -> dict:
    env_kelly_multiplier = float(os.getenv("RISK_KELLY_FRACTION_MULTIPLIER", "0.5"))
    env_min_position_usd = float(os.getenv("RISK_MIN_TRADE_SIZE", "5"))
    env_max_position_usd = float(os.getenv("RISK_MAX_SINGLE_TRADE_SIZE", "50"))
    effective_min_position_usd = max(min_position_usd, env_min_position_usd)
    effective_max_position_usd = min(max_position_usd, env_max_position_usd)
    effective_yes_probability = predicted_probability if yes_side_probability is None else yes_side_probability
    effective_kelly_multiplier = env_kelly_multiplier if kelly_fraction is None else float(kelly_fraction)

    # 1. Compute edge
    edge = predicted_probability - market_ask_price
    if edge <= 0:
        return {"action": "no_trade", "reason": "no edge"}

    # 2. Compute Kelly fraction
    if market_ask_price <= 0 or market_ask_price >= 1.0:
        return {"action": "no_trade", "reason": "invalid market price"}

    payout_ratio = (1.0 - market_ask_price) / market_ask_price
    kelly_full = (
        predicted_probability * payout_ratio - (1 - predicted_probability)
    ) / payout_ratio

    if kelly_full <= 0:
        return {"action": "no_trade", "reason": "Kelly says no edge after odds"}

    kelly_bet_fraction = kelly_full * effective_kelly_multiplier

    # 3. Compute dollar amount
    raw_amount = bankroll * kelly_full
    half_kelly_amount = raw_amount * effective_kelly_multiplier
    capped_amount = min(half_kelly_amount, bankroll * max_position_pct, effective_max_position_usd)
    final_amount = capped_amount
    low_confidence_reduction_applied = False
    if effective_yes_probability < 0.20 or effective_yes_probability > 0.80:
        final_amount *= 0.5
        low_confidence_reduction_applied = True
    final_amount = max(final_amount, 0)

    logger.info(
        "Kelly sizing: prob=%.3f, edge=%.3f, kelly_f=%.3f, raw=$%.2f, half_kelly=$%.2f, capped=$%.2f, final=$%.2f",
        predicted_probability,
        edge,
        kelly_full,
        raw_amount,
        half_kelly_amount,
        capped_amount,
        final_amount,
    )

    if final_amount < effective_min_position_usd:
        return {"action": "no_trade", "reason": f"Position ${final_amount:.2f} below minimum"}

    # 4. Compute fee-adjusted edge
    expected_profit = final_amount * (
        predicted_probability * (1.0 / market_ask_price - 1)
        - (1 - predicted_probability)
    )
    fee_cost = max(expected_profit, 0) * fee_rate
    net_expected_profit = expected_profit - fee_cost

    if net_expected_profit <= 0:
        return {"action": "no_trade", "reason": "Edge consumed by fees"}

    # 5. Return result
    return {
        "action": "trade",
        "amount_usd": round(final_amount, 2),
        "kelly_full": round(kelly_full, 4),
        "kelly_fraction_used": effective_kelly_multiplier,
        "kelly_bet_fraction": round(kelly_bet_fraction, 4),
        "edge": round(edge, 4),
        "raw_amount_usd": round(raw_amount, 2),
        "half_kelly_amount_usd": round(half_kelly_amount, 2),
        "capped_amount_usd": round(capped_amount, 2),
        "final_amount_usd": round(final_amount, 2),
        "low_confidence_reduction_applied": low_confidence_reduction_applied,
        "expected_profit_usd": round(net_expected_profit, 2),
        "risk_reward_ratio": round(net_expected_profit / final_amount, 4) if final_amount > 0 else 0,
    }
