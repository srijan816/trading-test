from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict

from arena.db import ArenaDB
from arena.engine.portfolio import close_position
from arena.models import ResolutionEvent, new_id, utc_now

logger = logging.getLogger(__name__)


class SettlementEngine:
    def __init__(self, db: ArenaDB) -> None:
        self.db = db

    def settle_market(self, market_id: str, venue: str, winning_outcome_id: str, winning_outcome_label: str, resolution_source_url: str, resolution_data: dict | None = None) -> ResolutionEvent:
        resolved_metadata = dict(resolution_data or {})
        market_row = self.db.get_market(market_id, venue)
        if market_row is not None:
            outcomes_json = market_row["outcomes_json"]
            outcomes = json.loads(outcomes_json) if isinstance(outcomes_json, str) else (outcomes_json or [])
            yes_outcome_id = next(
                (str(outcome.get("outcome_id")) for outcome in outcomes if str(outcome.get("label", "")).strip().lower() == "yes"),
                None,
            )
            if yes_outcome_id is not None:
                resolved_metadata.setdefault("actual_outcome", 1.0 if str(winning_outcome_id) == yes_outcome_id else 0.0)
            resolved_metadata.setdefault("market_question", str(market_row["question"]))
        resolved_metadata.setdefault("winning_outcome_label", winning_outcome_label)

        impacted: dict[str, float] = defaultdict(float)
        settled_ids: list[str] = []
        open_positions = list(self.db.list_open_positions())
        logger.info(f"Settlement: market {market_id}/{venue} — found {len(open_positions)} total open positions, filtering for market match...")
        for position in open_positions:
            if str(position.market_id) != str(market_id) or str(position.venue) != str(venue):
                continue
            portfolio = self.db.get_portfolio(position.strategy_id)
            if not portfolio:
                continue
            payout = position.quantity * (1.0 if position.outcome_id == winning_outcome_id else 0.0)
            updated = close_position(portfolio, position.position_id, payout)
            self.db.save_portfolio(updated)
            for pos in updated.positions:
                if pos.position_id == position.position_id:
                    self.db.upsert_position(pos)
            impacted[position.strategy_id] += payout - (position.quantity * position.avg_entry_price)
            settled_ids.append(position.position_id)
        resolution = ResolutionEvent(
            resolution_id=new_id("resolution"),
            market_id=market_id,
            venue=venue,
            resolved_at=utc_now(),
            winning_outcome_id=winning_outcome_id,
            winning_outcome_label=winning_outcome_label,
            resolution_source_url=resolution_source_url,
            positions_settled=settled_ids,
            total_pnl_impact=dict(impacted),
        )
        self.db.save_resolution(resolution)

        # Log settlement event
        self.db.record_event(
            "market_settled",
            {
                "market_id": market_id,
                "venue": venue,
                "winning_outcome_id": winning_outcome_id,
                "winning_outcome_label": winning_outcome_label,
                "positions_settled": len(settled_ids),
                "total_realized_pnl": round(sum(impacted.values()), 4),
                "pnl_by_strategy": {k: round(v, 4) for k, v in impacted.items()},
                "resolution_source": resolution_source_url,
            },
        )
        logger.info(
            "Settled market %s/%s -> %s, %d positions, PnL=%s",
            market_id, venue, winning_outcome_label,
            len(settled_ids), {k: round(v, 2) for k, v in impacted.items()},
        )

        # Fire calibration hook (non-blocking, must not crash settlement)
        try:
            from arena.calibration.resolution_hook import on_market_resolved
            loop = asyncio.get_running_loop()
            loop.create_task(
                self._fire_resolution_hook(market_id, venue, winning_outcome_id, resolved_metadata)
            )
        except RuntimeError:
            # No running event loop — fire synchronously (blocks but guarantees execution)
            try:
                from arena.calibration.resolution_hook import on_market_resolved
                asyncio.run(on_market_resolved(self.db, market_id, venue, winning_outcome_id, resolved_metadata))
            except Exception as e:
                logger.warning(f"Resolution hook failed (sync fallback): {e}")
        except Exception as e:
            logger.warning(f"Resolution hook scheduling failed: {e}")

        return resolution

    async def _fire_resolution_hook(self, market_id: str, venue: str, winning_outcome_id: str, resolution_data: dict) -> None:
        try:
            from arena.calibration.resolution_hook import on_market_resolved
            await on_market_resolved(self.db, market_id, venue, winning_outcome_id, resolution_data)
        except Exception as e:
            logger.warning(f"Resolution hook failed: {e}")
