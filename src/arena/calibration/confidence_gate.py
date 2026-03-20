from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from arena.calibration.crps_tracker import CRPSTracker


class ConfidenceGate:
    """Blocks trading when recent calibration metrics show unreliable forecasts."""

    def __init__(self, db_path: str, crps_history_path: str) -> None:
        self.db_path = db_path
        self.crps_history_path = Path(crps_history_path)
        self.brier_history_path = self.crps_history_path.with_name("brier_history.jsonl")

    @staticmethod
    def _parse_timestamp(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @staticmethod
    def _matches_city(entry: dict, city: str) -> bool:
        return str(entry.get("city", "")).strip().lower() == str(city).strip().lower()

    def _tracker(self) -> CRPSTracker:
        return CRPSTracker(self.crps_history_path, self.brier_history_path)

    def _recent_brier_mean(self, tracker: CRPSTracker, city: str, last_n_days: int = 7) -> float | None:
        cutoff = datetime.now(UTC) - timedelta(days=last_n_days)
        values = []
        for entry in tracker.brier_history:
            if not self._matches_city(entry, city):
                continue
            timestamp = self._parse_timestamp(entry.get("timestamp"))
            if timestamp is None or timestamp < cutoff:
                continue
            score = entry.get("brier_score")
            if score is not None:
                values.append(float(score))
        if not values:
            return None
        return sum(values) / len(values)

    def _resolved_count(self, tracker: CRPSTracker, city: str) -> int:
        return sum(1 for entry in tracker.history if self._matches_city(entry, city))

    def is_tradeable(self, city: str) -> tuple[bool, str]:
        tracker = self._tracker()
        total_resolved = self._resolved_count(tracker, city)
        summary = tracker.get_calibration_summary(city=city, last_n_days=7)
        ratio = float(summary.get("calibration_ratio", 0.0) or 0.0)
        mean_brier = self._recent_brier_mean(tracker, city, last_n_days=7)

        if total_resolved < 5:
            return False, f"insufficient calibration data: {total_resolved}/5 resolved"
        if ratio > 5.0:
            return False, f"calibration ratio {ratio:.1f}x exceeds 5.0x threshold"
        if mean_brier is not None and mean_brier > 0.35:
            return False, f"Brier score {mean_brier:.3f} exceeds 0.35 threshold"
        if ratio > 3.0:
            return True, f"calibration degraded: {ratio:.1f}x - reduced position sizing recommended"

        brier_text = f"{mean_brier:.3f}" if mean_brier is not None else "n/a"
        return True, f"calibration acceptable: CRPS ratio {ratio:.1f}x, Brier {brier_text}"

    def get_all_city_status(self) -> dict[str, dict]:
        tracker = self._tracker()
        cities = {
            str(entry.get("city")).strip().lower()
            for entry in [*tracker.history, *tracker.brier_history]
            if entry.get("city")
        }

        status: dict[str, dict] = {}
        for city in sorted(cities):
            summary = tracker.get_calibration_summary(city=city, last_n_days=7)
            mean_brier = self._recent_brier_mean(tracker, city, last_n_days=7)
            tradeable, reason = self.is_tradeable(city)
            status[city] = {
                "tradeable": tradeable,
                "crps_ratio": round(float(summary.get("calibration_ratio", 0.0) or 0.0), 6),
                "brier": round(float(mean_brier), 6) if mean_brier is not None else None,
                "n_resolved": self._resolved_count(tracker, city),
                "reason": reason,
            }
        return status
