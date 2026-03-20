from __future__ import annotations

import json
import logging
import statistics
from collections import deque
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
from properscoring import crps_gaussian, threshold_brier_score

logger = logging.getLogger("mirofish.crps")

CRPS_LOG_PATH = Path(__file__).resolve().parents[3] / "data" / "crps_history.jsonl"
BRIER_LOG_PATH = Path(__file__).resolve().parents[3] / "data" / "brier_history.jsonl"


class CRPSTracker:
    """
    Tracks CRPS (Continuous Ranked Probability Score) for every resolved market.
    CRPS evaluates the full probability distribution, not just binary threshold.
    Lower CRPS = better calibrated forecasts.

    Key insight: CRPS for a Gaussian forecast N(mu, sigma^2) against observation x is:
    CRPS = sigma * [sx*(2*Phi(sx)-1) + 2*phi(sx) - 1/sqrt(pi)]
    where sx = (x-mu)/sigma

    Perfect calibration reference: CRPS of a perfectly calibrated N(0,1) = 1/sqrt(pi) ≈ 0.564
    So for a forecast with sigma=2, perfect CRPS would be ~1.128.
    """

    def __init__(
        self,
        crps_history_path: str | Path | None = None,
        brier_history_path: str | Path | None = None,
    ) -> None:
        self.crps_history_path = Path(crps_history_path) if crps_history_path else CRPS_LOG_PATH
        self.brier_history_path = Path(brier_history_path) if brier_history_path else BRIER_LOG_PATH
        self.history: list[dict] = []
        self.brier_history: list[dict] = []
        self._load_history()

    def _load_history(self) -> None:
        self.crps_history_path.parent.mkdir(parents=True, exist_ok=True)
        self.brier_history_path.parent.mkdir(parents=True, exist_ok=True)
        if self.crps_history_path.exists():
            with open(self.crps_history_path, encoding="utf-8") as handle:
                self.history = [json.loads(line) for line in handle if line.strip()]
        if self.brier_history_path.exists():
            with open(self.brier_history_path, encoding="utf-8") as handle:
                self.brier_history = [json.loads(line) for line in handle if line.strip()]

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
    def _recent_entries(path: Path, limit: int) -> list[dict]:
        if not path.exists():
            return []
        recent: deque[str] = deque(maxlen=limit)
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    recent.append(line)
        return [json.loads(line) for line in recent]

    @staticmethod
    def _matches_city(entry: dict, city: str) -> bool:
        return str(entry.get("city", "")).strip().lower() == str(city).strip().lower()

    @staticmethod
    def _matches_metric(entry: dict, metric: str | None) -> bool:
        if metric is None:
            return True
        return str(entry.get("metric", "high")).strip().lower() == str(metric).strip().lower()

    @staticmethod
    def _clamp_probability(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    def _recent_crps_duplicate(
        self,
        city: str,
        target_date: str,
        mu: float,
        sigma: float,
    ) -> bool:
        recent_entries = self._recent_entries(self.crps_history_path, limit=10)
        for existing in reversed(recent_entries):
            if not self._matches_city(existing, city):
                continue
            if existing.get("target_date") != target_date:
                continue
            existing_mu = float(existing.get("mu", 0.0) or 0.0)
            existing_sigma = float(existing.get("sigma", 0.0) or 0.0)
            if abs(existing_mu - float(mu)) < 0.01 and abs(existing_sigma - float(sigma)) < 0.01:
                return True
            return True
        return False

    def _recent_brier_duplicate(self, market_id: str, target_date: str | None) -> bool:
        recent_entries = self._recent_entries(self.brier_history_path, limit=20)
        for existing in reversed(recent_entries):
            if str(existing.get("market_id", "")) != str(market_id):
                continue
            if target_date and existing.get("target_date") != target_date:
                continue
            return True
        return False

    def record(
        self,
        market_id: str,
        observation: float,
        mu: float,
        sigma: float,
        city: str,
        target_date: str,
        metric: str | None = None,
        sources: dict | None = None,
        observed_high_c: float | None = None,
        observed_low_c: float | None = None,
        observation_source: str | None = None,
        observation_timestamp: str | None = None,
        observation_secondary_source: str | None = None,
        observation_secondary_high_c: float | None = None,
        observation_secondary_low_c: float | None = None,
        observation_disagreement_c: float | None = None,
    ) -> dict | None:
        """
        Record CRPS for a resolved market.

        Returns None if a near-duplicate record (same city, target_date, mu, sigma)
        already exists in the last hour.

        Args:
            observation: actual observed temperature (°F)
            mu: ensemble mean forecast (°F)
            sigma: ensemble spread/uncertainty (°F)
        """
        # Robust deduplication: concurrent settlement hooks may hold stale in-memory state,
        # so we tail the JSONL file before appending and collapse repeated city/date writes.
        if self._recent_crps_duplicate(city=city, target_date=target_date, mu=mu, sigma=sigma):
            logger.info(
                "CRPS duplicate skipped for %s %s (mu=%.2f sigma=%.2f) — recent record already exists",
                city,
                target_date,
                mu,
                sigma,
            )
            return None

        sigma = max(float(sigma), 1e-6)

        crps_value = float(crps_gaussian(observation, mu, sigma))
        _, grads = crps_gaussian(observation, mu, sigma, grad=True)
        grad_mu = float(grads[0])
        grad_sigma = float(grads[1])

        thresholds = np.arange(mu - 3 * sigma, mu + 3 * sigma + 1.0, 1.0)
        if thresholds.size == 0:
            thresholds = np.array([mu], dtype=float)
        threshold_scores = threshold_brier_score(
            np.array([observation], dtype=float),
            np.array([[mu]], dtype=float),
            thresholds,
        )
        worst_idx = int(np.argmax(threshold_scores[0]))
        worst_threshold = float(thresholds[worst_idx])
        worst_brier = float(threshold_scores[0][worst_idx])

        ideal_crps = sigma / np.sqrt(np.pi)
        calibration_ratio = crps_value / ideal_crps if ideal_crps > 0 else float("inf")

        entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "market_id": market_id,
            "city": city,
            "target_date": target_date,
            "metric": str(metric or "high").strip().lower(),
            "observation": float(observation),
            "observed_high_c": float(observed_high_c) if observed_high_c is not None else None,
            "observed_low_c": float(observed_low_c) if observed_low_c is not None else None,
            "observation_source": observation_source,
            "observation_timestamp": observation_timestamp,
            "observation_secondary_source": observation_secondary_source,
            "observation_secondary_high_c": (
                float(observation_secondary_high_c) if observation_secondary_high_c is not None else None
            ),
            "observation_secondary_low_c": (
                float(observation_secondary_low_c) if observation_secondary_low_c is not None else None
            ),
            "observation_disagreement_c": (
                float(observation_disagreement_c) if observation_disagreement_c is not None else None
            ),
            "mu": float(mu),
            "sigma": sigma,
            "crps": crps_value,
            "grad_mu": grad_mu,
            "grad_sigma": grad_sigma,
            "calibration_ratio": calibration_ratio,
            "worst_threshold": worst_threshold,
            "worst_threshold_brier": worst_brier,
            "sources": sources or {},
        }

        self.history.append(entry)

        with open(self.crps_history_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry) + "\n")

        logger.info(
            f"CRPS for {city} {target_date}: {crps_value:.4f} "
            f"(ideal: {ideal_crps:.4f}, ratio: {calibration_ratio:.3f}, "
            f"grad_sigma: {grad_sigma:.4f})"
        )

        return entry

    def record_brier(
        self,
        city: str,
        target_date: str,
        market_id: str,
        question: str,
        forecast_prob: float,
        actual_outcome: float,
        metric: str | None = None,
        observed_high_c: float | None = None,
        observed_low_c: float | None = None,
        observation_source: str | None = None,
        observation_timestamp: str | None = None,
        observation_secondary_source: str | None = None,
        observation_secondary_high_c: float | None = None,
        observation_secondary_low_c: float | None = None,
        observation_disagreement_c: float | None = None,
    ) -> dict | None:
        if self._recent_brier_duplicate(market_id=market_id, target_date=target_date):
            logger.info("Brier duplicate skipped for %s (%s)", market_id, target_date)
            return None

        forecast_probability = self._clamp_probability(forecast_prob)
        actual = 1.0 if float(actual_outcome) >= 0.5 else 0.0
        brier_score = (forecast_probability - actual) ** 2
        entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "city": city,
            "target_date": target_date,
            "metric": str(metric or "high").strip().lower(),
            "market_id": market_id,
            "question": question,
            "forecast_probability": round(forecast_probability, 6),
            "actual_outcome": int(actual),
            "brier_score": round(float(brier_score), 6),
            "observed_high_c": float(observed_high_c) if observed_high_c is not None else None,
            "observed_low_c": float(observed_low_c) if observed_low_c is not None else None,
            "observation_source": observation_source,
            "observation_timestamp": observation_timestamp,
            "observation_secondary_source": observation_secondary_source,
            "observation_secondary_high_c": (
                float(observation_secondary_high_c) if observation_secondary_high_c is not None else None
            ),
            "observation_secondary_low_c": (
                float(observation_secondary_low_c) if observation_secondary_low_c is not None else None
            ),
            "observation_disagreement_c": (
                float(observation_disagreement_c) if observation_disagreement_c is not None else None
            ),
        }
        self.brier_history.append(entry)
        with open(self.brier_history_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry) + "\n")
        return entry

    def get_calibration_summary(
        self,
        city: str | None = None,
        last_n_days: int = 30,
        metric: str | None = None,
    ) -> dict:
        self._load_history()
        cutoff = datetime.now(UTC) - timedelta(days=max(int(last_n_days), 1))

        crps_records = [
            entry
            for entry in self.history
            if (city is None or self._matches_city(entry, city))
            and self._matches_metric(entry, metric)
            and (timestamp := self._parse_timestamp(entry.get("timestamp"))) is not None
            and timestamp >= cutoff
        ]
        brier_records = [
            entry
            for entry in self.brier_history
            if (city is None or self._matches_city(entry, city))
            and self._matches_metric(entry, metric)
            and (timestamp := self._parse_timestamp(entry.get("timestamp"))) is not None
            and timestamp >= cutoff
        ]

        crps_values = [float(entry["crps"]) for entry in crps_records if entry.get("crps") is not None]
        sigma_values = [float(entry["sigma"]) for entry in crps_records if entry.get("sigma") is not None]
        brier_values = [float(entry["brier_score"]) for entry in brier_records if entry.get("brier_score") is not None]

        mean_crps = float(statistics.fmean(crps_values)) if crps_values else 0.0
        median_crps = float(statistics.median(crps_values)) if crps_values else 0.0
        mean_brier = float(statistics.fmean(brier_values)) if brier_values else 0.0
        perfect_crps = float(statistics.fmean([sigma / np.sqrt(np.pi) for sigma in sigma_values])) if sigma_values else 0.0
        calibration_ratio = (mean_crps / perfect_crps) if perfect_crps > 0 else 0.0

        ratios = [float(entry.get("calibration_ratio", 0.0) or 0.0) for entry in crps_records]
        sigma_trend = "stable"
        if len(ratios) >= 20:
            previous = statistics.fmean(ratios[-20:-10])
            recent = statistics.fmean(ratios[-10:])
            if recent <= previous * 0.9:
                sigma_trend = "improving"
            elif recent >= previous * 1.1:
                sigma_trend = "degrading"

        city_label = city or "all"
        scope_label = f"{city_label}/{metric}" if metric else city_label
        if len(crps_records) < 5:
            recommended_action = f"collect at least 5 resolved markets for {scope_label}"
        elif calibration_ratio > 5.0:
            recommended_action = f"widen sigma by 2.0x for {scope_label}"
        elif calibration_ratio > 2.0:
            recommended_action = f"widen sigma by 1.5x for {scope_label}"
        elif calibration_ratio > 1.5:
            recommended_action = f"widen sigma by 1.2x for {scope_label}"
        elif calibration_ratio < 0.8:
            recommended_action = f"narrow sigma by 0.9x for {scope_label}"
        else:
            recommended_action = f"hold sigma steady for {scope_label}"

        return {
            "city": city_label,
            "metric": metric or "all",
            "n_records": len(crps_records),
            "mean_crps": round(mean_crps, 6),
            "median_crps": round(median_crps, 6),
            "mean_brier": round(mean_brier, 6),
            "calibration_ratio": round(calibration_ratio, 6),
            "sigma_trend": sigma_trend,
            "recommended_action": recommended_action,
        }

    def suggest_sigma_adjustment(self, city: str | None = None, last_n: int = 20) -> dict:
        """
        Use accumulated CRPS gradients to suggest sigma adjustment.

        If avg grad_sigma > 0: sigma is too large (overconfident uncertainty)
        If avg grad_sigma < 0: sigma is too small (underconfident)

        Returns dict with suggested_multiplier for sigma_calibration.json
        """
        self._load_history()
        relevant = [h for h in self.history if (city is None or self._matches_city(h, city))]
        if len(relevant) < 5:
            return {"status": "insufficient_data", "count": len(relevant)}

        recent = relevant[-last_n:]
        avg_grad_sigma = float(np.mean([h["grad_sigma"] for h in recent]))
        avg_grad_mu = float(np.mean([h["grad_mu"] for h in recent]))
        avg_crps = float(np.mean([h["crps"] for h in recent]))
        avg_ratio = float(np.mean([h["calibration_ratio"] for h in recent]))

        if avg_grad_sigma > 0.01:
            suggested_multiplier = 0.98
            direction = "decrease"
        elif avg_grad_sigma < -0.01:
            suggested_multiplier = 1.02
            direction = "increase"
        else:
            suggested_multiplier = 1.0
            direction = "hold"

        return {
            "status": "ready",
            "city": city or "all",
            "sample_size": len(recent),
            "avg_crps": round(avg_crps, 4),
            "avg_grad_sigma": round(avg_grad_sigma, 4),
            "avg_grad_mu": round(avg_grad_mu, 4),
            "avg_calibration_ratio": round(avg_ratio, 3),
            "direction": direction,
            "suggested_sigma_multiplier": suggested_multiplier,
        }
