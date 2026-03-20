from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
from properscoring import crps_gaussian, threshold_brier_score

logger = logging.getLogger("mirofish.crps")

CRPS_LOG_PATH = Path(__file__).resolve().parents[3] / "data" / "crps_history.jsonl"


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

    def __init__(self) -> None:
        self.history: list[dict] = []
        self._load_history()

    def _load_history(self) -> None:
        CRPS_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        if CRPS_LOG_PATH.exists():
            with open(CRPS_LOG_PATH, encoding="utf-8") as handle:
                self.history = [json.loads(line) for line in handle if line.strip()]

    def record(
        self,
        market_id: str,
        observation: float,
        mu: float,
        sigma: float,
        city: str,
        target_date: str,
        sources: dict | None = None,
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
        # Deduplication: skip if an identical record was written in the last hour
        one_hour_ago = datetime.utcnow().timestamp() - 3600
        for existing in reversed(self.history):
            if existing.get("city", "").lower() != city.lower():
                continue
            if existing.get("target_date") != target_date:
                continue
            existing_ts = existing.get("timestamp", "")
            try:
                ts_epoch = datetime.fromisoformat(existing_ts).timestamp()
            except (ValueError, TypeError):
                continue
            if ts_epoch < one_hour_ago:
                break  # history is chronological; no need to check further
            if (
                abs(existing.get("mu", 0) - mu) < 0.01
                and abs(existing.get("sigma", 0) - sigma) < 0.01
            ):
                logger.info(
                    "CRPS duplicate skipped for %s %s (mu=%.2f sigma=%.2f) — "
                    "existing record within 1 hour",
                    city, target_date, mu, sigma,
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
            "timestamp": datetime.utcnow().isoformat(),
            "market_id": market_id,
            "city": city,
            "target_date": target_date,
            "observation": float(observation),
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

        with open(CRPS_LOG_PATH, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry) + "\n")

        logger.info(
            f"CRPS for {city} {target_date}: {crps_value:.4f} "
            f"(ideal: {ideal_crps:.4f}, ratio: {calibration_ratio:.3f}, "
            f"grad_sigma: {grad_sigma:.4f})"
        )

        return entry

    def suggest_sigma_adjustment(self, city: str | None = None, last_n: int = 20) -> dict:
        """
        Use accumulated CRPS gradients to suggest sigma adjustment.

        If avg grad_sigma > 0: sigma is too large (overconfident uncertainty)
        If avg grad_sigma < 0: sigma is too small (underconfident)

        Returns dict with suggested_multiplier for sigma_calibration.json
        """
        relevant = [h for h in self.history if (city is None or h["city"] == city)]
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
