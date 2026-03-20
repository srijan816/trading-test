from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional


@dataclass
class MarketResearchEdgeAssessment:
    model_probability: float = 0.0
    market_probability: float = 0.0
    raw_edge_bps: int = 0
    adjusted_edge_bps: int = 0
    recommendation: Literal["STRONG_BUY_YES", "BUY_YES", "HOLD", "BUY_NO", "STRONG_BUY_NO"] = "HOLD"
    risk_factors: List[str] = field(default_factory=list)
    kelly_fraction: Optional[float] = None


@dataclass
class MarketResearchRequest:
    question: str
    market_type: str = "auto"  # "weather" | "event" | "crypto" | "sports" | "politics" | "entertainment" | "other" | "auto"
    market_data: Dict = field(default_factory=dict)
    ensemble_data: Optional[Dict] = None
    calibration_data: Optional[Dict] = None
    model: str = ""  # empty = use Nexus default
    search_depth: str = "standard"  # "quick" | "standard" | "deep"


@dataclass
class MarketResearchResponse:
    probability: float
    confidence: str  # "high" | "medium" | "low"
    reasoning: str
    reasoning_trace: Optional[str] = None
    edge_assessment: MarketResearchEdgeAssessment = field(default_factory=MarketResearchEdgeAssessment)
    sources: List[Dict] = field(default_factory=list)
    search_queries_used: List[str] = field(default_factory=list)
    model_used: str = ""
    tokens_used: Dict = field(default_factory=dict)
    duration_ms: int = 0
    market_type: str = "other"
    from_cache: bool = False
    ensemble_probability: Optional[float] = None
    llm_probability: Optional[float] = None
    ensemble_override_triggered: bool = False
