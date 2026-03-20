from arena.intelligence.output_parser import normalize_llm_output, parse_decision_payload, validate_decision_payload


def valid_payload():
    return {
        "timestamp": "2026-03-19T00:00:00+00:00",
        "strategy_id": "llm_analyst",
        "markets_considered": ["m1"],
        "predicted_probability": 0.62,
        "market_implied_probability": 0.55,
        "expected_edge_bps": 700,
        "confidence": 0.7,
        "evidence_items": [{"source": "forecast", "content": "HKO higher than market"}],
        "risk_notes": "Forecasts can shift.",
        "exit_plan": "Hold to resolution.",
        "thinking": "Structured rationale.",
        "web_searches_used": [],
        "actions": [
            {
                "action_type": "BUY",
                "market_id": "m1",
                "venue": "polymarket",
                "outcome_id": "yes",
                "outcome_label": "Yes",
                "amount_usd": 50.0,
                "limit_price": 0.56,
                "reasoning_summary": "Positive edge.",
            }
        ],
        "no_action_reason": None,
    }


def test_output_parser_accepts_valid_payload():
    payload = valid_payload()
    validate_decision_payload(payload)
    decision = parse_decision_payload(payload, strategy_type="llm")
    assert decision.strategy_id == "llm_analyst"
    assert decision.actions[0].action_type == "BUY"


def test_output_parser_rejects_missing_fields():
    payload = valid_payload()
    payload.pop("thinking")
    try:
        validate_decision_payload(payload)
    except ValueError as exc:
        assert "Missing required fields" in str(exc)
    else:
        raise AssertionError("Expected validate_decision_payload to raise")


def test_normalize_llm_output_handles_nemotron_shapes():
    raw = {
        "reasoning": "The best trade is to buy the 27°C bucket because the forecast remains close enough to the threshold.",
        "predicted_probability": {"1609292": "0.82"},
        "market_implied_probability": {"1609292": "0.45"},
        "expected_edge_bps": {"1609292": "3700"},
        "confidence": {"1609292": "0.76"},
        "evidence_items": ["HKO reading 26.8C", {"source": "forecast", "detail": "Open-Meteo peak 27.5C"}],
        "risk_notes": ["Sea breeze could cap temperature"],
        "exit_plan": ["Hold to resolution"],
        "actions": [],
        "web_searches_used": [],
    }
    packet = [
        {
            "market_id": "1609292",
            "venue": "polymarket",
            "question": "Will the highest temperature in Hong Kong be 27°C on March 19?",
            "outcomes": [
                {"outcome_id": "yes_token", "label": "Yes"},
                {"outcome_id": "no_token", "label": "No"},
            ],
        }
    ]
    normalized = normalize_llm_output(raw, packet, max_order_usd=100.0)
    assert normalized["predicted_probability"] == 0.82
    assert normalized["confidence"] == 0.76
    assert normalized["expected_edge_bps"] == 3700
    assert normalized["evidence_items"][0]["source"] == "llm_stated"
    assert normalized["actions"][0]["market_id"] == "1609292"
    assert normalized["actions"][0]["outcome_label"] == "Yes"
    assert normalized["actions"][0]["amount_usd"] == 100.0
