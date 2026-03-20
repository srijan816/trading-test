from __future__ import annotations


SOURCE_BIAS = {
    "ecmwf": {
        "Hong Kong": 1.31,
        "Chicago": -0.90,
        "London": -0.38,
        "Tokyo": 0.59,
        "Seoul": 0.17,
        "Lucknow": -0.06,
        "_default": 0.0,
    },
    "gfs": {
        "Hong Kong": 0.10,
        "Chicago": 0.00,
        "London": -0.25,
        "Tokyo": 0.63,
        "Seoul": 0.38,
        "Lucknow": 1.64,
        "_default": 0.0,
    },
    "open_meteo": {
        "_default": 0.0,
    },
    "hko": {
        "Hong Kong": 0.0,
        "_default": 0.0,
    },
    "nvidia_fourcastnet": {
        "_default": 0.0,
    },
}


def _invert_source_bias(source_bias: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    by_location: dict[str, dict[str, float]] = {}
    for source, per_location in source_bias.items():
        for location, bias in per_location.items():
            if location == "_default":
                continue
            by_location.setdefault(location, {})[source] = float(bias)
    return by_location


REFERENCE_BIASES = _invert_source_bias(SOURCE_BIAS)
