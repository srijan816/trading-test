# Sigma Calibration

- Date of analysis: 2026-03-20
- Data sources: Historical Forecast API + Previous Runs API (`best_match`, `ecmwf_ifs025`, `gfs_seamless`)
- Data range: 2026-01-18 to 2026-03-18
- Location-days analyzed: 360

## Data Quality Caveats

- best_match for Hong Kong matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.
- best_match for Chicago matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.
- best_match for London matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.
- best_match for Tokyo matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.
- best_match for Seoul matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.
- best_match for Lucknow matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.
- gfs_seamless for Chicago matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.

## Per-Source RMSE and Bias

| Source | Samples | Bias (C) | RMSE (C) | Std (C) | P05 | P95 | Skew |
| --- | --- | --- | --- | --- | --- | --- | --- |
| best_match | 360 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| ecmwf_ifs025 | 360 | 0.12 | 1.08 | 1.07 | -1.60 | 1.91 | 0.00 |
| gfs_seamless | 360 | 0.42 | 1.20 | 1.13 | -1.00 | 2.51 | 0.95 |
| ensemble | 360 | 0.20 | 1.04 | 1.02 | -1.50 | 1.75 | 0.00 |

## Ensemble vs Single Source

| Location | Ensemble RMSE | Best Single RMSE | Best Model |
| --- | --- | --- | --- |
| Hong Kong | 0.94 | 0.75 | gfs_seamless |
| Chicago | 1.37 | 1.37 | ecmwf_ifs025 |
| London | 0.82 | 0.81 | ecmwf_ifs025 |
| Tokyo | 0.96 | 1.03 | ecmwf_ifs025 |
| Seoul | 0.80 | 0.62 | ecmwf_ifs025 |
| Lucknow | 1.22 | 0.80 | ecmwf_ifs025 |

## Global Sigma Calibration

| Method | σ Mult | Brier | LogLoss | Best |
| --- | --- | --- | --- | --- |
| A | 0.30 | 0.059 | 0.272 |  |
| A | 0.40 | 0.056 | 0.237 |  |
| A | 0.50 | 0.055 | 0.213 |  |
| A | 0.60 | 0.053 | 0.196 |  |
| A | 0.70 | 0.053 | 0.186 |  |
| A | 0.80 | 0.052 | 0.180 |  |
| A | 0.90 | 0.052 | 0.177 |  |
| A | 1.00 | 0.053 | 0.176 |  |
| A | 1.10 | 0.053 | 0.178 |  |
| A | 1.20 | 0.054 | 0.180 |  |
| A | 1.30 | 0.054 | 0.184 |  |
| A | 1.40 | 0.055 | 0.188 |  |
| A | 1.50 | 0.057 | 0.194 |  |
| A | 1.70 | 0.059 | 0.206 |  |
| A | 2.00 | 0.063 | 0.225 |  |
| A | 2.50 | 0.072 | 0.260 |  |
| A | 3.00 | 0.081 | 0.294 |  |
| B | 0.30 | 0.058 | 0.258 |  |
| B | 0.40 | 0.055 | 0.224 |  |
| B | 0.50 | 0.054 | 0.202 |  |
| B | 0.60 | 0.053 | 0.188 |  |
| B | 0.70 | 0.052 | 0.179 |  |
| B | 0.80 | 0.052 | 0.175 |  |
| B | 0.90 | 0.052 | 0.174 |  |
| B | 1.00 | 0.052 | 0.175 |  |
| B | 1.10 | 0.053 | 0.178 |  |
| B | 1.20 | 0.054 | 0.182 |  |
| B | 1.30 | 0.055 | 0.187 |  |
| B | 1.40 | 0.056 | 0.192 |  |
| B | 1.50 | 0.058 | 0.199 |  |
| B | 1.70 | 0.061 | 0.212 |  |
| B | 2.00 | 0.066 | 0.233 |  |
| B | 2.50 | 0.075 | 0.270 |  |
| B | 3.00 | 0.084 | 0.304 |  |
| C | 0.30 | 0.060 | 0.292 |  |
| C | 0.40 | 0.058 | 0.253 |  |
| C | 0.50 | 0.056 | 0.224 |  |
| C | 0.60 | 0.054 | 0.204 |  |
| C | 0.70 | 0.053 | 0.190 |  |
| C | 0.80 | 0.052 | 0.180 |  |
| C | 0.90 | 0.051 | 0.174 |  |
| C | 1.00 | 0.051 | 0.170 |  |
| C | 1.10 | 0.051 | 0.169 | best |
| C | 1.20 | 0.051 | 0.169 |  |
| C | 1.30 | 0.052 | 0.171 |  |
| C | 1.40 | 0.052 | 0.173 |  |
| C | 1.50 | 0.053 | 0.176 |  |
| C | 1.70 | 0.054 | 0.183 |  |
| C | 2.00 | 0.057 | 0.198 |  |
| C | 2.50 | 0.063 | 0.225 |  |
| C | 3.00 | 0.070 | 0.254 |  |

Best overall calibration was Method C with σ×1.10 (Brier 0.051).

## Per-Location Results

| Location | Best Method | Best σ× | Brier | RMSE | Temp Range |
| --- | --- | --- | --- | --- | --- |
| Hong Kong | C | 1.00 | 0.035 | 0.9C | 14.1 to 24.7C |
| Chicago | A | 1.20 | 0.084 | 1.4C | -12.8 to 22.1C |
| London | C | 1.50 | 0.050 | 0.8C | 5.9 to 18.8C |
| Tokyo | B | 0.60 | 0.039 | 1.0C | 1.8 to 21.2C |
| Seoul | C | 0.90 | 0.030 | 0.8C | -7.8 to 15.1C |
| Lucknow | C | 0.90 | 0.040 | 1.2C | 18.4 to 35.2C |

## Calibration Curve

| Predicted Bin | Mean Pred | Mean Actual | Count | Deviation |
| --- | --- | --- | --- | --- |
| 0.0-0.1 | 0.011 | 0.014 | 2775 | 0.003 |
| 0.1-0.2 | 0.145 | 0.150 | 300 | 0.005 |
| 0.2-0.3 | 0.245 | 0.254 | 213 | 0.008 |
| 0.3-0.4 | 0.349 | 0.357 | 182 | 0.009 |
| 0.4-0.5 | 0.446 | 0.462 | 171 | 0.016 |
| 0.5-0.6 | 0.553 | 0.652 | 181 | 0.099 |
| 0.6-0.7 | 0.653 | 0.652 | 181 | 0.001 |
| 0.7-0.8 | 0.757 | 0.818 | 220 | 0.062 |
| 0.8-0.9 | 0.856 | 0.862 | 289 | 0.005 |
| 0.9-1.0 | 0.990 | 0.988 | 3048 | 0.002 |

Overall calibration quality: **GOOD**

## Error Distribution

```text
  -4 to -3 |    1 
  -3 to -2 |    9 #
  -2 to -1 |   27 ####
  -1 to  0 |  100 ###############
   0 to  1 |  156 ########################
   1 to  2 |   56 #########
   2 to  3 |    7 #
   3 to  4 |    3 
   4 to  5 |    1 
```

## Recommendations

1. Use Method C in `algo_forecast.py` for sigma estimation.
2. Default the sigma multiplier to 1.10; this was the best global fit by Brier score.
3. Per-location tuning changes the best fit materially, so keeping location overrides is justified.
4. The most accurate non-suspicious single source over this sample was `ecmwf_ifs025` by global RMSE.
5. The current source-disagreement sigma approach (Method B) was worse than the best alternative on this calibration set.
6. Reference ensemble RMSE for Method A comparisons was 1.04C.
7. Quasi-actual series were detected and excluded from the ensemble calibration path: best_match for Hong Kong matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.; best_match for Chicago matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.; best_match for London matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.; best_match for Tokyo matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.; best_match for Seoul matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.; best_match for Lucknow matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.; gfs_seamless for Chicago matched actuals almost perfectly (RMSE 0.00C) and was excluded from ensemble calibration.

## Per-Location Source Detail

### Hong Kong

| Source | Count | Bias (C) | RMSE (C) | Std (C) | Suspicious? |
| --- | --- | --- | --- | --- | --- |
| best_match | 60 | 0.00 | 0.00 | 0.00 | yes |
| ecmwf_ifs025 | 60 | 1.31 | 1.55 | 0.83 | no |
| gfs_seamless | 60 | 0.10 | 0.75 | 0.75 | no |

### Chicago

| Source | Count | Bias (C) | RMSE (C) | Std (C) | Suspicious? |
| --- | --- | --- | --- | --- | --- |
| best_match | 60 | 0.00 | 0.00 | 0.00 | yes |
| ecmwf_ifs025 | 60 | -0.90 | 1.37 | 1.04 | no |
| gfs_seamless | 60 | 0.00 | 0.00 | 0.00 | yes |

### London

| Source | Count | Bias (C) | RMSE (C) | Std (C) | Suspicious? |
| --- | --- | --- | --- | --- | --- |
| best_match | 60 | 0.00 | 0.00 | 0.00 | yes |
| ecmwf_ifs025 | 60 | -0.38 | 0.81 | 0.72 | no |
| gfs_seamless | 60 | -0.25 | 0.93 | 0.90 | no |

### Tokyo

| Source | Count | Bias (C) | RMSE (C) | Std (C) | Suspicious? |
| --- | --- | --- | --- | --- | --- |
| best_match | 60 | 0.00 | 0.00 | 0.00 | yes |
| ecmwf_ifs025 | 60 | 0.59 | 1.03 | 0.85 | no |
| gfs_seamless | 60 | 0.63 | 1.19 | 1.02 | no |

### Seoul

| Source | Count | Bias (C) | RMSE (C) | Std (C) | Suspicious? |
| --- | --- | --- | --- | --- | --- |
| best_match | 60 | 0.00 | 0.00 | 0.00 | yes |
| ecmwf_ifs025 | 60 | 0.17 | 0.62 | 0.60 | no |
| gfs_seamless | 60 | 0.38 | 1.26 | 1.21 | no |

### Lucknow

| Source | Count | Bias (C) | RMSE (C) | Std (C) | Suspicious? |
| --- | --- | --- | --- | --- | --- |
| best_match | 60 | 0.00 | 0.00 | 0.00 | yes |
| ecmwf_ifs025 | 60 | -0.06 | 0.80 | 0.80 | no |
| gfs_seamless | 60 | 1.64 | 2.07 | 1.28 | no |
