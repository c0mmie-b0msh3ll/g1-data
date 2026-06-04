# AIOps W1 Offline Pipeline

Run from the repository root:

```bash
python w1/lab/analyze.py
```

The script reads raw telemetry from `g1/metrics` and `g1/logs`, runs EDA/calibration, applies the final detectors, and writes all artifacts to `outputs`.

Primary detector: robust 3-alpha/MAD using the first 6 hours as baseline and 3-of-5 persistence.

Relation to the classic 3-alpha rule:

```text
classic 3-alpha: mean + 3 * std
robust 3-alpha: median + 3 * 1.4826 * MAD
MAD = median(|x - median(x)|)
```

The `1.4826` factor scales MAD so it is comparable to standard deviation for approximately normal data. This keeps the same 3-alpha intuition while reducing sensitivity to outliers in the baseline.

Secondary detector: IsolationForest trained on the same baseline window.

Trend method: EWMA span 20 for visual smoothing.

Selected log template config: `{"drain_sim_th": 0.6, "drain_depth": 4, "drain_max_children": 100, "parametrize_numeric_tokens": true}`.

Key outputs:

- `outputs/eda_summary.csv`
- `outputs/method_comparison.csv`
- `outputs/detector_observability.csv`
- `outputs/drain_comparison.csv`
- `outputs/anomalies_metrics.csv`
- `outputs/log_templates.csv`
- `outputs/log_template_timeseries.csv`
- `outputs/incident_timeline.csv`
- `outputs/dashboard.html`
- `FINDINGS.md`
- `SUBMIT.md`
