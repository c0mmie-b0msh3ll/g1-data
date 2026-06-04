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

Final presentation deliverables:

- `outputs/presentations/shopx-aiops-final-rca-html.pptx`
- `outputs/presentations/shopx-aiops-final-html.html`
- `FINAL_PRESENTATION_SCRIPT.md`
- `FINAL_QNA.md`

The presentation is generated from HTML/CSS and notebook/chart assets:

```bash
python tools/build_html_powerpoint.py
```

This generator uses `diagrams`, `playwright`, `python-pptx`, and a local Graphviz
installation for the pipeline diagram.

Real-time simulated replay is split into a data pipeline and a frontend dashboard. Start the dashboard server, then click **Run Real-Time Workflow** in the page:

```bash
python w1/lab/realtime_dashboard.py
```

Dashboard URL:

```text
http://127.0.0.1:8765
```

The dashboard button triggers the full local workflow: stream/replay data -> calculate detector signals -> detect anomalies -> RCA.

`realtime.py` is the stream/data pipeline script behind that button. It can also be run directly:

```bash
python w1/lab/realtime.py
```

It writes data artifacts only:

- `outputs/realtime/events.jsonl`
- `outputs/realtime/alerts.jsonl`
- `outputs/realtime/rca_timeline.json`
- `outputs/realtime/rca_hypotheses.json`

`realtime_dashboard.py` serves the frontend, exposes the local `/api/run` trigger, reads those artifacts, and writes a static snapshot for review:

- `outputs/realtime/dashboard.html`
