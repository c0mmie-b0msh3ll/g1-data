# SUBMIT

## Group Reflection

Our analysis treated the incident as an evidence-ordering problem instead of starting from the alert text. We first validated the telemetry shape, row counts, timestamp ranges, duplicate records, gaps, nulls, and baseline behavior. Then we compared a robust MAD detector, EWMA trend smoothing, and IsolationForest. The robust method was easiest to defend because each anomaly maps back to a concrete service and metric with a baseline threshold. IsolationForest was useful as a secondary check, while EWMA helped explain the slope and timing visually. For logs, we calibrated template extraction so important messages did not collapse into one generic upstream failure. The resulting timeline shows cart-service memory and GC pressure, cache eviction failures, OOMKilled events, restart growth, and downstream timeout/5xx propagation. The main lesson is that early operational signals were present before the page, but they required correlating metrics and template-level logs rather than reading isolated alerts.

## Contributions

- Member 1: metrics validation and robust MAD anomaly analysis.
- Member 2: IsolationForest and EWMA comparison.
- Member 3: log preprocessing and Drain-style template calibration.
- Member 4: incident timeline and root-cause synthesis.
- Member 5: dashboard, charts, and report packaging.
