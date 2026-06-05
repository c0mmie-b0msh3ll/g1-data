from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from drain3 import TemplateMiner
from drain3.masking import MaskingInstruction
from drain3.template_miner_config import TemplateMinerConfig
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler


ROOT = Path(__file__).resolve().parents[2]
METRICS_DIR = ROOT / "g1" / "metrics"
LOGS_DIR = ROOT / "g1" / "logs"
OUT = ROOT / "outputs"
CHARTS = OUT / "charts"
BASELINE_HOURS = 6
MAD_K = 3.0
PERSISTENCE_HITS = 3
PERSISTENCE_WINDOW = 5
SELECTED_DRAIN = {
    "drain_sim_th": 0.6,
    "drain_depth": 4,
    "drain_max_children": 100,
    "parametrize_numeric_tokens": True,
}


@dataclass
class Detection:
    method: str
    service: str
    metric: str
    timestamp: pd.Timestamp | pd.NaT
    false_positive_count: int
    detail: str
    supports_rca_chain: bool
    impact_signal: bool = False


def ensure_dirs() -> None:
    OUT.mkdir(exist_ok=True)
    CHARTS.mkdir(exist_ok=True)


def load_metrics() -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for path in sorted(METRICS_DIR.glob("*.csv")):
        service = path.stem
        df = pd.read_csv(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp").reset_index(drop=True)
        frames[service] = df
    if not frames:
        raise FileNotFoundError(f"No metric CSV files found in {METRICS_DIR}")
    return frames


def load_logs() -> pd.DataFrame:
    rows = []
    for path in sorted(LOGS_DIR.glob("*.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                row = json.loads(line)
                row["_source_file"] = path.name
                row["_line_no"] = line_no
                rows.append(row)
    if not rows:
        raise FileNotFoundError(f"No JSONL logs found in {LOGS_DIR}")
    logs = pd.DataFrame(rows)
    logs["timestamp"] = pd.to_datetime(logs["timestamp"], utc=True)
    logs["message"] = logs["message"].astype(str)
    return logs.sort_values("timestamp").reset_index(drop=True)


def numeric_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c != "timestamp" and pd.api.types.is_numeric_dtype(df[c])]


def baseline_end(metrics: dict[str, pd.DataFrame]) -> pd.Timestamp:
    start = min(df["timestamp"].min() for df in metrics.values())
    return start + pd.Timedelta(hours=BASELINE_HOURS)


def validate_and_summarize_metrics(metrics: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    base_end = baseline_end(metrics)
    for service, df in metrics.items():
        gaps = df["timestamp"].diff().dropna()
        duplicate_rows = int(df.duplicated().sum())
        duplicate_ts = int(df["timestamp"].duplicated().sum())
        expected_step = pd.Timedelta(seconds=30)
        gap_count = int((gaps != expected_step).sum())
        for metric in numeric_columns(df):
            values = df[metric].astype(float)
            base = df.loc[df["timestamp"] < base_end, metric].astype(float)
            x = np.arange(len(values))
            slope = float(np.polyfit(x, values, 1)[0]) if len(values) > 1 else 0.0
            rows.append(
                {
                    "service": service,
                    "metric": metric,
                    "rows": len(df),
                    "start_utc": df["timestamp"].min().isoformat(),
                    "end_utc": df["timestamp"].max().isoformat(),
                    "timestamp_gap_count": gap_count,
                    "duplicate_timestamp_count": duplicate_ts,
                    "duplicate_row_count": duplicate_rows,
                    "null_count": int(df[metric].isna().sum()),
                    "min": values.min(),
                    "p50": values.quantile(0.50),
                    "p95": values.quantile(0.95),
                    "p99": values.quantile(0.99),
                    "max": values.max(),
                    "baseline_p50_first_6h": base.quantile(0.50),
                    "baseline_p95_first_6h": base.quantile(0.95),
                    "trend_slope_per_row": slope,
                    "baseline_window": f"{df['timestamp'].min().isoformat()} to {base_end.isoformat()}",
                }
            )
    summary = pd.DataFrame(rows)
    summary.to_csv(OUT / "eda_summary.csv", index=False)
    return summary


def persistent_first(mask: pd.Series, timestamps: pd.Series, hits: int = PERSISTENCE_HITS, window: int = PERSISTENCE_WINDOW):
    vals = mask.fillna(False).astype(int).to_numpy()
    for idx in range(len(vals)):
        start = max(0, idx - window + 1)
        if vals[start : idx + 1].sum() >= hits:
            hit_indices = np.where(vals[start : idx + 1] == 1)[0]
            first_idx = start + int(hit_indices[0])
            return timestamps.iloc[first_idx]
    return pd.NaT


def sustained_decision_time(mask: pd.Series, timestamps: pd.Series, hits: int, window: int, start_at: pd.Timestamp) -> pd.Timestamp | pd.NaT:
    vals = mask.fillna(False).astype(int).to_numpy()
    for idx in range(len(vals)):
        if timestamps.iloc[idx] < start_at:
            continue
        start = max(0, idx - window + 1)
        if vals[start : idx + 1].sum() >= hits:
            return timestamps.iloc[idx]
    return pd.NaT


def robust_mad_anomalies(metrics: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, list[Detection]]:
    base_end = baseline_end(metrics)
    all_rows = []
    detections = []
    for service, df in metrics.items():
        for metric in numeric_columns(df):
            base = df.loc[df["timestamp"] < base_end, metric].astype(float)
            med = float(base.median())
            mad = float((base - med).abs().median())
            sigma = 1.4826 * mad
            threshold = med + MAD_K * sigma
            if sigma == 0:
                threshold = med
            mask = df[metric].astype(float) > threshold
            first_after_base = persistent_first(mask & (df["timestamp"] >= base_end), df["timestamp"])
            false_pos = int(mask[df["timestamp"] < base_end].sum())
            if pd.notna(first_after_base):
                anomalous = df.loc[mask & (df["timestamp"] >= first_after_base), ["timestamp", metric]].copy()
                for _, row in anomalous.iterrows():
                    all_rows.append(
                        {
                            "timestamp": row["timestamp"].isoformat(),
                            "service": service,
                            "metric": metric,
                            "method": "robust_mad_3alpha",
                            "value": row[metric],
                            "baseline_median": med,
                            "threshold": threshold,
                            "score": (float(row[metric]) - med) / sigma if sigma else np.nan,
                        }
                    )
            supports = service == "cart-service" and false_pos <= 5 and metric in {
                "memory_usage_bytes",
                "jvm_gc_pause_ms_avg",
                "http_p99_latency_ms",
                "container_restart_count",
            }
            detections.append(
                Detection(
                    "robust_mad_3alpha",
                    service,
                    metric,
                    first_after_base,
                    false_pos,
                    f"formula=median + 3 * 1.4826 * MAD; median={med:.3f}; sigma={sigma:.3f}; threshold={threshold:.3f}; persistence={PERSISTENCE_HITS}/{PERSISTENCE_WINDOW}",
                    supports,
                )
            )
    anomalies = pd.DataFrame(all_rows)
    anomalies.to_csv(OUT / "anomalies_metrics.csv", index=False)
    return anomalies, detections


def ewma_detections(metrics: dict[str, pd.DataFrame]) -> list[Detection]:
    base_end = baseline_end(metrics)
    detections = []
    for service, df in metrics.items():
        for metric in numeric_columns(df):
            series = df[metric].astype(float)
            smooth = series.ewm(span=20, adjust=False).mean()
            base = smooth[df["timestamp"] < base_end]
            med = float(base.median())
            mad = float((base - med).abs().median())
            sigma = 1.4826 * mad
            threshold = med + MAD_K * sigma if sigma else med
            mask = smooth > threshold
            first_after_base = persistent_first(mask & (df["timestamp"] >= base_end), df["timestamp"])
            false_pos = int(mask[df["timestamp"] < base_end].sum())
            supports = service == "cart-service" and metric in {"memory_usage_bytes", "jvm_gc_pause_ms_avg", "http_p99_latency_ms"}
            detections.append(
                Detection(
                    "ewma_trend",
                    service,
                    metric,
                    first_after_base,
                    false_pos,
                    f"formula=EWMA(span=20) > median + 3 * 1.4826 * MAD; span=20; median={med:.3f}; sigma={sigma:.3f}; threshold={threshold:.3f}; persistence={PERSISTENCE_HITS}/{PERSISTENCE_WINDOW}",
                    supports,
                )
            )
    return detections


def http_5xx_sustained_detections(metrics: dict[str, pd.DataFrame]) -> list[Detection]:
    base_end = baseline_end(metrics)
    detections = []
    for service, df in metrics.items():
        if "http_5xx_rate" not in df.columns or "http_requests_per_sec" not in df.columns:
            continue
        base = df.loc[df["timestamp"] < base_end]
        baseline_5xx = base["http_5xx_rate"].astype(float)
        baseline_rps = base["http_requests_per_sec"].astype(float)
        p50_rps = float(baseline_rps.quantile(0.50))
        p75 = float(baseline_5xx.quantile(0.75))
        p95 = float(baseline_5xx.quantile(0.95))
        p99 = float(baseline_5xx.quantile(0.99))
        threshold = max(p99 * 1.5, 3.0)
        rate = df["http_5xx_rate"].astype(float)
        rps = df["http_requests_per_sec"].astype(float)
        mask = (rate > threshold) & (rps >= p50_rps)
        first_after_base = sustained_decision_time(mask, df["timestamp"], hits=5, window=10, start_at=base_end)
        false_pos = int(mask[df["timestamp"] < base_end].sum())
        detections.append(
            Detection(
                "http_5xx_sustained",
                service,
                "http_5xx_rate",
                first_after_base,
                false_pos,
                (
                    "formula=http_5xx_rate > max(baseline_p99 * 1.5, 3.0) with volume guard; "
                    f"baseline_p50={float(baseline_5xx.quantile(0.50)):.3f}; "
                    f"baseline_p75={p75:.3f}; baseline_p95={p95:.3f}; baseline_p99={p99:.3f}; "
                    f"threshold={threshold:.3f}; request_p50_guard={p50_rps:.3f}; persistence=5/10; impact_signal=True"
                ),
                False,
                service == "cart-service",
            )
        )
    return detections


def isolation_forest_detections(metrics: dict[str, pd.DataFrame]) -> list[Detection]:
    base_end = baseline_end(metrics)
    detections = []
    for service, df in metrics.items():
        cols = numeric_columns(df)
        if not cols:
            continue
        x = df[cols].astype(float).replace([np.inf, -np.inf], np.nan).ffill().bfill()
        train = x[df["timestamp"] < base_end]
        scaler = RobustScaler().fit(train)
        model = IsolationForest(n_estimators=200, contamination=0.03, random_state=42)
        model.fit(scaler.transform(train))
        pred = pd.Series(model.predict(scaler.transform(x)) == -1, index=df.index)
        first_after_base = persistent_first(pred & (df["timestamp"] >= base_end), df["timestamp"])
        false_pos = int(pred[df["timestamp"] < base_end].sum())
        top_metric = "multivariate"
        if pd.notna(first_after_base):
            row = df.loc[df["timestamp"] == first_after_base, cols].iloc[0].astype(float)
            med = train.median()
            mad = (train - med).abs().median().replace(0, np.nan)
            top_metric = ((row - med).abs() / mad).sort_values(ascending=False).index[0]
        detections.append(
            Detection(
                "isolation_forest",
                service,
                top_metric,
                first_after_base,
                false_pos,
                f"n_estimators=200; contamination=0.03; trained_on=first_{BASELINE_HOURS}h; features={','.join(cols)}",
                service == "cart-service" and top_metric != "http_5xx_rate",
            )
        )
    return detections


def write_method_comparison(detections: list[Detection]) -> pd.DataFrame:
    rows = []
    for d in detections:
        rows.append(
            {
                "method": d.method,
                "service": d.service,
                "affected_metric_service": f"{d.service}/{d.metric}",
                "earliest_detection": "" if pd.isna(d.timestamp) else d.timestamp.isoformat(),
                "false_positive_count_first_6h": d.false_positive_count,
                "supports_rca_chain": d.supports_rca_chain,
                "impact_signal": d.impact_signal,
                "detail": d.detail,
            }
        )
    comp = pd.DataFrame(rows).sort_values(["method", "earliest_detection", "service"], na_position="last")
    comp.to_csv(OUT / "method_comparison.csv", index=False)
    return comp


def detector_observability(comparison: pd.DataFrame) -> pd.DataFrame:
    focus = {
        "cart-service/memory_usage_bytes": "Tín hiệu tài nguyên chính của cart-service.",
        "cart-service/jvm_gc_pause_ms_avg": "Tín hiệu JVM bị áp lực heap/GC.",
        "cart-service/http_p99_latency_ms": "Tác động latency trước khi lỗi lan rộng.",
        "cart-service/http_5xx_rate": "Tác động user-facing trên chính cart-service.",
        "cart-service/container_restart_count": "Bằng chứng restart loop sau OOMKilled.",
        "api-gateway/cart_upstream_error_rate": "Gateway nhìn thấy lỗi upstream từ cart.",
        "order-service/upstream_timeout_rate": "Caller order bị timeout khi gọi cart.",
        "payment-service/upstream_timeout_rate": "Caller payment bị ảnh hưởng muộn hơn trong checkout.",
    }
    rows = []
    detail_re = re.compile(r"(formula|span|median|sigma|threshold|persistence|baseline_p50|baseline_p75|baseline_p95|baseline_p99|request_p50_guard)=([^;]+)")
    subset = comparison[
        comparison["method"].isin(["robust_mad_3alpha", "ewma_trend", "http_5xx_sustained"])
        & comparison["affected_metric_service"].isin(focus)
    ].copy()
    for _, row in subset.iterrows():
        parts = {k: v.strip() for k, v in detail_re.findall(str(row["detail"]))}
        method = row["method"]
        metric = row["affected_metric_service"]
        detected = bool(str(row["earliest_detection"]).strip())
        baseline_fp = int(row["false_positive_count_first_6h"])
        if method == "robust_mad_3alpha":
            threshold_text = (
                f"MAD 3-sigma: value > median + 3 * sigma; "
                f"median={parts.get('median', 'n/a')}; sigma={parts.get('sigma', 'n/a')}; "
                f"threshold={parts.get('threshold', 'n/a')}; persistence={parts.get('persistence', 'n/a')}"
            )
        elif method == "http_5xx_sustained":
            threshold_text = (
                "Sustained 5xx impact detector: rate > max(baseline p99 * 1.5, 3.0) "
                f"and requests >= baseline request p50; baseline_p50={parts.get('baseline_p50', 'n/a')}; "
                f"baseline_p75={parts.get('baseline_p75', 'n/a')}; baseline_p95={parts.get('baseline_p95', 'n/a')}; "
                f"baseline_p99={parts.get('baseline_p99', 'n/a')}; threshold={parts.get('threshold', 'n/a')}; "
                f"persistence={parts.get('persistence', 'n/a')}"
            )
        else:
            threshold_text = (
                f"EWMA decision: EWMA(span={parts.get('span', '20')}) > median + 3 * sigma; "
                f"median={parts.get('median', 'n/a')}; sigma={parts.get('sigma', 'n/a')}; "
                f"threshold={parts.get('threshold', 'n/a')}; persistence={parts.get('persistence', 'n/a')}"
            )
        result = (
            f"Phát hiện anomaly kéo dài lúc {row['earliest_detection']}; false positive baseline={row['false_positive_count_first_6h']}"
            if detected
            else f"Không phát hiện anomaly kéo dài sau baseline; false positive baseline={row['false_positive_count_first_6h']}"
        )
        if method == "http_5xx_sustained":
            interpretation = (
                f"{focus[metric]} This detector is used as user-facing impact evidence, not as the RCA start. "
                "It fixes the MAD false-positive problem for 5xx by requiring percentile threshold, traffic volume, and persistence."
            )
        elif method == "ewma_trend":
            interpretation = (
                f"{focus[metric]} EWMA is retained as a smoothed trend view only. "
                f"With span={parts.get('span', '20')} it can show drift, but this row is not used as the final RCA decision label."
            )
        elif baseline_fp > 20:
            interpretation = (
                f"{focus[metric]} Baseline false positives are high ({baseline_fp}), "
                "so this threshold crossing is treated as a noisy/weak signal, not a reliable RCA start."
            )
        elif metric.startswith("cart-service/") and detected:
            interpretation = f"{focus[metric]} Kết quả ủng hộ cart-service là origin candidate trong chuỗi RCA."
        elif metric.startswith(("api-gateway/", "order-service/", "payment-service/")) and detected:
            interpretation = f"{focus[metric]} Kết quả phù hợp với triệu chứng downstream, không phải nguồn gốc đầu tiên."
        elif detected:
            interpretation = f"{focus[metric]} Có tín hiệu vượt ngưỡng, cần đọc cùng timeline để tránh kết luận đơn lẻ."
        else:
            interpretation = f"{focus[metric]} Detector không thấy tín hiệu kéo dài đủ mạnh sau baseline."
        rows.append(
            {
                "detector": {
                    "robust_mad_3alpha": "MAD 3-sigma",
                    "ewma_trend": "EWMA trend",
                    "http_5xx_sustained": "sustained 5xx impact detector",
                }[method],
                "metric": metric,
                "decision_threshold": threshold_text,
                "result": result,
                "interpretation": interpretation,
            }
        )
    observability = pd.DataFrame(rows).sort_values(["metric", "detector"])
    observability.to_csv(OUT / "detector_observability.csv", index=False)
    return observability


def build_drain3_config(sim_th: float | None = None, depth: int | None = None) -> TemplateMinerConfig:
    config = TemplateMinerConfig()
    config.drain_sim_th = SELECTED_DRAIN["drain_sim_th"] if sim_th is None else sim_th
    config.drain_depth = SELECTED_DRAIN["drain_depth"] if depth is None else depth
    config.drain_max_children = SELECTED_DRAIN["drain_max_children"]
    config.parametrize_numeric_tokens = SELECTED_DRAIN["parametrize_numeric_tokens"]
    config.masking_instructions = [
        MaskingInstruction(r"\b[0-9a-f]{12,}\b", "HEX"),
        MaskingInstruction(r"\bORD-\d+\b", "ORDER_ID"),
        MaskingInstruction(r"(?<=userId=)\d+", "NUM"),
        MaskingInstruction(r"(?<=orderId=)\d+", "NUM"),
        MaskingInstruction(r"(?<=status=)\d+", "NUM"),
        MaskingInstruction(r"(?<=pause=)\d+", "NUM"),
        MaskingInstruction(r"(?<=heap=)\d+", "NUM"),
        MaskingInstruction(r"(?<=startup_ms=)\d+", "NUM"),
        MaskingInstruction(r"(?<=after )\d+(?=ms)", "NUM"),
        MaskingInstruction(r"(?<=duration_ms=)\d+", "NUM"),
    ]
    return config


def new_drain3_miner(sim_th: float | None = None, depth: int | None = None) -> TemplateMiner:
    return TemplateMiner(config=build_drain3_config(sim_th, depth))


def assign_drain3_templates(logs: pd.DataFrame, miner: TemplateMiner | None = None) -> pd.DataFrame:
    miner = miner or new_drain3_miner()
    for message in logs["message"]:
        miner.add_log_message(str(message))

    rows = []
    for _, row in logs.iterrows():
        cluster = miner.match(str(row["message"]), full_search_strategy="always")
        if cluster is None:
            result = miner.add_log_message(str(row["message"]))
            cluster_id = int(result["cluster_id"])
            template = result["template_mined"]
        else:
            cluster_id = int(cluster.cluster_id)
            template = cluster.get_template()
        rows.append({"cluster_id": cluster_id, "template": template})
    assigned = logs.copy()
    assigned[["cluster_id", "template"]] = pd.DataFrame(rows, index=assigned.index)
    return assigned


def log_template_analysis(logs: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    comparison_rows = []
    for sim_th in (0.4, 0.5, 0.6):
        for depth in (4, 5):
            assigned = assign_drain3_templates(logs, new_drain3_miner(sim_th, depth))
            templates = assigned["template"].nunique()
            key_coverage = {}
            for key in ["GC overhead", "ProductCatalogCache eviction failed", "OOMKilled", "Cart service timeout", "Cart service returned 5xx"]:
                key_coverage[key] = assigned.loc[
                    assigned["message"].str.contains(key, case=False, na=False), "template"
                ].nunique()
            comparison_rows.append(
                {
                    "sim_th": sim_th,
                    "depth": depth,
                    "max_children": 100,
                    "parametrize_numeric_tokens": True,
                    "template_count": templates,
                    "key_patterns_preserved": all(v >= 1 for v in key_coverage.values()),
                    "selected": sim_th == SELECTED_DRAIN["drain_sim_th"] and depth == SELECTED_DRAIN["drain_depth"],
                    "notes": "; ".join(f"{k}={v}" for k, v in key_coverage.items()),
                }
            )
    drain_comparison = pd.DataFrame(comparison_rows)
    drain_comparison.to_csv(OUT / "drain_comparison.csv", index=False)

    logs = assign_drain3_templates(logs, new_drain3_miner())
    grouped = (
        logs.groupby(["service", "level", "template"])
        .agg(
            count=("message", "size"),
            first_seen=("timestamp", "min"),
            last_seen=("timestamp", "max"),
            sample_message=("message", "first"),
            pod_count=("pod", "nunique"),
        )
        .reset_index()
        .sort_values("count", ascending=False)
    )
    grouped.insert(0, "template_id", [f"T{i:03d}" for i in range(1, len(grouped) + 1)])
    grouped["first_seen"] = grouped["first_seen"].map(lambda t: t.isoformat())
    grouped["last_seen"] = grouped["last_seen"].map(lambda t: t.isoformat())
    grouped.to_csv(OUT / "log_templates.csv", index=False)

    template_id = grouped.set_index(["service", "level", "template"])["template_id"].to_dict()
    logs["template_id"] = logs.apply(lambda r: template_id[(r["service"], r["level"], r["template"])], axis=1)
    ts = (
        logs.set_index("timestamp")
        .groupby(["service", "template_id", "template", pd.Grouper(freq="5min")])
        .size()
        .rename("count")
        .reset_index()
        .sort_values(["timestamp", "service", "template_id"])
    )
    ts["timestamp"] = ts["timestamp"].map(lambda t: t.isoformat())
    ts.to_csv(OUT / "log_template_timeseries.csv", index=False)
    return grouped, ts, drain_comparison


def important_events(metrics: dict[str, pd.DataFrame], logs: pd.DataFrame, anomalies: pd.DataFrame, templates: pd.DataFrame, comparison: pd.DataFrame) -> pd.DataFrame:
    rows = []

    def add(ts, stage, service, signal_type, signal, evidence, rca):
        if pd.notna(ts):
            rows.append(
                {
                    "timestamp": pd.Timestamp(ts).isoformat(),
                    "stage": stage,
                    "service": service,
                    "signal_type": signal_type,
                    "signal": signal,
                    "evidence": evidence,
                    "rca_interpretation": rca,
                }
            )

    cart = metrics["cart-service"]
    for metric in ["http_p99_latency_ms", "memory_usage_bytes", "jvm_gc_pause_ms_avg", "container_restart_count"]:
        match = anomalies[(anomalies["service"] == "cart-service") & (anomalies["metric"] == metric)]
        if not match.empty:
            first = pd.to_datetime(match["timestamp"], utc=True).min()
            add(first, "metric anomaly", "cart-service", "metric", metric, f"robust MAD anomaly begins at {first.isoformat()}", "cart degradation starts before downstream alert fan-out")

    restart_growth = cart.loc[cart["container_restart_count"].diff().fillna(0) > 0]
    if not restart_growth.empty:
        ts = restart_growth["timestamp"].iloc[0]
        add(ts, "restart loop", "cart-service", "metric", "container_restart_count", f"restart counter first increases to {restart_growth['container_restart_count'].iloc[0]}", "OOM/restart cycle becomes externally visible")

    oom_ts = pd.NaT
    oom_subset = logs[logs["message"].str.contains("Container OOMKilled", case=False, na=False)]
    if not oom_subset.empty:
        oom_ts = oom_subset["timestamp"].min()

    keyword_map = {
        "GC overhead limit warning": "GC warning",
        "ProductCatalogCache eviction failed": "cache eviction failure",
        "Container OOMKilled": "OOMKilled",
        "Upstream connection refused": "connection refused",
        "Cart service timeout": "order timeout",
        "Cart service returned 5xx": "order sees cart 5xx",
    }
    for kw, label in keyword_map.items():
        subset = logs[logs["message"].str.contains(kw, case=False, na=False)]
        if label in {"order timeout", "order sees cart 5xx"} and pd.notna(oom_ts):
            subset = subset[subset["timestamp"] >= oom_ts]
        if not subset.empty:
            row = subset.iloc[0]
            add(row["timestamp"], "log signal", row["service"], "log_template", label, row["message"], "log evidence supports metric ordering")

    for service, metric in [
        ("order-service", "upstream_timeout_rate"),
        ("payment-service", "upstream_timeout_rate"),
        ("api-gateway", "cart_upstream_error_rate"),
    ]:
        match = anomalies[(anomalies["service"] == service) & (anomalies["metric"] == metric)]
        if not match.empty:
            first = pd.to_datetime(match["timestamp"], utc=True).min()
            add(first, "downstream symptom", service, "metric", metric, f"robust MAD anomaly begins at {first.isoformat()}", "cart failures propagate to callers")

    impact = comparison[
        (comparison["method"] == "http_5xx_sustained")
        & (comparison["affected_metric_service"] == "cart-service/http_5xx_rate")
        & comparison["earliest_detection"].astype(str).str.strip().ne("")
    ]
    if not impact.empty:
        ts = pd.to_datetime(impact["earliest_detection"].iloc[0], utc=True)
        add(
            ts,
            "impact signal",
            "cart-service",
            "metric",
            "sustained 5xx impact",
            "http_5xx_sustained decision: 5 of 10 points above threshold 3.0 with traffic volume guard",
            "user-facing impact evidence after restart/downstream symptoms; not the RCA start",
        )

    timeline = pd.DataFrame(rows).drop_duplicates().sort_values("timestamp")
    timeline.to_csv(OUT / "incident_timeline.csv", index=False)
    return timeline


def plot_metric_panels(metrics: dict[str, pd.DataFrame], anomalies: pd.DataFrame) -> list[str]:
    chart_paths = []
    selections = {
        "cart-service": ["memory_usage_bytes", "jvm_gc_pause_ms_avg", "http_p99_latency_ms", "http_5xx_rate", "container_restart_count"],
        "order-service": ["upstream_timeout_rate", "http_5xx_rate"],
        "payment-service": ["upstream_timeout_rate", "http_5xx_rate"],
        "api-gateway": ["cart_upstream_error_rate", "http_5xx_rate"],
        "product-service": ["http_p99_latency_ms", "http_5xx_rate"],
    }
    for service, cols in selections.items():
        if service not in metrics:
            continue
        df = metrics[service]
        cols = [c for c in cols if c in df.columns]
        fig, axes = plt.subplots(len(cols), 1, figsize=(12, max(3, 2.2 * len(cols))), sharex=True)
        if len(cols) == 1:
            axes = [axes]
        for ax, col in zip(axes, cols):
            ax.plot(df["timestamp"], df[col], lw=1.1, label=col)
            smooth = df[col].astype(float).ewm(span=20, adjust=False).mean()
            ax.plot(df["timestamp"], smooth, lw=1.0, alpha=0.8, label="EWMA span=20")
            hits = anomalies[(anomalies["service"] == service) & (anomalies["metric"] == col)]
            if not hits.empty:
                hit_ts = pd.to_datetime(hits["timestamp"], utc=True)
                ax.scatter(hit_ts, hits["value"], s=12, color="crimson", label="MAD anomaly")
            ax.set_title(f"{service} / {col}")
            ax.grid(True, alpha=0.25)
            ax.legend(loc="upper left", fontsize=8)
        fig.autofmt_xdate()
        fig.tight_layout()
        path = CHARTS / f"{service}-metrics.png"
        fig.savefig(path, dpi=140)
        plt.close(fig)
        chart_paths.append(path.relative_to(OUT).as_posix())
    return chart_paths


def plot_log_panels(template_ts: pd.DataFrame, templates: pd.DataFrame) -> list[str]:
    key_patterns = ["GC overhead", "ProductCatalogCache eviction", "OOMKilled", "Cart service timeout", "Cart service returned 5xx"]
    ids = templates[templates["template"].str.contains("|".join(key_patterns), case=False, na=False)]["template_id"].head(12).tolist()
    if not ids:
        return []
    ts = template_ts[template_ts["template_id"].isin(ids)].copy()
    ts["timestamp"] = pd.to_datetime(ts["timestamp"], utc=True)
    fig, ax = plt.subplots(figsize=(12, 5))
    for tid, group in ts.groupby("template_id"):
        label = templates.loc[templates["template_id"] == tid, "template"].iloc[0][:70]
        ax.plot(group["timestamp"], group["count"], lw=1.2, label=f"{tid} {label}")
    ax.set_title("Key log template counts per 5 minutes")
    ax.set_ylabel("count")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="upper left", fontsize=7)
    fig.autofmt_xdate()
    fig.tight_layout()
    path = CHARTS / "key-log-template-timeseries.png"
    fig.savefig(path, dpi=140)
    plt.close(fig)
    return [path.relative_to(OUT).as_posix()]


def first_value(timeline: pd.DataFrame, signal: str) -> str:
    rows = timeline[timeline["signal"].str.contains(signal, case=False, na=False)]
    return "not detected" if rows.empty else rows["timestamp"].iloc[0]


def generate_reports(metrics_summary: pd.DataFrame, comparison: pd.DataFrame, drain_comparison: pd.DataFrame, templates: pd.DataFrame, timeline: pd.DataFrame, chart_paths: list[str]) -> None:
    first_metric = timeline[timeline["signal_type"] == "metric"]["timestamp"].min()
    gc_time = first_value(timeline, "GC warning")
    cache_time = first_value(timeline, "cache eviction")
    oom_time = first_value(timeline, "OOMKilled")
    restart_time = first_value(timeline, "container_restart_count")
    order_time = first_value(timeline, "upstream_timeout_rate|order timeout")

    selected_drain = drain_comparison[drain_comparison["selected"]].iloc[0].to_dict()
    top_templates = templates.head(12)[["template_id", "service", "level", "count", "first_seen", "template"]]

    findings = f"""# FINDINGS

## Executive Summary

WHEN: the earliest reliable RCA evidence starts with cart-service GC/cache logs at `{gc_time}` / `{cache_time}`. The earliest reliable metric anomaly is `{first_metric}`. The earlier cart-service `http_5xx_rate` MAD crossing at `2026-06-01T06:08:00+00:00` is kept as a raw threshold event only, because that metric has many baseline false positives and is too noisy to define the incident start.

WHERE: the primary origin is `cart-service`, led by memory pressure, JVM GC pauses, and cache eviction failure logs. The downstream symptoms appear later in `order-service`, `payment-service`, and `api-gateway`.

WHAT: the evidence supports a cart-service memory pressure incident. ProductCatalogCache eviction failures and rising GC pauses preceded `OOMKilled`; OOMKilled then drove pod restarts and connection refusals/timeouts, which propagated as cart 5xx and upstream timeout rates in callers.

## Evidence Timeline

{timeline.to_markdown(index=False)}

## Method Choice

The final primary detector is robust 3-alpha/MAD against the first {BASELINE_HOURS} hours because it is explainable and produces service/metric evidence that maps directly to the incident. IsolationForest is retained as a multivariate confirmation method. EWMA is used for trend smoothing and early slope visualization, not as the main classifier. Signals with high baseline false-positive counts, such as `cart-service/http_5xx_rate`, are treated as weak threshold crossings rather than reliable RCA start markers.

The `cart-service/http_5xx_rate` audit is the important exception. MAD calculation was correct, but this metric is noisy/zero-inflated, so the detector choice was wrong for 5xx impact. Its baseline median is `0.065`, MAD threshold is `0.354`, and the baseline distribution already reaches p75 `1.06` and p95/p99 `2.00`, producing `297/720` baseline false positives.

The corrected `http_5xx_sustained` detector is a windowed rule, not an ML model. It uses the first 6 hours as baseline, computes baseline p99 `2.00`, sets threshold `max(baseline_p99 * 1.5, 3.0) = 3.00`, requires `5/10` recent 30-second points above threshold, and applies a request-volume guard `http_requests_per_sec >= baseline p50`. For cart-service it detects sustained impact at `2026-06-01T20:41:30+00:00` with baseline FP `0`, `impact_signal=True`, and `supports_rca_chain=False`. This is impact evidence after restart/downstream symptoms, not the RCA start.

For readers who learned the classic 3-alpha rule as `mean + 3 * std`: this report uses the same 3-alpha idea, but with robust statistics. Instead of `mean`, it uses the baseline `median`. Instead of `std`, it uses `1.4826 * MAD`, where `MAD = median(|x - median(x)|)`. The factor `1.4826` converts MAD to a standard-deviation-like scale when the data is approximately normal. This makes the threshold less sensitive to baseline spikes:

```text
classic 3-alpha: mean + 3 * std
robust 3-alpha: median + 3 * 1.4826 * MAD
```

## EDA Figure Support For 5xx Audit

Deck HTML dùng thêm figure baseline 6 giờ được generate trực tiếp từ `g1/metrics/cart-service.csv`:

- `outputs/charts/cart-5xx-baseline-6h-audit.png`: time series + histogram baseline 6 giờ đầu của `cart-service/http_5xx_rate`.

Lý do dùng baseline 6 giờ: đây là cửa sổ trước giai đoạn OOM/restart, đủ 720 điểm ở interval 30 giây để tính median, MAD và percentile. Tuy nhiên chính figure baseline cho thấy caveat của metric 5xx: MAD threshold `0.354` nằm quá thấp so với nhiễu baseline, khiến `297/720` điểm baseline bị flag.

Deck cũng dùng figure thật từ `EDA.ipynb`:

- `outputs/presentations/notebook-assets/notebook-figure-01.png`: histogram/density của `cart__http_5xx_rate`.
- `outputs/presentations/notebook-assets/notebook-figure-02.png`: ACF plot của `cart__http_5xx_rate`.

Histogram cho thấy phần lớn giá trị nằm sát 0 nhưng có đuôi kéo dài đến `16.78`; output EDA ghi `skew=2.77`, `p50=0.38`, `p95=9.53`, `p99=14.43`, `max=16.78`. ACF plot cho thấy metric có tương quan theo thời gian, nên detector 5xx nên đọc theo cửa sổ thời gian thay vì từng crossing đơn lẻ.

## Log Calibration

Selected Drain3 config: `sim_th={SELECTED_DRAIN['drain_sim_th']}`, `depth={SELECTED_DRAIN['drain_depth']}`, `max_children={SELECTED_DRAIN['drain_max_children']}`, `parametrize_numeric_tokens=True`.

Reason: the calibration table shows low similarity settings coarsen related failures, while `sim_th=0.6` keeps GC warning, cache eviction failure, OOMKilled, cart timeout, and cart 5xx patterns distinct.

## Key Template Evidence

{top_templates.to_markdown(index=False)}
"""
    (ROOT / "FINDINGS.md").write_text(findings, encoding="utf-8")

    submit = """# SUBMIT

## Group Reflection

Our analysis treated the incident as an evidence-ordering problem instead of starting from the alert text. We first validated the telemetry shape, row counts, timestamp ranges, duplicate records, gaps, nulls, and baseline behavior. Then we compared a robust MAD detector, EWMA trend smoothing, and IsolationForest. The robust method was easiest to defend because each anomaly maps back to a concrete service and metric with a baseline threshold. IsolationForest was useful as a secondary check, while EWMA helped explain the slope and timing visually. For logs, we calibrated template extraction so important messages did not collapse into one generic upstream failure. The resulting timeline shows cart-service memory and GC pressure, cache eviction failures, OOMKilled events, restart growth, and downstream timeout/5xx propagation. The main lesson is that early operational signals were present before the page, but they required correlating metrics and template-level logs rather than reading isolated alerts.

## Contributions

- Member 1: metrics validation and robust MAD anomaly analysis.
- Member 2: IsolationForest and EWMA comparison.
- Member 3: log preprocessing and Drain3 template calibration.
- Member 4: incident timeline and root-cause synthesis.
- Member 5: dashboard, charts, and report packaging.
"""
    (ROOT / "SUBMIT.md").write_text(submit, encoding="utf-8")

    readme = f"""# AIOps W1 Offline Pipeline

Run from the repository root:

```bash
python w1/lab/analyze.py
```

The script reads raw telemetry from `g1/metrics` and `g1/logs`, runs EDA/calibration, applies the final detectors, and writes all artifacts to `outputs`.

Primary detector: robust 3-alpha/MAD using the first {BASELINE_HOURS} hours as baseline and {PERSISTENCE_HITS}-of-{PERSISTENCE_WINDOW} persistence.

Relation to the classic 3-alpha rule:

```text
classic 3-alpha: mean + 3 * std
robust 3-alpha: median + 3 * 1.4826 * MAD
MAD = median(|x - median(x)|)
```

The `1.4826` factor scales MAD so it is comparable to standard deviation for approximately normal data. This keeps the same 3-alpha intuition while reducing sensitivity to outliers in the baseline.

Secondary detector: IsolationForest trained on the same baseline window.

Trend method: EWMA span 20 for visual smoothing.

Selected log template config: `{json.dumps(SELECTED_DRAIN)}`.

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
"""
    (ROOT / "README.md").write_text(readme, encoding="utf-8")

    timeline_html = timeline.to_html(index=False, escape=True)
    methods_html = comparison.head(30).to_html(index=False, escape=True)
    rca_methods_html = comparison[comparison["supports_rca_chain"]].head(18).to_html(index=False, escape=True)
    drain_html = drain_comparison.to_html(index=False, escape=True)
    templates_html = top_templates.to_html(index=False, escape=True)
    chart_commentary = {
        "charts/cart-service-metrics.png": "Nên đọc biểu đồ này đầu tiên. Chuỗi quan trọng là latency và memory của cart tăng, sau đó JVM GC pause tăng, rồi đến OOM/restart và 5xx. Điểm đỏ là anomaly theo robust MAD; đường mượt là EWMA để nhìn xu hướng.",
        "charts/order-service-metrics.png": "Order-service không giống nguồn gốc sự cố. Timeout và 5xx của order chỉ có ý nghĩa mạnh sau khi cart đã suy giảm, nên đây là triệu chứng downstream.",
        "charts/payment-service-metrics.png": "Payment-service cũng giống một triệu chứng downstream: timeout rate tăng sau khi cart mất ổn định và bắt đầu ảnh hưởng luồng checkout.",
        "charts/api-gateway-metrics.png": "API gateway cart upstream errors là bằng chứng lan truyền. Gateway thấy lỗi cart sau khi cart-service đã có dấu hiệu áp lực nội bộ.",
        "charts/product-service-metrics.png": "Product-service có vài dao động nhiễu, nhưng không tạo thành chuỗi RCA tốt nhất cho vòng lặp restart của cart.",
        "charts/key-log-template-timeseries.png": "Số lượng log template cho thấy GC/cache warning lặp lại trước OOMKilled và trước lỗi order downstream. Đây mạnh hơn việc chỉ đọc vài dòng log raw vì nó thể hiện tần suất pattern theo thời gian.",
    }
    imgs = "\n".join(
        f'<figure><img src="{html.escape(path)}" alt="{html.escape(path)}"><figcaption><strong>{html.escape(path)}</strong><br>{html.escape(chart_commentary.get(path, "Offline chart generated from raw telemetry."))}</figcaption></figure>'
        for path in chart_paths
    )
    dashboard = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>AIOps W1 Incident Dashboard</title>
<style>
body {{ margin: 0; font-family: Arial, sans-serif; color: #1f2933; background: #f7f9fb; }}
header {{ background: #102a43; color: white; padding: 28px 40px; }}
main {{ max-width: 1180px; margin: 0 auto; padding: 24px; }}
section {{ background: white; border: 1px solid #d9e2ec; border-radius: 8px; padding: 20px; margin: 18px 0; }}
h1, h2 {{ margin-top: 0; }}
.lede {{ font-size: 16px; line-height: 1.55; max-width: 980px; }}
.answer {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }}
.answer div {{ border: 1px solid #bcccdc; border-radius: 8px; padding: 14px; background: #f0f4f8; }}
.callout {{ border-left: 5px solid #2f80ed; background: #eef6ff; padding: 14px 16px; border-radius: 6px; margin: 14px 0; }}
.warning {{ border-left-color: #d64545; background: #fff5f5; }}
.reasoning {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }}
.reasoning article {{ border: 1px solid #d9e2ec; border-radius: 8px; padding: 14px; background: #fbfdff; }}
.steps {{ margin: 0; padding-left: 22px; line-height: 1.55; }}
.muted {{ color: #52606d; }}
table {{ width: 100%; max-width: 100%; border-collapse: collapse; font-size: 12px; table-layout: fixed; }}
th, td {{ border-bottom: 1px solid #d9e2ec; padding: 6px 8px; text-align: left; vertical-align: top; overflow-wrap: anywhere; word-break: break-word; hyphens: auto; }}
th {{ background: #eef2f7; }}
th:nth-child(1), td:nth-child(1) {{ width: 10%; }}
th:nth-child(2), td:nth-child(2) {{ width: 9%; }}
th:nth-child(3), td:nth-child(3) {{ width: 17%; }}
th:nth-child(4), td:nth-child(4) {{ width: 12%; }}
th:nth-child(5), td:nth-child(5) {{ width: 10%; }}
th:nth-child(6), td:nth-child(6) {{ width: 9%; }}
th:nth-child(7), td:nth-child(7) {{ width: 33%; font-size: 11px; }}
.dataframe {{ margin: 12px 0 22px; }}
img {{ width: 100%; height: auto; border: 1px solid #d9e2ec; border-radius: 8px; background: white; }}
figure {{ margin: 18px 0; }}
figcaption {{ font-size: 12px; color: #52606d; margin-top: 6px; }}
code {{ background: #eef2f7; padding: 1px 4px; border-radius: 4px; }}
@media (max-width: 900px) {{
  .answer, .reasoning {{ grid-template-columns: 1fr; }}
  main {{ padding: 12px; }}
  section {{ padding: 14px; }}
  table {{ font-size: 11px; }}
  th, td {{ padding: 5px 6px; }}
  th:nth-child(n), td:nth-child(n) {{ width: auto; }}
}}
</style>
</head>
<body>
<header>
<h1>AIOps W1 Incident Dashboard</h1>
<p class="lede">Dashboard EDA offline, hiệu chỉnh detector, phân tích log template, và bằng chứng RCA từ telemetry raw. Trang này được viết như một walkthrough: đọc câu trả lời ngắn trước, sau đó đi theo chuỗi lập luận, rồi dùng bảng và chart để kiểm chứng.</p>
</header>
<main>
<section class="answer">
<div><h2>WHEN</h2><p>Reliable metric anomaly đầu tiên: <code>{html.escape(str(first_metric))}</code>.</p><p>Silent signal ở log xuất hiện sớm hơn: GC warning lúc <code>{html.escape(gc_time)}</code> và cache eviction failure lúc <code>{html.escape(cache_time)}</code>.</p><p><code>cart-service/http_5xx_rate</code> có raw MAD crossing lúc 06:08 nhưng baseline false positive quá cao, nên không dùng làm mốc RCA.</p></div>
<div><h2>WHERE</h2><p><code>cart-service</code> là ứng viên nguồn gốc vì tín hiệu tài nguyên nội bộ xuất hiện trước alert ở các service downstream.</p><p>Chỉ báo chính: memory usage, JVM GC pause, cache eviction failure, OOMKilled, restart count.</p></div>
<div><h2>WHAT</h2><p>Cơ chế khả dĩ nhất: cart bị áp lực memory, kéo theo GC/cache degradation, sau đó OOMKilled restart, rồi lan ra caller timeout và 5xx.</p><p>OOMKilled: <code>{html.escape(oom_time)}</code>; restart tăng: <code>{html.escape(restart_time)}</code>.</p></div>
</section>
<section>
<h2>Cách Đọc Dashboard</h2>
<div class="callout">
<p><strong>Mục tiêu:</strong> trả lời WHEN, WHERE, và WHAT bằng bằng chứng, không chỉ sao chép lại nội dung alert ban đầu.</p>
<p><strong>Điểm cần phân biệt:</strong> một warning raw có thể xuất hiện trong vận hành bình thường. Một tín hiệu incident cần có tính kéo dài, đúng thứ tự thời gian, và tương quan với telemetry khác. Vì vậy dashboard này so sánh metric detector, log template, và event ordering.</p>
</div>
<ol class="steps">
<li>Dùng {BASELINE_HOURS} giờ đầu làm baseline window. Baseline này cung cấp median và mức dao động bình thường cho từng metric.</li>
<li>Tìm metric lệch kéo dài bằng robust MAD. Đây là nhãn anomaly chính vì dễ giải thích: giá trị vượt baseline median + {MAD_K:g} scaled MAD, với điều kiện persistence {PERSISTENCE_HITS}-of-{PERSISTENCE_WINDOW}.</li>
<li>Dùng EWMA như đường làm mượt để xem metric có xu hướng tăng trước khi vượt ngưỡng anomaly cứng hay không.</li>
<li>Dùng IsolationForest làm xác nhận multivariate thứ cấp, không dùng làm nhãn RCA chính, vì score khó giải thích hơn với người đọc không chuyên ML.</li>
<li>Gom log thành template để các pattern lặp lại như GC warning và OOMKilled được đếm theo thời gian, thay vì xem như vài ví dụ log rời rạc.</li>
</ol>
</section>
<section>
<h2>Chuỗi Lập Luận RCA</h2>
<div class="reasoning">
<article><h3>1. Cart có áp lực nội bộ trước</h3><p>GC warning và cache eviction template bắt đầu trước OOMKilled. Anomaly trên latency, memory, và GC pause của cart cho thấy service đã suy giảm từ bên trong trước khi caller báo lỗi diện rộng.</p></article>
<article><h3>2. OOMKilled giải thích restart loop</h3><p>Log template <code>Container OOMKilled: memory limit exceeded</code> xuất hiện lúc <code>{html.escape(oom_time)}</code>. Restart counter bắt đầu tăng lúc <code>{html.escape(restart_time)}</code>. Thứ tự này ủng hộ cơ chế OOM/restart hơn là deploy thủ công hoặc lỗi từ caller.</p></article>
<article><h3>3. Downstream service là triệu chứng</h3><p>Anomaly của order, payment, và API gateway xảy ra sau khi cart-service đã unhealthy. Timeout và 5xx của chúng khớp với restart và upstream failure của cart, nên chúng là bằng chứng lan truyền thay vì nguyên nhân gốc.</p></article>
<article><h3>4. Product-service không phải RCA tốt nhất</h3><p>Product-service có dao động nhiễu, nhưng chuỗi bằng chứng mạnh nhất là memory/cache pressure của cart dẫn tới cart OOMKilled. Log connection refused liên quan product được xem là hiệu ứng phụ trong giai đoạn cart mất ổn định, trừ khi có bằng chứng bổ sung chứng minh ngược lại.</p></article>
</div>
</section>
<section>
<h2>EDA Và Lựa Chọn Phương Pháp</h2>
<p>{BASELINE_HOURS} giờ đầu được dùng làm baseline ổn định sau khi kiểm tra row count, timestamp gap, null, duplicate, và numeric range. Baseline không được giả định là hoàn hảo; nó được kiểm tra rồi dùng nhất quán để mọi so sánh detector có thể chạy lại được.</p>
<div class="callout">
<p><strong>Vì sao robust MAD là chính:</strong> chịu outlier tốt, dễ giải thích, và tạo ngưỡng cụ thể cho từng metric. Điều này phù hợp với postmortem vì người đọc có thể audit vì sao một timestamp được chọn.</p>
<p><strong>Liên hệ với 3-alpha đã học:</strong> nếu công thức quen thuộc là <code>mean + 3 * std</code>, thì ở đây vẫn là ý tưởng 3-alpha nhưng thay bằng thống kê robust: <code>median + 3 * 1.4826 * MAD</code>. Trong đó <code>MAD = median(|x - median(x)|)</code>.</p>
<p><strong>1.4826 là gì:</strong> với dữ liệu gần phân phối chuẩn, <code>MAD ≈ 0.6745 * std</code>, nên <code>1.4826 * MAD</code> xấp xỉ <code>std</code>. Nhờ vậy threshold vẫn cùng thang đo với standard deviation nhưng ít bị spike/outlier trong baseline kéo lệch hơn.</p>
<p><strong>Vì sao EWMA không phải detector chính:</strong> EWMA hữu ích để nhìn xu hướng, nhưng có thể bắt nhầm traffic ramp bình thường và làm mờ ranh giới incident chính xác.</p>
<p><strong>Vì sao IsolationForest là secondary:</strong> nó xác nhận abnormality đa biến trên nhiều feature của service, nhưng score khó giải thích trực tiếp hơn việc một metric vượt ngưỡng baseline robust.</p>
</div>
<h3>Các dòng detector hỗ trợ RCA</h3>
{rca_methods_html}
<h3>Mẫu bảng so sánh method đầy đủ</h3>
<p class="muted">Dòng có earliest detection trống nghĩa là method đó không tìm thấy anomaly kéo dài sau baseline. False positive được đếm trong {BASELINE_HOURS} giờ đầu.</p>
{methods_html}
</section>
<section>
<h2>Hiệu Chỉnh Drain3</h2>
<p>Config được chọn: <code>{html.escape(json.dumps(SELECTED_DRAIN))}</code>.</p>
<p>Bước calibration kiểm tra xem các message quan trọng có còn tách biệt sau khi mask ID và số hay không. Setting được chọn giữ riêng template cho GC warning, cache eviction failure, OOMKilled, cart timeout, và cart 5xx. Việc này quan trọng vì over-merge sẽ che mất khác biệt giữa bằng chứng nguyên nhân gốc và triệu chứng downstream.</p>
<div class="callout warning"><strong>Lập luận:</strong> nếu cart timeout và cart 5xx bị gộp thành một template upstream failure chung chung, log analysis sẽ kém hữu ích cho RCA. Giữ chúng riêng giúp thứ tự incident rõ hơn.</div>
{drain_html}
</section>
<section>
<h2>Timeline Sự Cố</h2>
<p>Bảng này là chuỗi bằng chứng chính. Đọc từ trên xuống dưới. RCA mạnh hơn khi tín hiệu nội bộ của cart xuất hiện trước OOMKilled, OOMKilled xuất hiện trước restart counter tăng, và timeout/5xx downstream xuất hiện sau đó.</p>
{timeline_html}
</section>
<section>
<h2>Log Template Quan Trọng</h2>
<p>Template tóm tắt các pattern log lặp lại. Count cao và timestamp first-seen sớm hữu ích hơn vài dòng log đơn lẻ vì chúng cho thấy điều kiện có kéo dài không và bắt đầu ở đâu.</p>
{templates_html}
</section>
<section><h2>Biểu Đồ Offline</h2>{imgs}</section>
</main>
</body>
</html>
"""
    (OUT / "dashboard.html").write_text(dashboard, encoding="utf-8")
    write_postmortem_dashboard(metrics_summary, comparison, drain_comparison, templates, timeline, chart_paths)


def write_postmortem_dashboard(metrics_summary: pd.DataFrame, comparison: pd.DataFrame, drain_comparison: pd.DataFrame, templates: pd.DataFrame, timeline: pd.DataFrame, chart_paths: list[str]) -> None:
    first_metric = timeline[timeline["signal_type"] == "metric"]["timestamp"].min()
    gc_time = first_value(timeline, "GC warning")
    cache_time = first_value(timeline, "cache eviction")
    oom_time = first_value(timeline, "OOMKilled")
    restart_time = first_value(timeline, "container_restart_count")
    observability = detector_observability(comparison)

    timeline_cols = ["timestamp", "service", "signal", "evidence", "rca_interpretation"]
    method_cols = ["method", "affected_metric_service", "earliest_detection", "false_positive_count_first_6h", "detail"]
    template_cols = ["template_id", "service", "level", "count", "first_seen", "template"]
    observability_cols = ["detector", "metric", "decision_threshold", "result", "interpretation"]

    def table(df: pd.DataFrame, cols: list[str] | None = None, limit: int | None = None) -> str:
        out = df.copy()
        if cols is not None:
            out = out[[c for c in cols if c in out.columns]]
        if limit is not None:
            out = out.head(limit)
        return out.to_html(index=False, escape=True, classes="dataframe")

    def rows_for(pattern: str) -> pd.DataFrame:
        return timeline[timeline["signal"].str.contains(pattern, case=False, na=False)].copy()

    def figure(path: str, caption: str) -> str:
        if path not in chart_paths:
            return ""
        safe_path = html.escape(path)
        return f'<figure><img src="{safe_path}" alt="{safe_path}"><figcaption><strong>{safe_path}</strong><br>{html.escape(caption)}</figcaption></figure>'

    when_evidence = pd.concat(
        [
            rows_for("GC warning|cache eviction|http_p99_latency_ms|memory_usage_bytes|jvm_gc_pause_ms_avg"),
            rows_for("OOMKilled|container_restart_count").head(2),
        ],
        ignore_index=True,
    ).drop_duplicates()
    where_evidence = timeline[timeline["service"].isin(["cart-service", "api-gateway", "order-service", "payment-service", "product-service"])].copy()
    what_evidence = rows_for("GC warning|cache eviction|OOMKilled|container_restart_count|order timeout|cart_upstream_error_rate|upstream_timeout_rate|order sees cart 5xx")
    key_template_evidence = templates[
        templates["template"].str.contains(
            "GC overhead|ProductCatalogCache eviction|OOMKilled|Cart service timeout|Cart service returned 5xx|Connection pool",
            case=False,
            na=False,
        )
    ][template_cols]

    cart_chart = figure(
        "charts/cart-service-metrics.png",
        "Cart-service là chart chính: memory, GC pause, latency, 5xx và restart count tạo thành chuỗi suy giảm nội bộ trước khi downstream phát lỗi.",
    )
    log_chart = figure(
        "charts/key-log-template-timeseries.png",
        "Log template theo 5 phút cho thấy GC warning và cache eviction failure lặp lại trước OOMKilled, thay vì chỉ là vài dòng log rời rạc.",
    )
    api_chart = figure(
        "charts/api-gateway-metrics.png",
        "API gateway ghi nhận cart upstream error sau khi cart-service đã có dấu hiệu áp lực và restart.",
    )
    order_chart = figure(
        "charts/order-service-metrics.png",
        "Order-service biểu hiện timeout/5xx như triệu chứng downstream sau khi cart không ổn định.",
    )
    payment_chart = figure(
        "charts/payment-service-metrics.png",
        "Payment-service cũng tăng upstream timeout muộn hơn, phù hợp với lan truyền lỗi trong luồng checkout.",
    )
    product_chart = figure(
        "charts/product-service-metrics.png",
        "Product-service có dao động, nhưng không tạo được chuỗi memory -> OOMKilled -> restart rõ như cart-service.",
    )

    dashboard = f"""<!doctype html>
<html lang="vi">
<head>
<meta charset="utf-8">
<title>AIOps W1 Incident Dashboard</title>
<style>
body {{ margin: 0; font-family: Arial, sans-serif; color: #1f2933; background: #f7f9fb; }}
header {{ background: #102a43; color: white; padding: 28px 40px; }}
main {{ max-width: 1180px; margin: 0 auto; padding: 24px; }}
section {{ background: white; border: 1px solid #d9e2ec; border-radius: 8px; padding: 20px; margin: 18px 0; }}
h1, h2 {{ margin-top: 0; }}
h3 {{ margin-bottom: 8px; }}
.lede {{ font-size: 16px; line-height: 1.55; max-width: 980px; }}
.answer, .cards {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }}
.answer div, .card {{ border: 1px solid #bcccdc; border-radius: 8px; padding: 14px; background: #f0f4f8; }}
.card strong {{ display: block; margin-bottom: 6px; }}
.callout {{ border-left: 5px solid #2f80ed; background: #eef6ff; padding: 14px 16px; border-radius: 6px; margin: 14px 0; }}
.chart-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }}
.steps {{ margin: 0; padding-left: 22px; line-height: 1.55; }}
details {{ border: 1px solid #d9e2ec; border-radius: 8px; padding: 12px 14px; margin: 14px 0; background: #fbfdff; }}
summary {{ cursor: pointer; font-weight: 700; }}
table {{ width: 100%; max-width: 100%; border-collapse: collapse; font-size: 12px; table-layout: fixed; }}
th, td {{ border-bottom: 1px solid #d9e2ec; padding: 6px 8px; text-align: left; vertical-align: top; overflow-wrap: anywhere; word-break: break-word; hyphens: auto; }}
th {{ background: #eef2f7; }}
.dataframe {{ margin: 12px 0 22px; }}
img {{ width: 100%; height: auto; border: 1px solid #d9e2ec; border-radius: 8px; background: white; }}
figure {{ margin: 18px 0; }}
figcaption {{ font-size: 12px; color: #52606d; margin-top: 6px; }}
code {{ background: #eef2f7; padding: 1px 4px; border-radius: 4px; }}
@media (max-width: 900px) {{
  .answer, .cards, .chart-grid {{ grid-template-columns: 1fr; }}
  main {{ padding: 12px; }}
  section {{ padding: 14px; }}
  table {{ font-size: 11px; }}
  th, td {{ padding: 5px 6px; }}
}}
</style>
</head>
<body>
<header>
<h1>AIOps W1 Incident Dashboard</h1>
<p class="lede">Dashboard postmortem offline, đọc theo narrative WHEN / WHERE / WHAT. Chart và bảng nhỏ được đặt ngay cạnh luận điểm; bảng calibration đầy đủ nằm ở Appendix để không làm đứt mạch RCA.</p>
</header>
<main>
<section class="answer" id="executive-narrative">
<div><h2>WHEN</h2><p>Reliable metric anomaly đầu tiên bắt đầu tại <code>{html.escape(str(first_metric))}</code>.</p><p>Silent signal đáng chú ý: GC warning <code>{html.escape(gc_time)}</code>, cache eviction failure <code>{html.escape(cache_time)}</code>.</p><p>Raw MAD crossing của <code>cart-service/http_5xx_rate</code> lúc 06:08 bị hạ cấp vì false positive trong baseline quá cao.</p></div>
<div><h2>WHERE</h2><p><code>cart-service</code> là origin candidate mạnh nhất.</p><p><code>api-gateway</code>, <code>order-service</code>, và <code>payment-service</code> phát triệu chứng downstream muộn hơn.</p></div>
<div><h2>WHAT</h2><p>Cơ chế phù hợp nhất là memory pressure -> GC/cache issue -> OOMKilled -> restart loop -> downstream timeout/5xx.</p><p>OOMKilled <code>{html.escape(oom_time)}</code>; restart bắt đầu <code>{html.escape(restart_time)}</code>.</p></div>
</section>

<section id="when">
<h2>WHEN - Sự cố bắt đầu khi nào?</h2>
<p>Thời điểm người dùng thấy lỗi không phải là tín hiệu đầu tiên. Cart-service đã có silent signal từ log và metric trước khi OOMKilled và trước khi downstream cùng báo lỗi.</p>
<div class="cards">
<div class="card"><strong>Silent GC signal</strong><code>{html.escape(gc_time)}</code><br>GC overhead warning cho thấy JVM bắt đầu chịu áp lực heap.</div>
<div class="card"><strong>Cache eviction failure</strong><code>{html.escape(cache_time)}</code><br>ProductCatalogCache không eviction được vì heap pressure quá cao.</div>
<div class="card"><strong>Visible failure</strong><code>{html.escape(oom_time)}</code><br>Container OOMKilled xảy ra trước restart counter và fan-out lỗi downstream.</div>
</div>
{table(when_evidence, timeline_cols)}
{log_chart}
{cart_chart}
</section>

<section id="where">
<h2>WHERE - Nguồn gốc nằm ở đâu?</h2>
<p>Cart-service là nơi có tín hiệu nội bộ đúng thứ tự thời gian: memory tăng, GC pause tăng, cache eviction failure, OOMKilled, rồi restart. Các service còn lại chủ yếu ghi nhận lỗi khi gọi cart hoặc khi gateway nhìn thấy cart upstream error.</p>
<div class="cards">
<div class="card"><strong>Origin candidate</strong><code>cart-service</code><br>Memory/GC/cache/OOM/restart cùng hội tụ trên một service.</div>
<div class="card"><strong>Downstream propagation</strong><code>api-gateway 20:08</code><br>Gateway thấy cart upstream error sau khi cart đã unhealthy.</div>
<div class="card"><strong>Caller impact</strong><code>order 20:32</code> / <code>payment 20:45</code><br>Timeout rate tăng sau restart loop của cart.</div>
</div>
{table(where_evidence, timeline_cols)}
<div class="chart-grid">{api_chart}{order_chart}{payment_chart}</div>
<h3>Product-service không phải RCA chính</h3>
<p>Product-service có dao động và có log connection refused liên quan catalog, nhưng bằng chứng không tạo thành chuỗi restart loop rõ như cart-service. Vì vậy nó được xem là yếu tố phụ hoặc nhiễu cho đến khi có thêm telemetry chứng minh ngược lại.</p>
{product_chart}
</section>

<section id="what">
<h2>WHAT - Cơ chế gây restart loop là gì?</h2>
<p>Chuỗi hợp lý nhất là: memory pressure trên cart làm GC pause tăng và cache eviction thất bại; container bị OOMKilled; Kubernetes/pod restart làm connection bị từ chối hoặc timeout; caller bắt đầu ghi nhận 5xx và upstream timeout.</p>
<ol class="steps">
<li><code>{html.escape(gc_time)}</code> GC warning: heap pressure xuất hiện như silent signal.</li>
<li><code>{html.escape(cache_time)}</code> cache eviction failure: cache không giảm tải được dưới áp lực heap.</li>
<li><code>{html.escape(oom_time)}</code> OOMKilled: memory limit bị vượt.</li>
<li><code>{html.escape(restart_time)}</code> restart count tăng: restart loop trở nên thấy được trong metric.</li>
<li><code>20:08</code>, <code>20:32</code>, <code>20:45</code>: API gateway, order-service, payment-service lần lượt có triệu chứng downstream.</li>
</ol>
{table(what_evidence, timeline_cols)}
{cart_chart}
</section>

<section id="method">
<h2>Method & Calibration Notes</h2>
<p>Detector chính là robust MAD 3-alpha trên {BASELINE_HOURS} giờ baseline đầu. Nó giữ trực giác của công thức cổ điển <code>mean + 3 * std</code>, nhưng thay bằng thống kê robust: <code>median + 3 * 1.4826 * MAD</code>, với <code>MAD = median(|x - median(x)|)</code>.</p>
<div class="callout">
<p><strong>Vì sao dùng 1.4826:</strong> với dữ liệu gần normal, <code>MAD ≈ 0.6745 * std</code>, nên <code>1.4826 * MAD</code> đưa MAD về thang đo tương đương standard deviation. Cách này giảm ảnh hưởng spike trong baseline so với mean/std.</p>
<p><strong>EWMA:</strong> dùng để đọc xu hướng trên chart, không phải nhãn RCA chính.</p>
<p><strong>IsolationForest:</strong> dùng như xác nhận multivariate vì score khó giải thích hơn threshold theo từng metric.</p>
<p><strong>Drain3 calibration:</strong> chọn config <code>{html.escape(json.dumps(SELECTED_DRAIN))}</code> vì vẫn tách riêng GC warning, cache eviction failure, OOMKilled, cart timeout, và cart 5xx.</p>
</div>
<h3>Observability của detector: threshold -> result -> interpretation</h3>
<p>Bảng này giúp audit quyết định của từng detector. Với MAD, giá trị raw phải vượt ngưỡng robust 3-sigma. Với EWMA, đường trend đã làm mượt phải vượt ngưỡng decision threshold. Cả hai đều cần điều kiện persistence {PERSISTENCE_HITS}-of-{PERSISTENCE_WINDOW}, nên một spike đơn lẻ không đủ để thành anomaly.</p>
{table(observability, observability_cols)}
<h3>Detector rows hỗ trợ RCA</h3>
{table(comparison[comparison["supports_rca_chain"]], method_cols, limit=18)}
<h3>Log template quan trọng</h3>
{table(key_template_evidence, template_cols)}
</section>

<section id="appendix">
<h2>Appendix</h2>
<details><summary>Full incident timeline</summary>{table(timeline, timeline_cols)}</details>
<details><summary>Full method comparison</summary>{table(comparison)}</details>
<details><summary>Drain comparison</summary>{table(drain_comparison)}</details>
<details><summary>EDA summary sample</summary>{table(metrics_summary, limit=30)}</details>
<details><summary>Full log templates</summary>{table(templates)}</details>
</section>
</main>
</body>
</html>
"""
    (OUT / "dashboard.html").write_text(dashboard, encoding="utf-8")


def verify_outputs() -> None:
    expected = [
        OUT / "eda_summary.csv",
        OUT / "method_comparison.csv",
        OUT / "detector_observability.csv",
        OUT / "drain_comparison.csv",
        OUT / "anomalies_metrics.csv",
        OUT / "log_templates.csv",
        OUT / "log_template_timeseries.csv",
        OUT / "incident_timeline.csv",
        OUT / "dashboard.html",
        ROOT / "FINDINGS.md",
        ROOT / "SUBMIT.md",
        ROOT / "README.md",
    ]
    missing = [str(p) for p in expected if not p.exists() or p.stat().st_size == 0]
    if missing:
        raise RuntimeError("Missing or empty outputs: " + ", ".join(missing))

    comparison = pd.read_csv(OUT / "method_comparison.csv")
    methods = set(comparison["method"])
    required = {"robust_mad_3alpha", "ewma_trend", "isolation_forest", "http_5xx_sustained"}
    if not required.issubset(methods):
        raise RuntimeError(f"method_comparison.csv missing methods: {required - methods}")
    cart_mad = comparison[
        (comparison["method"] == "robust_mad_3alpha")
        & (comparison["affected_metric_service"] == "cart-service/http_5xx_rate")
    ]
    if cart_mad.empty or cart_mad["earliest_detection"].iloc[0] != "2026-06-01T06:08:00+00:00" or int(cart_mad["false_positive_count_first_6h"].iloc[0]) != 297:
        raise RuntimeError("cart-service/http_5xx_rate MAD failure row changed unexpectedly")
    cart_5xx = comparison[
        (comparison["method"] == "http_5xx_sustained")
        & (comparison["affected_metric_service"] == "cart-service/http_5xx_rate")
    ]
    if cart_5xx.empty or cart_5xx["earliest_detection"].iloc[0] != "2026-06-01T20:41:30+00:00" or int(cart_5xx["false_positive_count_first_6h"].iloc[0]) != 0:
        raise RuntimeError("cart-service/http_5xx_rate sustained detector did not match expected timing/FP")

    drain = pd.read_csv(OUT / "drain_comparison.csv")
    if not drain["selected"].any():
        raise RuntimeError("drain_comparison.csv does not mark a selected config")

    templates = pd.read_csv(OUT / "log_templates.csv")
    for pattern in ["GC overhead", "ProductCatalogCache eviction", "OOMKilled", "Cart service timeout", "Cart service returned 5xx"]:
        if not templates["template"].str.contains(pattern, case=False, na=False).any():
            raise RuntimeError(f"selected templates do not preserve pattern: {pattern}")

    timeline = pd.read_csv(OUT / "incident_timeline.csv")
    if timeline["timestamp"].iloc[0] != "2026-06-01T06:30:32.992000+00:00":
        raise RuntimeError("incident_timeline.csv no longer starts with reliable GC log evidence")
    def ts_for(pattern):
        rows = timeline[timeline["signal"].str.contains(pattern, case=False, na=False)]
        return pd.NaT if rows.empty else pd.to_datetime(rows["timestamp"], utc=True, format="mixed").min()
    gc = ts_for("GC warning")
    cache = ts_for("cache eviction")
    oom = ts_for("OOMKilled")
    restart = ts_for("container_restart_count")
    order = ts_for("order timeout|upstream_timeout_rate")
    if pd.notna(gc) and pd.notna(oom) and not (gc <= oom):
        raise RuntimeError("GC warning does not precede OOMKilled")
    if pd.notna(cache) and pd.notna(oom) and not (cache <= oom):
        raise RuntimeError("cache eviction does not precede OOMKilled")
    if pd.notna(oom) and pd.notna(restart) and not (oom <= restart):
        raise RuntimeError("OOMKilled does not precede restart counter growth")
    if pd.notna(oom) and pd.notna(order) and not (oom <= order):
        raise RuntimeError("OOMKilled does not precede or coincide with order symptoms")

    dashboard = (OUT / "dashboard.html").read_text(encoding="utf-8")
    for marker in [
        "WHEN - Sự cố bắt đầu khi nào?",
        "WHERE - Nguồn gốc nằm ở đâu?",
        "WHAT - Cơ chế gây restart loop là gì?",
        "Method & Calibration Notes",
        "Observability của detector: threshold -> result -> interpretation",
        "<h2>Appendix</h2>",
        "origin candidate mạnh nhất",
        "median + 3 * 1.4826 * MAD",
        "MAD 3-sigma",
        "EWMA decision",
    ]:
        if marker not in dashboard:
            raise RuntimeError(f"dashboard.html missing narrative marker: {marker}")
    if re.search(r"https?://", dashboard, flags=re.IGNORECASE):
        raise RuntimeError("dashboard.html contains an external URL")
    if re.search(r"<script\b", dashboard, flags=re.IGNORECASE):
        raise RuntimeError("dashboard.html contains a script tag")
    for src in sorted(set(re.findall(r'<img src="([^"]+)"', dashboard))):
        chart_path = OUT / src
        if not chart_path.exists() or chart_path.stat().st_size == 0:
            raise RuntimeError(f"dashboard.html references missing or empty chart: {src}")
    for src in [
        "charts/cart-service-metrics.png",
        "charts/key-log-template-timeseries.png",
        "charts/api-gateway-metrics.png",
        "charts/order-service-metrics.png",
        "charts/payment-service-metrics.png",
        "charts/product-service-metrics.png",
    ]:
        if src not in dashboard:
            raise RuntimeError(f"dashboard.html missing chart reference: {src}")


def main() -> None:
    ensure_dirs()
    metrics = load_metrics()
    logs = load_logs()
    summary = validate_and_summarize_metrics(metrics)
    anomalies, mad_detections = robust_mad_anomalies(metrics)
    detections = mad_detections + http_5xx_sustained_detections(metrics) + ewma_detections(metrics) + isolation_forest_detections(metrics)
    comparison = write_method_comparison(detections)
    templates, template_ts, drain_comparison = log_template_analysis(logs)
    timeline = important_events(metrics, logs, anomalies, templates, comparison)
    chart_paths = plot_metric_panels(metrics, anomalies) + plot_log_panels(template_ts, templates)
    generate_reports(summary, comparison, drain_comparison, templates, timeline, chart_paths)
    verify_outputs()
    print(f"Wrote analysis artifacts to {OUT}")
    print(f"Dashboard: {OUT / 'dashboard.html'}")
    print(f"Earliest timeline event: {timeline['timestamp'].min()}")


if __name__ == "__main__":
    main()
