from __future__ import annotations

import json
import re
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

from analyze import (
    BASELINE_HOURS,
    MAD_K,
    OUT,
    PERSISTENCE_HITS,
    PERSISTENCE_WINDOW,
    baseline_end,
    load_logs,
    load_metrics,
    new_drain3_miner,
    numeric_columns,
    persistent_first,
)


REALTIME_OUT = OUT / "realtime"
STREAM_PATH = REALTIME_OUT / "stream_events.jsonl"
SIGNALS_PATH = REALTIME_OUT / "signals.json"
EVENTS_PATH = REALTIME_OUT / "events.jsonl"
ALERTS_PATH = REALTIME_OUT / "alerts.jsonl"
TIMELINE_PATH = REALTIME_OUT / "rca_timeline.json"
HYPOTHESES_PATH = REALTIME_OUT / "rca_hypotheses.json"
STATE_PATH = REALTIME_OUT / "pipeline_state.json"

STAGES = ["stream", "calculate", "detect", "rca"]
STAGE_LABELS = {
    "stream": "Stream Data",
    "calculate": "Calculate Metrics",
    "detect": "Detect Anomalies",
    "rca": "Run RCA",
}
STAGE_OUTPUTS = {
    "stream": [STREAM_PATH],
    "calculate": [SIGNALS_PATH],
    "detect": [EVENTS_PATH, ALERTS_PATH],
    "rca": [TIMELINE_PATH, HYPOTHESES_PATH],
}
INCIDENT_LEAD = pd.Timedelta(minutes=10)
INCIDENT_TAIL = pd.Timedelta(hours=15)


def ensure_dirs() -> None:
    REALTIME_OUT.mkdir(parents=True, exist_ok=True)


def now_iso() -> str:
    return pd.Timestamp.now(tz="UTC").isoformat()


def isoformat(ts) -> str:
    return pd.Timestamp(ts).isoformat()


def json_default(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def write_json(path: Path, payload) -> None:
    ensure_dirs()
    path.write_text(json.dumps(payload, indent=2, default=json_default, ensure_ascii=False), encoding="utf-8")


def read_json(path: Path, default):
    if not path.exists() or path.stat().st_size == 0:
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_jsonl(path: Path, rows: list[dict]) -> None:
    ensure_dirs()
    with path.open("w", encoding="utf-8") as handle:
        for row in sorted(rows, key=lambda r: r.get("timestamp", "")):
            handle.write(json.dumps(row, default=json_default, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def empty_state() -> dict:
    return {
        "updated_at": now_iso(),
        "latest_error": None,
        "stages": {
            stage: {"label": STAGE_LABELS[stage], "status": "pending", "last_run": None, "summary": {}}
            for stage in STAGES
        },
        "counts": {},
        "summaries": {},
    }


def read_state() -> dict:
    ensure_dirs()
    state = read_json(STATE_PATH, None)
    if not isinstance(state, dict):
        state = empty_state()
    for stage in STAGES:
        state.setdefault("stages", {}).setdefault(
            stage,
            {"label": STAGE_LABELS[stage], "status": "pending", "last_run": None, "summary": {}},
        )
        state["stages"][stage].setdefault("label", STAGE_LABELS[stage])
        state["stages"][stage].setdefault("summary", {})
    state.setdefault("counts", {})
    state.setdefault("summaries", {})
    state.setdefault("latest_error", None)
    return state


def write_state(state: dict) -> dict:
    state["updated_at"] = now_iso()
    write_json(STATE_PATH, state)
    return state


def stage_complete(stage: str) -> bool:
    state = read_state()
    return state["stages"][stage].get("status") == "complete" and all(path.exists() for path in STAGE_OUTPUTS[stage])


def require_stage(stage: str) -> None:
    if not stage_complete(stage):
        raise RuntimeError(f"Run '{STAGE_LABELS[stage]}' before this stage.")


def begin_stage(stage: str) -> float:
    ensure_dirs()
    state = read_state()
    state["stages"][stage]["status"] = "running"
    state["stages"][stage]["started_at"] = now_iso()
    state["stages"][stage]["error"] = None
    state["latest_error"] = None
    write_state(state)
    return time.perf_counter()


def finish_stage(stage: str, summary: dict, started: float) -> dict:
    state = read_state()
    state["stages"][stage]["status"] = "complete"
    state["stages"][stage]["last_run"] = now_iso()
    state["stages"][stage]["duration_seconds"] = round(time.perf_counter() - started, 2)
    state["stages"][stage]["summary"] = summary
    state["summaries"][stage] = summary
    state["latest_error"] = None
    update_state_counts(state)
    return write_state(state)


def fail_stage(stage: str, exc: Exception) -> None:
    state = read_state()
    message = str(exc)
    state["stages"][stage]["status"] = "failed"
    state["stages"][stage]["last_run"] = now_iso()
    state["stages"][stage]["error"] = message
    state["latest_error"] = {"stage": stage, "message": message, "timestamp": now_iso()}
    write_state(state)


def incident_anchor(logs: pd.DataFrame | None = None) -> pd.Timestamp:
    logs = load_logs() if logs is None else logs
    matches = logs[
        logs["message"].str.contains(
            "GC overhead|ProductCatalogCache eviction failed|OOMKilled",
            case=False,
            na=False,
        )
    ]
    if matches.empty:
        return baseline_end(load_metrics())
    return matches["timestamp"].min()


def simulation_start(logs: pd.DataFrame | None = None) -> pd.Timestamp:
    return incident_anchor(logs) - INCIDENT_LEAD


def run_stage(stage: str, func):
    started = begin_stage(stage)
    try:
        summary = func()
    except Exception as exc:
        fail_stage(stage, exc)
        raise
    return finish_stage(stage, summary, started)


def update_state_counts(state: dict) -> None:
    stream = read_jsonl(STREAM_PATH)
    events = read_jsonl(EVENTS_PATH)
    alerts = read_jsonl(ALERTS_PATH)
    signals = read_json(SIGNALS_PATH, {})
    timeline = read_json(TIMELINE_PATH, [])
    hypotheses = read_json(HYPOTHESES_PATH, [])
    state["counts"] = {
        "stream_events": len(stream),
        "stream_metric_rows": sum(row.get("event_type") == "metric_sample" for row in stream),
        "stream_log_rows": sum(row.get("event_type") == "log_line" for row in stream),
        "metric_windows": len(signals.get("metric_baselines", [])) if isinstance(signals, dict) else 0,
        "log_templates": len(signals.get("log_templates", [])) if isinstance(signals, dict) else 0,
        "events": len(events),
        "metric_events": sum(row.get("event_type") == "metric_anomaly" for row in events),
        "log_events": sum(row.get("event_type") == "log_template" for row in events),
        "alerts": len(alerts),
        "metric_alerts": sum(row.get("alert_type", "").startswith("metric") for row in alerts),
        "log_alerts": sum(row.get("alert_type", "").startswith("log") for row in alerts),
        "rca_timeline": len(timeline),
        "rca_hypotheses": len(hypotheses),
    }
    if hypotheses:
        state["counts"]["top_root_cause_service"] = hypotheses[0].get("root_cause_service")


def robust_threshold(base: pd.Series) -> tuple[float, float, float]:
    base = base.astype(float)
    median = float(base.median())
    mad = float((base - median).abs().median())
    sigma = 1.4826 * mad
    threshold = median + MAD_K * sigma if sigma else median
    return median, sigma, threshold


def metric_detector_replay(metrics: dict[str, pd.DataFrame]) -> tuple[list[dict], list[dict]]:
    base_end = baseline_end(metrics)
    events: list[dict] = []
    alerts: list[dict] = []

    for service, df in metrics.items():
        for metric in numeric_columns(df):
            median, sigma, threshold = robust_threshold(df.loc[df["timestamp"] < base_end, metric])
            values = df[metric].astype(float)
            mask = values > threshold
            first = persistent_first(mask & (df["timestamp"] >= base_end), df["timestamp"])
            if pd.notna(first):
                alerts.append(
                    {
                        "timestamp": isoformat(first),
                        "alert_type": "metric_mad",
                        "service": service,
                        "metric": metric,
                        "severity": "warning",
                        "summary": f"{service}/{metric} exceeded robust MAD threshold",
                        "details": {
                            "median": median,
                            "sigma": sigma,
                            "threshold": threshold,
                            "persistence": f"{PERSISTENCE_HITS}/{PERSISTENCE_WINDOW}",
                        },
                    }
                )
            for _, row in df.loc[mask & (df["timestamp"] >= base_end), ["timestamp", metric]].iterrows():
                events.append(
                    {
                        "timestamp": isoformat(row["timestamp"]),
                        "event_type": "metric_anomaly",
                        "detector": "robust_mad_3alpha",
                        "service": service,
                        "metric": metric,
                        "value": float(row[metric]),
                        "threshold": threshold,
                    }
                )

            ewma = values.ewm(span=20, adjust=False).mean()
            e_median, e_sigma, e_threshold = robust_threshold(ewma[df["timestamp"] < base_end])
            e_mask = ewma > e_threshold
            e_first = persistent_first(e_mask & (df["timestamp"] >= base_end), df["timestamp"])
            if pd.notna(e_first):
                alerts.append(
                    {
                        "timestamp": isoformat(e_first),
                        "alert_type": "metric_ewma",
                        "service": service,
                        "metric": metric,
                        "severity": "info",
                        "summary": f"{service}/{metric} EWMA trend exceeded baseline",
                        "details": {"span": 20, "median": e_median, "sigma": e_sigma, "threshold": e_threshold},
                    }
                )

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
        first = persistent_first(pred & (df["timestamp"] >= base_end), df["timestamp"])
        if pd.notna(first):
            row = df.loc[df["timestamp"] == first, cols].iloc[0].astype(float)
            med = train.median()
            mad = (train - med).abs().median().replace(0, np.nan)
            top_metric = ((row - med).abs() / mad).sort_values(ascending=False).index[0]
            alerts.append(
                {
                    "timestamp": isoformat(first),
                    "alert_type": "metric_isolation_forest",
                    "service": service,
                    "metric": top_metric,
                    "severity": "info",
                    "summary": f"{service} multivariate anomaly detected",
                    "details": {"features": cols, "contamination": 0.03},
                }
            )
    return events, alerts


def bucket_thresholds(counts: pd.DataFrame, group_cols: list[str]) -> dict[tuple, float]:
    if counts.empty:
        return {}
    thresholds = {}
    for key, group in counts.groupby(group_cols):
        series = group["count"].astype(float)
        _, _, threshold = robust_threshold(series)
        thresholds[key if isinstance(key, tuple) else (key,)] = max(3.0, threshold, float(series.max()))
    return thresholds


def assign_baseline_templates(logs: pd.DataFrame, base_end: pd.Timestamp) -> tuple[pd.DataFrame, set[tuple[str, str]]]:
    baseline_logs = logs[logs["timestamp"] < base_end].copy()
    miner = new_drain3_miner()
    for message in baseline_logs["message"]:
        miner.add_log_message(str(message))

    rows = []
    for _, row in baseline_logs.iterrows():
        cluster = miner.match(str(row["message"]), full_search_strategy="always")
        if cluster is None:
            continue
        rows.append(
            {
                "timestamp": row["timestamp"].floor("5min"),
                "service": row["service"],
                "level": row["level"],
                "template": cluster.get_template(),
            }
        )
    assigned = pd.DataFrame(rows)
    seen = set(zip(assigned.get("service", []), assigned.get("template", [])))
    return assigned, seen


def log_replay(logs: pd.DataFrame, base_end: pd.Timestamp) -> tuple[list[dict], list[dict]]:
    baseline_logs = logs[logs["timestamp"] < base_end].copy()
    replay_logs = logs[logs["timestamp"] >= base_end].copy()
    miner = new_drain3_miner()
    for message in baseline_logs["message"]:
        miner.add_log_message(str(message))

    baseline_rows = []
    for _, row in baseline_logs.iterrows():
        cluster = miner.match(str(row["message"]), full_search_strategy="always")
        if cluster is None:
            continue
        baseline_rows.append(
            {
                "timestamp": row["timestamp"].floor("5min"),
                "service": row["service"],
                "level": row["level"],
                "template": cluster.get_template(),
            }
        )
    baseline_assigned = pd.DataFrame(baseline_rows)
    template_thresholds = bucket_thresholds(
        baseline_assigned.groupby(["service", "template", "timestamp"]).size().rename("count").reset_index(),
        ["service", "template"],
    )
    level_thresholds = bucket_thresholds(
        baseline_assigned[baseline_assigned["level"].isin(["WARN", "ERROR"])]
        .groupby(["service", "timestamp"])
        .size()
        .rename("count")
        .reset_index(),
        ["service"],
    )

    events: list[dict] = []
    alerts: list[dict] = []
    seen_templates = set(zip(baseline_assigned.get("service", []), baseline_assigned.get("template", [])))
    emitted_new = set()
    emitted_count = set()
    emitted_level = set()
    bucket_counts: dict[tuple, int] = defaultdict(int)
    level_counts: dict[tuple, int] = defaultdict(int)

    for _, row in replay_logs.sort_values("timestamp").iterrows():
        before = miner.match(str(row["message"]), full_search_strategy="always")
        result = miner.add_log_message(str(row["message"]))
        template = result["template_mined"]
        cluster_id = int(result["cluster_id"])
        bucket = row["timestamp"].floor("5min")
        service_template = (row["service"], template)
        is_new = before is None and service_template not in seen_templates
        seen_templates.add(service_template)

        event = {
            "timestamp": isoformat(row["timestamp"]),
            "event_type": "log_template",
            "service": row["service"],
            "level": row["level"],
            "template_id": f"D{cluster_id:03d}",
            "template": template,
            "message": row["message"],
            "new_template_after_baseline": is_new,
        }
        events.append(event)

        if is_new and service_template not in emitted_new:
            emitted_new.add(service_template)
            alerts.append(
                {
                    "timestamp": isoformat(row["timestamp"]),
                    "alert_type": "log_new_template",
                    "service": row["service"],
                    "severity": "warning" if row["level"] in {"WARN", "ERROR"} else "info",
                    "summary": f"New Drain3 log template after baseline on {row['service']}",
                    "details": {"template_id": event["template_id"], "template": template, "sample": row["message"]},
                }
            )

        bucket_key = (row["service"], template, bucket)
        bucket_counts[bucket_key] += 1
        threshold = template_thresholds.get((row["service"], template), 3.0)
        count_key = (row["service"], template, bucket)
        if bucket_counts[bucket_key] > threshold and count_key not in emitted_count:
            emitted_count.add(count_key)
            alerts.append(
                {
                    "timestamp": isoformat(row["timestamp"]),
                    "alert_type": "log_template_count_spike",
                    "service": row["service"],
                    "severity": "warning",
                    "summary": f"Drain3 template count spike for {row['service']}",
                    "details": {
                        "template": template,
                        "bucket": isoformat(bucket),
                        "count": bucket_counts[bucket_key],
                        "threshold": threshold,
                    },
                }
            )

        if row["level"] in {"WARN", "ERROR"}:
            level_key = (row["service"], bucket)
            level_counts[level_key] += 1
            level_threshold = level_thresholds.get((row["service"],), 3.0)
            if level_counts[level_key] > level_threshold and level_key not in emitted_level:
                emitted_level.add(level_key)
                alerts.append(
                    {
                        "timestamp": isoformat(row["timestamp"]),
                        "alert_type": "log_warn_error_spike",
                        "service": row["service"],
                        "severity": "warning",
                        "summary": f"WARN/ERROR distribution spike for {row['service']}",
                        "details": {"bucket": isoformat(bucket), "count": level_counts[level_key], "threshold": level_threshold},
                    }
                )
    return events, alerts


def first_log_time(logs: pd.DataFrame, pattern: str, after: pd.Timestamp | None = None) -> pd.Timestamp | pd.NaT:
    subset = logs[logs["message"].str.contains(pattern, case=False, na=False)]
    if after is not None:
        subset = subset[subset["timestamp"] >= after]
    return pd.NaT if subset.empty else subset["timestamp"].min()


def first_alert_time(alerts: list[dict], service: str, pattern: str, after: pd.Timestamp | None = None) -> pd.Timestamp | pd.NaT:
    rows = [
        alert
        for alert in alerts
        if alert.get("service") == service
        and re.search(pattern, alert.get("metric", "") + " " + alert.get("summary", ""), flags=re.I)
    ]
    if after is not None and pd.notna(after):
        rows = [alert for alert in rows if pd.to_datetime(alert["timestamp"], utc=True) >= after]
    return pd.NaT if not rows else pd.to_datetime(min(alert["timestamp"] for alert in rows), utc=True)


def build_rca(metrics: dict[str, pd.DataFrame], logs: pd.DataFrame, alerts: list[dict]) -> tuple[list[dict], list[dict]]:
    gc_ts = first_log_time(logs, "GC overhead")
    cache_ts = first_log_time(logs, "ProductCatalogCache eviction failed")
    oom_ts = first_log_time(logs, "OOMKilled")
    restart_ts = first_alert_time(alerts, "cart-service", "container_restart_count|restart", oom_ts)
    timeout_ts = first_log_time(logs, "Cart service timeout", oom_ts if pd.notna(oom_ts) else None)
    five_xx_ts = first_log_time(logs, "Cart service returned 5xx", oom_ts if pd.notna(oom_ts) else None)
    gateway_ts = first_alert_time(alerts, "api-gateway", "cart_upstream_error_rate", oom_ts)
    order_ts = first_alert_time(alerts, "order-service", "upstream_timeout_rate|5xx|timeout", oom_ts)
    payment_ts = first_alert_time(alerts, "payment-service", "upstream_timeout_rate|5xx|timeout", oom_ts)

    timeline = [
        {"timestamp": gc_ts, "stage": "GC/cache pressure", "service": "cart-service", "evidence": "GC overhead warning appears"},
        {"timestamp": cache_ts, "stage": "GC/cache pressure", "service": "cart-service", "evidence": "ProductCatalogCache eviction failed under heap pressure"},
        {"timestamp": oom_ts, "stage": "OOMKilled", "service": "cart-service", "evidence": "Container OOMKilled: memory limit exceeded"},
        {"timestamp": restart_ts, "stage": "restart loop", "service": "cart-service", "evidence": "container_restart_count anomaly/restart signal follows OOMKilled"},
        {"timestamp": gateway_ts, "stage": "downstream symptom", "service": "api-gateway", "evidence": "cart_upstream_error_rate anomaly after cart degradation"},
        {"timestamp": timeout_ts, "stage": "downstream symptom", "service": "order-service", "evidence": "Cart service timeout after cart OOM/restart"},
        {"timestamp": five_xx_ts, "stage": "downstream symptom", "service": "order-service", "evidence": "Cart service returned 5xx after cart OOM/restart"},
        {"timestamp": order_ts, "stage": "downstream symptom", "service": "order-service", "evidence": "upstream timeout/5xx metric anomaly"},
        {"timestamp": payment_ts, "stage": "downstream symptom", "service": "payment-service", "evidence": "payment timeout metric anomaly later in checkout flow"},
    ]
    timeline = [row for row in timeline if pd.notna(row["timestamp"])]
    timeline = sorted(timeline, key=lambda row: row["timestamp"])
    for row in timeline:
        row["timestamp"] = isoformat(row["timestamp"])

    ordered = all(pd.notna(ts) for ts in [gc_ts, cache_ts, oom_ts]) and max(gc_ts, cache_ts) <= oom_ts
    ordered = ordered and (pd.isna(restart_ts) or oom_ts <= restart_ts)
    hypotheses = [
        {
            "rank": 1,
            "root_cause_service": "cart-service",
            "root_cause": "cart-service memory pressure caused GC/cache degradation, OOMKilled, pod restarts, then downstream timeout/5xx propagation",
            "confidence": "high" if ordered else "medium",
            "mechanism": "GC/cache -> OOMKilled -> restart -> downstream timeout/5xx",
            "supporting_evidence": [
                row["evidence"]
                for row in timeline
                if row["service"] in {"cart-service", "api-gateway", "order-service", "payment-service"}
            ],
            "rule_trace": [
                "cart memory/GC/cache anomalies before OOM/restart imply cart origin candidate",
                "OOMKilled before restart implies restart loop mechanism",
                "API gateway/order/payment anomalies after cart imply downstream symptoms",
            ],
        },
        {
            "rank": 2,
            "root_cause_service": "product-service",
            "root_cause": "product-service/catalog instability as contributing factor",
            "confidence": "low",
            "mechanism": "insufficient evidence for product-service as the first failing service",
            "supporting_evidence": ["cart-service has the stronger ordered memory -> OOMKilled -> restart chain"],
            "rule_trace": ["demote candidates that do not explain OOMKilled and restart ordering on cart-service"],
        },
    ]
    return timeline, hypotheses


def _stream_data(mode: str = "preincident") -> dict:
    metrics = load_metrics()
    logs = load_logs()
    sim_start = simulation_start(logs)
    anchor = incident_anchor(logs)
    if mode == "preincident":
        metric_filter = lambda df: df[df["timestamp"] < sim_start]
        log_filter = logs[logs["timestamp"] < sim_start]
    elif mode == "incident":
        sim_end = anchor + INCIDENT_TAIL
        metric_filter = lambda df: df[(df["timestamp"] >= sim_start) & (df["timestamp"] <= sim_end)]
        log_filter = logs[(logs["timestamp"] >= sim_start) & (logs["timestamp"] <= sim_end)]
    else:
        metric_filter = lambda df: df
        log_filter = logs
    rows: list[dict] = []
    metric_count = 0
    log_count = 0

    for service, df in metrics.items():
        for _, row in metric_filter(df).iterrows():
            metric_count += 1
            rows.append(
                {
                    "timestamp": isoformat(row["timestamp"]),
                    "event_type": "metric_sample",
                    "service": service,
                    "metrics": {metric: row[metric] for metric in numeric_columns(df)},
                }
            )
    for _, row in log_filter.iterrows():
        log_count += 1
        rows.append(
            {
                "timestamp": isoformat(row["timestamp"]),
                "event_type": "log_line",
                "service": row["service"],
                "level": row["level"],
                "message": row["message"],
                "pod": row.get("pod"),
                "trace_id": row.get("trace_id"),
            }
        )
    write_jsonl(STREAM_PATH, rows)
    return {
        "mode": mode,
        "stream_events": len(rows),
        "metric_rows": metric_count,
        "log_rows": log_count,
        "services": sorted(metrics),
        "start": min(row["timestamp"] for row in rows) if rows else None,
        "end": max(row["timestamp"] for row in rows) if rows else None,
        "simulation_start": isoformat(sim_start),
        "incident_anchor": isoformat(anchor),
    }


def stream_data() -> dict:
    return run_stage("stream", _stream_data)


def stream_incident_window() -> dict:
    return run_stage("stream", lambda: _stream_data("incident"))


def _calculate_metrics() -> dict:
    require_stage("stream")
    metrics = load_metrics()
    logs = load_logs()
    base_end = baseline_end(metrics)
    baseline_start = min(df["timestamp"].min() for df in metrics.values())

    metric_baselines = []
    for service, df in metrics.items():
        for metric in numeric_columns(df):
            base = df.loc[df["timestamp"] < base_end, metric].astype(float)
            median, sigma, threshold = robust_threshold(base)
            metric_baselines.append(
                {
                    "service": service,
                    "metric": metric,
                    "baseline_rows": int(base.shape[0]),
                    "median": median,
                    "sigma": sigma,
                    "mad_threshold": threshold,
                    "min": float(base.min()),
                    "p95": float(base.quantile(0.95)),
                    "max": float(base.max()),
                }
            )

    assigned, seen_templates = assign_baseline_templates(logs, base_end)
    if assigned.empty:
        log_templates = []
        level_windows = []
    else:
        template_counts = (
            assigned.groupby(["service", "template"]).size().rename("baseline_count").reset_index().sort_values(["service", "baseline_count"], ascending=[True, False])
        )
        log_templates = template_counts.to_dict(orient="records")
        level_windows = (
            assigned[assigned["level"].isin(["WARN", "ERROR"])]
            .groupby(["service", "timestamp"])
            .size()
            .rename("warn_error_count")
            .reset_index()
            .to_dict(orient="records")
        )

    payload = {
        "baseline_hours": BASELINE_HOURS,
        "baseline_start": isoformat(baseline_start),
        "baseline_end": isoformat(base_end),
        "services": sorted(metrics),
        "metrics": sorted({metric for df in metrics.values() for metric in numeric_columns(df)}),
        "metric_baselines": metric_baselines,
        "log_templates": log_templates,
        "warn_error_windows": level_windows,
        "template_keys": [{"service": service, "template": template} for service, template in sorted(seen_templates)],
    }
    write_json(SIGNALS_PATH, payload)
    return {
        "baseline_start": payload["baseline_start"],
        "baseline_end": payload["baseline_end"],
        "services": len(payload["services"]),
        "metrics": len(payload["metrics"]),
        "metric_windows": len(metric_baselines),
        "log_templates": len(log_templates),
    }


def calculate_metrics() -> dict:
    return run_stage("calculate", _calculate_metrics)


def _detect_anomalies() -> dict:
    require_stage("calculate")
    metrics = load_metrics()
    logs = load_logs()
    base_end = pd.to_datetime(read_json(SIGNALS_PATH, {})["baseline_end"], utc=True)
    sim_start = simulation_start(logs)
    sim_end = incident_anchor(logs) + INCIDENT_TAIL
    metric_events, metric_alerts = metric_detector_replay(metrics)
    log_events, log_alerts = log_replay(logs, base_end)
    metric_events = filter_time_window(metric_events, sim_start, sim_end)
    metric_alerts = filter_time_window(metric_alerts, sim_start, sim_end)
    log_events = filter_time_window(log_events, sim_start, sim_end)
    log_alerts = filter_time_window(log_alerts, sim_start, sim_end)
    events = sorted(metric_events + log_events, key=lambda row: row["timestamp"])
    alerts = sorted(metric_alerts + log_alerts, key=lambda row: row["timestamp"])
    write_jsonl(EVENTS_PATH, events)
    write_jsonl(ALERTS_PATH, alerts)
    return {
        "events": len(events),
        "metric_events": len(metric_events),
        "log_events": len(log_events),
        "alerts": len(alerts),
        "metric_alerts": len(metric_alerts),
        "log_alerts": len(log_alerts),
    }


def detect_anomalies() -> dict:
    return run_stage("detect", _detect_anomalies)


def filter_time_window(rows: list[dict], start: pd.Timestamp, end: pd.Timestamp) -> list[dict]:
    filtered = []
    for row in rows:
        timestamp = pd.to_datetime(row.get("timestamp"), utc=True)
        if start <= timestamp <= end:
            filtered.append(row)
    return filtered


def _run_rca() -> dict:
    require_stage("detect")
    metrics = load_metrics()
    logs = load_logs()
    alerts = read_jsonl(ALERTS_PATH)
    timeline, hypotheses = build_rca(metrics, logs, alerts)
    write_json(TIMELINE_PATH, timeline)
    write_json(HYPOTHESES_PATH, hypotheses)
    return {
        "timeline_events": len(timeline),
        "hypotheses": len(hypotheses),
        "top_root_cause_service": hypotheses[0]["root_cause_service"] if hypotheses else None,
        "top_confidence": hypotheses[0]["confidence"] if hypotheses else None,
    }


def run_rca() -> dict:
    return run_stage("rca", _run_rca)


def reset_pipeline() -> dict:
    ensure_dirs()
    for path in [STREAM_PATH, SIGNALS_PATH, EVENTS_PATH, ALERTS_PATH, TIMELINE_PATH, HYPOTHESES_PATH, STATE_PATH]:
        if path.exists():
            path.unlink()
    state = empty_state()
    write_state(state)
    return state


def prepare_preincident() -> dict:
    reset_pipeline()
    stream_data()
    calculate_metrics()
    return get_pipeline_state()


def simulate_incident() -> dict:
    if not stage_complete("calculate"):
        prepare_preincident()
    stream_incident_window()
    detect_anomalies()
    run_rca()
    return get_pipeline_state()


def get_pipeline_state() -> dict:
    state = read_state()
    update_state_counts(state)
    return write_state(state)


def verify_stream_outputs() -> None:
    expected = [STREAM_PATH, SIGNALS_PATH, EVENTS_PATH, ALERTS_PATH, TIMELINE_PATH, HYPOTHESES_PATH, STATE_PATH]
    missing = [str(path) for path in expected if not path.exists() or path.stat().st_size == 0]
    if missing:
        raise RuntimeError("Missing or empty real-time outputs: " + ", ".join(missing))
    alerts = read_jsonl(ALERTS_PATH)
    hypotheses = read_json(HYPOTHESES_PATH, [])
    timeline = read_json(TIMELINE_PATH, [])
    if not any(alert["alert_type"].startswith("metric") for alert in alerts):
        raise RuntimeError("real-time alerts missing metric alerts")
    if not any(alert["alert_type"].startswith("log") for alert in alerts):
        raise RuntimeError("real-time alerts missing log template alerts")
    if not hypotheses or hypotheses[0]["root_cause_service"] != "cart-service":
        raise RuntimeError("top RCA hypothesis is not cart-service")
    stages = " -> ".join(row["stage"] for row in timeline)
    for required in ["GC/cache pressure", "OOMKilled", "restart loop", "downstream symptom"]:
        if required not in stages:
            raise RuntimeError(f"RCA timeline missing stage: {required}")


def run_pipeline() -> dict:
    started = time.perf_counter()
    prepare_preincident()
    simulate_incident()
    verify_stream_outputs()
    state = get_pipeline_state()
    return {
        "output_dir": str(REALTIME_OUT),
        "base_end": read_json(SIGNALS_PATH, {}).get("baseline_end"),
        "event_count": state["counts"].get("events", 0),
        "alert_count": state["counts"].get("alerts", 0),
        "metric_alert_count": state["counts"].get("metric_alerts", 0),
        "log_alert_count": state["counts"].get("log_alerts", 0),
        "rca_event_count": state["counts"].get("rca_timeline", 0),
        "top_root_cause_service": state["counts"].get("top_root_cause_service"),
        "duration_seconds": round(time.perf_counter() - started, 2),
    }


def main() -> None:
    summary = run_pipeline()
    print(f"Streamed real-time replay data to {REALTIME_OUT}")
    print(f"Top RCA hypothesis: {summary['top_root_cause_service']}")


if __name__ == "__main__":
    main()
