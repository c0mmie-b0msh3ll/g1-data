from __future__ import annotations

import json
import mimetypes
import re
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from analyze import BASELINE_HOURS, baseline_end, load_logs, load_metrics, numeric_columns
from realtime import (
    ALERTS_PATH,
    EVENTS_PATH,
    HYPOTHESES_PATH,
    REALTIME_OUT,
    SIGNALS_PATH,
    TIMELINE_PATH,
    build_rca,
    filter_time_window,
    incident_anchor,
    isoformat,
    log_replay,
    metric_detector_replay,
    prepare_preincident,
    read_json,
    simulation_start,
    write_json,
    write_jsonl,
)


HOST = "127.0.0.1"
PORT = 8765
DEFAULT_SPEED = 60
SPEEDS = {30, 60, 120, 240, 480}
POLL_WINDOW = 30
STATIC_DIR = REALTIME_OUT / "static"
PLOTLY_PATH = STATIC_DIR / "plotly.min.js"

PANELS = [
    {
        "id": "cart_memory",
        "title": "cart memory usage",
        "service": "cart-service",
        "metric": "memory_usage_bytes",
        "unit": "bytes",
        "color": "#ff7b72",
    },
    {
        "id": "cart_gc",
        "title": "cart GC pause",
        "service": "cart-service",
        "metric": "jvm_gc_pause_ms_avg",
        "unit": "ms",
        "color": "#f2cc60",
    },
    {
        "id": "cart_latency",
        "title": "cart p99 latency",
        "service": "cart-service",
        "metric": "http_p99_latency_ms",
        "unit": "ms",
        "color": "#79c0ff",
    },
    {
        "id": "cart_5xx",
        "title": "cart 5xx rate",
        "service": "cart-service",
        "metric": "http_5xx_rate",
        "unit": "%",
        "color": "#d2a8ff",
    },
    {
        "id": "cart_restarts",
        "title": "cart restart count",
        "service": "cart-service",
        "metric": "container_restart_count",
        "unit": "count",
        "color": "#ffa657",
    },
    {
        "id": "gateway_cart_errors",
        "title": "api-gateway cart upstream errors",
        "service": "api-gateway",
        "metric": "cart_upstream_error_rate",
        "unit": "%",
        "color": "#56d364",
    },
    {
        "id": "order_timeouts",
        "title": "order upstream timeouts",
        "service": "order-service",
        "metric": "upstream_timeout_rate",
        "unit": "%",
        "color": "#39c5cf",
    },
    {
        "id": "payment_timeouts",
        "title": "payment upstream timeouts",
        "service": "payment-service",
        "metric": "upstream_timeout_rate",
        "unit": "%",
        "color": "#bc8cff",
    },
]


def _to_ts(value) -> pd.Timestamp:
    return pd.to_datetime(value, utc=True)


def _json_response(handler: BaseHTTPRequestHandler, payload: dict, status: int = 200) -> None:
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


def _read_body_json(handler: BaseHTTPRequestHandler) -> dict:
    length = int(handler.headers.get("Content-Length", "0") or 0)
    if length <= 0:
        return {}
    raw = handler.rfile.read(length).decode("utf-8")
    return json.loads(raw) if raw.strip() else {}


def _latest(rows: list[dict], limit: int = 30) -> list[dict]:
    return sorted(rows, key=lambda row: row.get("timestamp", ""), reverse=True)[:limit]


def _thresholds(signals: dict) -> dict[tuple[str, str], float]:
    out = {}
    for row in signals.get("metric_baselines", []):
        service = row.get("service")
        metric = row.get("metric")
        if service and metric:
            out[(service, metric)] = row.get("mad_threshold")
    return out


def _event_timestamp(row: dict) -> pd.Timestamp:
    return _to_ts(row["timestamp"])


def _filter_available(rows: list[dict], cursor: pd.Timestamp) -> list[dict]:
    return [row for row in rows if _event_timestamp(row) <= cursor]


def _rows_in_intervals(rows: list[dict], intervals: list[tuple[pd.Timestamp, pd.Timestamp]]) -> list[dict]:
    filtered = []
    for row in rows:
        ts = _event_timestamp(row)
        if any(start <= ts <= end for start, end in intervals):
            filtered.append(row)
    return filtered


def _build_curated_intervals(logs: pd.DataFrame) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    anchor = incident_anchor(logs)
    oom = logs.loc[logs["message"].str.contains("OOMKilled", case=False, na=False), "timestamp"].min()
    five_xx = logs.loc[logs["message"].str.contains("Cart service returned 5xx", case=False, na=False), "timestamp"].min()
    intervals = [
        (simulation_start(logs), anchor + pd.Timedelta(minutes=25)),
        (anchor + pd.Timedelta(minutes=50), anchor + pd.Timedelta(minutes=85)),
    ]
    if pd.notna(oom):
        intervals.append((oom - pd.Timedelta(minutes=10), oom + pd.Timedelta(minutes=20)))
    if pd.notna(five_xx):
        intervals.append((five_xx - pd.Timedelta(minutes=6), five_xx + pd.Timedelta(minutes=20)))
    intervals = sorted(intervals, key=lambda item: item[0])
    merged: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    for start, end in intervals:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _metric_rows(metrics: dict[str, pd.DataFrame], start: pd.Timestamp, end: pd.Timestamp) -> list[dict]:
    rows: list[dict] = []
    for service, df in metrics.items():
        subset = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)].copy()
        for _, row in subset.iterrows():
            rows.append(
                {
                    "timestamp": isoformat(row["timestamp"]),
                    "event_type": "metric_sample",
                    "service": service,
                    "metrics": {metric: row[metric] for metric in numeric_columns(df)},
                }
            )
    return sorted(rows, key=lambda row: row["timestamp"])


def _log_rows(logs: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> list[dict]:
    rows = []
    subset = logs[(logs["timestamp"] >= start) & (logs["timestamp"] <= end)].copy()
    for _, row in subset.iterrows():
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
    return sorted(rows, key=lambda row: row["timestamp"])


@dataclass
class ReplayContext:
    metrics: dict[str, pd.DataFrame]
    logs: pd.DataFrame
    signals: dict
    thresholds: dict[tuple[str, str], float]
    baseline_start: pd.Timestamp
    baseline_end: pd.Timestamp
    normal_start: pd.Timestamp
    normal_end: pd.Timestamp
    incident_start: pd.Timestamp
    incident_end: pd.Timestamp
    replay_intervals: list[tuple[pd.Timestamp, pd.Timestamp]]
    replay_duration_seconds: float
    normal_metric_rows: list[dict]
    incident_metric_rows: list[dict]
    incident_log_rows: list[dict]
    events: list[dict]
    alerts: list[dict]
    timeline: list[dict]
    hypotheses: list[dict]
    chart_ranges: dict[str, list[float]]


CONTEXT_CACHE: ReplayContext | None = None


def build_context() -> ReplayContext:
    global CONTEXT_CACHE
    if CONTEXT_CACHE is not None:
        return CONTEXT_CACHE
    prepare_preincident()
    metrics = load_metrics()
    logs = load_logs()
    base_end = baseline_end(metrics)
    base_start = min(df["timestamp"].min() for df in metrics.values())
    sim_start = simulation_start(logs)
    anchor = incident_anchor(logs)
    replay_intervals = _build_curated_intervals(logs)
    sim_end = replay_intervals[-1][1]
    replay_duration = sum((end - start).total_seconds() for start, end in replay_intervals)
    normal_start = max(base_start, sim_start - pd.Timedelta(hours=1))
    normal_end = sim_start - pd.Timedelta(seconds=30)
    signals = read_json(SIGNALS_PATH, {})
    thresholds = _thresholds(signals)
    interval_logs = logs[
        (logs["timestamp"] < base_end)
        | pd.concat(
            [
                (logs["timestamp"] >= start) & (logs["timestamp"] <= end)
                for start, end in replay_intervals
            ],
            axis=1,
        ).any(axis=1)
    ].copy()

    metric_events, metric_alerts = metric_detector_replay(metrics)
    log_events, log_alerts = log_replay(interval_logs, base_end)
    events = sorted(_rows_in_intervals(metric_events + log_events, replay_intervals), key=lambda row: row["timestamp"])
    alerts = sorted(_rows_in_intervals(metric_alerts + log_alerts, replay_intervals), key=lambda row: row["timestamp"])
    timeline, hypotheses = build_rca(metrics, logs, metric_alerts + log_alerts)
    timeline = sorted(_rows_in_intervals(timeline, replay_intervals), key=lambda row: row["timestamp"])
    write_jsonl(EVENTS_PATH, events)
    write_jsonl(ALERTS_PATH, alerts)
    write_json(TIMELINE_PATH, timeline)
    write_json(HYPOTHESES_PATH, hypotheses)

    incident_metric_rows = [
        row
        for start, end in replay_intervals
        for row in _metric_rows(metrics, start, end)
    ]
    normal_metric_rows = _metric_rows(metrics, normal_start, normal_end)
    chart_ranges: dict[str, list[float]] = {}
    for panel in PANELS:
        values = [
            float(row["metrics"][panel["metric"]])
            for row in normal_metric_rows + incident_metric_rows
            if row.get("service") == panel["service"]
            and row.get("metrics", {}).get(panel["metric"]) is not None
        ]
        threshold = thresholds.get((panel["service"], panel["metric"]))
        if threshold is not None:
            values.append(float(threshold))
        high = max(values) if values else 1.0
        chart_ranges[panel["id"]] = [0, high * 1.12 if high else 1.0]

    CONTEXT_CACHE = ReplayContext(
        metrics=metrics,
        logs=logs,
        signals=signals,
        thresholds=thresholds,
        baseline_start=base_start,
        baseline_end=base_end,
        normal_start=normal_start,
        normal_end=normal_end,
        incident_start=sim_start,
        incident_end=sim_end,
        replay_intervals=replay_intervals,
        replay_duration_seconds=replay_duration,
        normal_metric_rows=normal_metric_rows,
        incident_metric_rows=incident_metric_rows,
        incident_log_rows=[
            row
            for start, end in replay_intervals
            for row in _log_rows(logs, start, end)
        ],
        events=events,
        alerts=alerts,
        timeline=timeline,
        hypotheses=hypotheses,
        chart_ranges=chart_ranges,
    )
    return CONTEXT_CACHE


@dataclass
class SimulationSession:
    context: ReplayContext = field(default_factory=build_context)
    status: str = "normal"
    speed: int = DEFAULT_SPEED
    cursor: pd.Timestamp = field(init=False)
    started_wall: float = field(default_factory=time.monotonic)
    cursor_started_at: pd.Timestamp = field(init=False)
    replay_offset_started: float = 0.0
    replay_offset: float = 0.0
    failure: str | None = None

    def __post_init__(self) -> None:
        self.cursor = self.context.normal_start
        self.cursor_started_at = self.cursor

    def timestamp_at_replay_offset(self, offset_seconds: float) -> pd.Timestamp:
        remaining = max(offset_seconds, 0.0)
        for start, end in self.context.replay_intervals:
            duration = (end - start).total_seconds()
            if remaining <= duration:
                return start + pd.Timedelta(seconds=remaining)
            remaining -= duration
        return self.context.replay_intervals[-1][1]

    def replay_offset_for_timestamp(self, timestamp: pd.Timestamp) -> float | None:
        elapsed = 0.0
        for start, end in self.context.replay_intervals:
            if start <= timestamp <= end:
                return elapsed + (timestamp - start).total_seconds()
            elapsed += (end - start).total_seconds()
        return None

    def available_by_replay_offset(self, rows: list[dict]) -> list[dict]:
        if self.status == "normal":
            return []
        out = []
        for row in rows:
            offset = self.replay_offset_for_timestamp(_event_timestamp(row))
            if offset is not None and offset <= self.replay_offset:
                out.append(row)
        return out

    def reset(self) -> None:
        self.context = build_context()
        self.status = "normal"
        self.speed = DEFAULT_SPEED
        self.cursor = self.context.normal_start
        self.cursor_started_at = self.cursor
        self.replay_offset = 0.0
        self.replay_offset_started = 0.0
        self.started_wall = time.monotonic()
        self.failure = None

    def simulate(self) -> None:
        self.status = "running"
        self.cursor = self.context.incident_start
        self.cursor_started_at = self.cursor
        self.replay_offset = 0.0
        self.replay_offset_started = 0.0
        self.started_wall = time.monotonic()
        self.failure = None

    def pause(self) -> None:
        self.tick()
        if self.status == "running":
            self.status = "paused"

    def resume(self) -> None:
        if self.status == "paused":
            self.status = "running"
            self.cursor_started_at = self.cursor
            self.replay_offset_started = self.replay_offset
            self.started_wall = time.monotonic()

    def set_speed(self, speed: int) -> None:
        if speed not in SPEEDS:
            raise ValueError("speed must be one of 30, 60, 120, 240, or 480")
        self.tick()
        self.speed = speed
        self.cursor_started_at = self.cursor
        self.replay_offset_started = self.replay_offset
        self.started_wall = time.monotonic()

    def tick(self) -> None:
        now = time.monotonic()
        elapsed = now - self.started_wall
        delta = pd.Timedelta(seconds=elapsed * self.speed)
        if self.status == "normal":
            span = (self.context.normal_end - self.context.normal_start).total_seconds()
            offset = (delta.total_seconds() % max(span, 1))
            self.cursor = self.context.normal_start + pd.Timedelta(seconds=offset)
        elif self.status == "running":
            self.replay_offset = min(
                self.replay_offset_started + delta.total_seconds(),
                self.context.replay_duration_seconds,
            )
            self.cursor = self.timestamp_at_replay_offset(self.replay_offset)
            if self.replay_offset >= self.context.replay_duration_seconds:
                self.status = "complete"

    def visible_metric_rows(self) -> list[dict]:
        self.tick()
        if self.status == "normal":
            rows = [row for row in self.context.normal_metric_rows if _event_timestamp(row) <= self.cursor]
            if len(rows) < POLL_WINDOW:
                rows = self.context.normal_metric_rows[-POLL_WINDOW:] + rows
            return rows[-POLL_WINDOW * len(PANELS) :]
        rows = self.available_by_replay_offset(self.context.incident_metric_rows)
        if len(rows) < POLL_WINDOW:
            lead = self.context.incident_metric_rows[: POLL_WINDOW * len(PANELS)]
            rows = lead[: POLL_WINDOW * len(PANELS)]
        return rows[-POLL_WINDOW * len(PANELS) :]

    def visible_logs(self) -> list[dict]:
        if self.status == "normal":
            return []
        return _latest(self.available_by_replay_offset(self.context.incident_log_rows), 12)

    def available_alerts(self) -> list[dict]:
        if self.status == "normal":
            return []
        return self.available_by_replay_offset(self.context.alerts)

    def available_events(self) -> list[dict]:
        if self.status == "normal":
            return []
        return self.available_by_replay_offset(self.context.events)

    def rca_state(self, alerts: list[dict]) -> dict:
        if self.status == "normal":
            return {"status": "none", "confidence": None, "hypothesis": None, "timeline": []}
        cart_pressure = any(
            row.get("service") == "cart-service"
            and re.search(r"memory_usage_bytes|jvm_gc_pause_ms_avg|http_p99_latency_ms|http_5xx_rate|GC overhead|cache", row.get("metric", "") + " " + row.get("summary", ""), re.I)
            for row in alerts
        )
        restart = any(
            row.get("service") == "cart-service"
            and re.search(r"container_restart_count|restart", row.get("metric", "") + " " + row.get("summary", ""), re.I)
            for row in alerts
        )
        downstream = any(row.get("service") in {"api-gateway", "order-service", "payment-service"} for row in alerts)
        available_timeline = self.available_by_replay_offset(self.context.timeline)
        top = self.context.hypotheses[0] if self.context.hypotheses else None
        if cart_pressure and restart and downstream and top:
            return {"status": "high", "confidence": "high", "hypothesis": top, "timeline": available_timeline}
        if cart_pressure:
            return {
                "status": "tentative",
                "confidence": "medium",
                "hypothesis": {
                    "root_cause_service": "cart-service",
                    "root_cause": "cart-service is the leading candidate, pending OOM/restart and downstream propagation evidence",
                    "confidence": "medium",
                },
                "timeline": available_timeline,
            }
        return {"status": "none", "confidence": None, "hypothesis": None, "timeline": []}

    def chart_series(self, rows: list[dict]) -> dict[str, dict]:
        charts: dict[str, dict] = {}
        for panel in PANELS:
            points = [
                row
                for row in rows
                if row.get("service") == panel["service"]
                and row.get("metrics", {}).get(panel["metric"]) is not None
            ][-POLL_WINDOW:]
            threshold = self.context.thresholds.get((panel["service"], panel["metric"]))
            charts[panel["id"]] = {
                **panel,
                "threshold": threshold,
                "x": list(range(1, len(points) + 1)),
                "y": [float(row["metrics"][panel["metric"]]) for row in points],
                "timestamps": [row["timestamp"] for row in points],
                "y_range": self.context.chart_ranges.get(panel["id"], [0, 1]),
                "service": panel["service"],
                "metric": panel["metric"],
            }
        return charts

    def state(self) -> dict:
        rows = self.visible_metric_rows()
        alerts = self.available_alerts()
        events = self.available_events()
        rca = self.rca_state(alerts)
        return {
            "session": {
                "status": self.status,
                "speed": self.speed,
                "cursor": isoformat(self.cursor),
                "baseline_start": isoformat(self.context.baseline_start),
                "baseline_end": isoformat(self.context.baseline_end),
                "incident_start": isoformat(self.context.incident_start),
                "incident_end": isoformat(self.context.incident_end),
                "replay_elapsed_seconds": round(self.replay_offset, 1),
                "replay_duration_seconds": round(self.context.replay_duration_seconds, 1),
                "failure": self.failure,
            },
            "counts": {
                "metric_samples_visible": len(rows),
                "events_visible": len(events),
                "alerts_visible": len(alerts),
            },
            "charts": self.chart_series(rows),
            "active_alerts": _latest(alerts, 30),
            "recent_events": _latest(events, 30),
            "recent_logs": self.visible_logs(),
            "rca": rca,
        }


SESSION_LOCK = threading.RLock()
SESSION = SimulationSession()


def dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CloudWatch-Style Incident Dashboard</title>
<script src="/static/plotly.min.js"></script>
<style>
:root { --bg:#0b1018; --panel:#111827; --panel2:#0f1724; --line:#263244; --ink:#e6edf3; --muted:#8b949e; --blue:#58a6ff; --green:#3fb950; --yellow:#d29922; --red:#f85149; }
* { box-sizing:border-box; }
body { margin:0; background:var(--bg); color:var(--ink); font-family:Segoe UI, Arial, sans-serif; }
header { display:flex; justify-content:space-between; gap:16px; align-items:center; padding:14px 18px; border-bottom:1px solid var(--line); background:#090e16; position:sticky; top:0; z-index:2; }
h1 { margin:0; font-size:18px; font-weight:700; }
.sub { margin-top:4px; color:var(--muted); font-size:12px; }
.controls { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
button, select { border:1px solid var(--line); border-radius:6px; background:#1f6feb; color:white; padding:8px 10px; font-weight:700; cursor:pointer; }
button.secondary, select { background:#161b22; color:var(--ink); }
button:disabled { opacity:.55; cursor:wait; }
main { padding:14px; display:grid; gap:12px; }
.top { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:10px; }
.grid { display:grid; grid-template-columns:repeat(4,minmax(260px,1fr)); gap:10px; }
.lower { display:grid; grid-template-columns:1fr 1fr; gap:10px; }
.panel { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:10px; min-width:0; }
.k { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.04em; }
.v { margin-top:6px; font-size:20px; font-weight:800; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.chart { height:205px; }
.panel h2 { margin:0 0 8px; font-size:13px; color:var(--muted); text-transform:uppercase; letter-spacing:.04em; }
.panel-title { display:flex; justify-content:space-between; gap:8px; align-items:baseline; }
.metric-name { color:var(--muted); font-size:11px; }
table { width:100%; border-collapse:collapse; table-layout:fixed; font-size:12px; }
th,td { padding:7px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; overflow-wrap:anywhere; }
th { color:var(--muted); font-weight:700; }
.empty { color:var(--muted); padding:8px 0; }
.badge { display:inline-block; border-radius:999px; padding:3px 8px; font-size:11px; font-weight:800; background:#30363d; color:var(--muted); text-transform:uppercase; }
.badge.normal { color:var(--green); background:#12351f; }
.badge.running { color:var(--blue); background:#102b45; }
.badge.paused { color:var(--yellow); background:#3b2d0f; }
.badge.complete,.badge.high { color:var(--green); background:#12351f; }
.badge.tentative { color:var(--yellow); background:#3b2d0f; }
.badge.none { color:var(--muted); }
.timeline { display:grid; gap:8px; }
.step { border-left:3px solid var(--blue); background:var(--panel2); padding:8px; border-radius:5px; }
.step strong { display:block; }
.step span { color:var(--muted); font-size:11px; }
@media (max-width:1100px) { .grid { grid-template-columns:repeat(2,minmax(0,1fr)); } .top,.lower { grid-template-columns:1fr 1fr; } }
@media (max-width:680px) { header { display:block; } .controls { margin-top:10px; } .grid,.top,.lower { grid-template-columns:1fr; } }
</style>
</head>
<body>
<header>
  <div>
    <h1>Live Incident Dashboard</h1>
    <div class="sub">Pre-incident metrics move continuously. Incident replay reveals detector evidence progressively from the first 6-hour baseline.</div>
  </div>
  <div class="controls">
    <button id="simulate">Simulate Incident</button>
    <button id="pause" class="secondary">Pause</button>
    <button id="reset" class="secondary">Reset</button>
    <select id="speed" title="Replay speed"><option value="30">30x</option><option value="60" selected>60x</option><option value="120">120x</option><option value="240">240x</option><option value="480">480x</option></select>
  </div>
</header>
<main>
  <section class="top">
    <div class="panel"><div class="k">Session</div><div id="status" class="v"><span class="badge">loading</span></div></div>
    <div class="panel"><div class="k">Cursor</div><div id="cursor" class="v">...</div></div>
    <div class="panel"><div class="k">Alerts</div><div id="alertCount" class="v">0</div></div>
    <div class="panel"><div class="k">RCA</div><div id="rcaSummary" class="v">No evidence</div></div>
  </section>
  <section id="charts" class="grid"></section>
  <section class="lower">
    <div class="panel"><h2>Active Alerts</h2><div id="alerts" class="empty">No incident alerts.</div></div>
    <div class="panel"><h2>RCA Evidence Gate</h2><div id="rca" class="empty">RCA is empty during normal mode.</div></div>
  </section>
  <section class="lower">
    <div class="panel"><h2>Recent Detector Events</h2><div id="events" class="empty">No detector events.</div></div>
    <div class="panel"><h2>Recent Incident Logs</h2><div id="logs" class="empty">No incident logs.</div></div>
  </section>
</main>
<script>
const chartRoot = document.getElementById("charts");
let chartIds = new Set();
let latestStatus = "normal";
let stateLoading = false;
const DEFAULT_CHARTS = {
  cart_memory: { title: "cart memory usage", service: "cart-service", metric: "memory_usage_bytes" },
  cart_gc: { title: "cart GC pause", service: "cart-service", metric: "jvm_gc_pause_ms_avg" },
  cart_latency: { title: "cart p99 latency", service: "cart-service", metric: "http_p99_latency_ms" },
  cart_5xx: { title: "cart 5xx rate", service: "cart-service", metric: "http_5xx_rate" },
  cart_restarts: { title: "cart restart count", service: "cart-service", metric: "container_restart_count" },
  gateway_cart_errors: { title: "api-gateway cart upstream errors", service: "api-gateway", metric: "cart_upstream_error_rate" },
  order_timeouts: { title: "order upstream timeouts", service: "order-service", metric: "upstream_timeout_rate" },
  payment_timeouts: { title: "payment upstream timeouts", service: "payment-service", metric: "upstream_timeout_rate" }
};

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, ch => ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" }[ch]));
}
function table(rows, cols) {
  if (!rows || !rows.length) return '<div class="empty">No rows.</div>';
  return `<table><thead><tr>${cols.map(c => `<th>${esc(c)}</th>`).join("")}</tr></thead><tbody>` +
    rows.map(r => `<tr>${cols.map(c => `<td>${esc(typeof r[c] === "object" ? JSON.stringify(r[c]) : r[c])}</td>`).join("")}</tr>`).join("") +
    "</tbody></table>";
}
function ensureCharts(charts) {
  for (const [id, cfg] of Object.entries(charts)) {
    if (chartIds.has(id)) continue;
    chartIds.add(id);
    chartRoot.insertAdjacentHTML("beforeend", `<div class="panel"><div class="panel-title"><h2>${esc(cfg.title)}</h2><span class="metric-name">${esc(cfg.service)}/${esc(cfg.metric)}</span></div><div id="${esc(id)}" class="chart"><div class="empty">Waiting for samples...</div></div></div>`);
  }
}
function drawChart(id, cfg) {
  if (!cfg.x || !cfg.x.length) return;
  const hover = (cfg.x || []).map((x, i) => `${cfg.service}<br>${cfg.metric}<br>${cfg.timestamps?.[i] || ""}<br>value: ${cfg.y[i]} ${cfg.unit}<br>threshold: ${cfg.threshold ?? "n/a"}`);
  const traces = [{ x: cfg.x, y: cfg.y, type: "scatter", mode: "lines", line: { color: cfg.color, width: 2 }, text: hover, hovertemplate: "%{text}<extra></extra>" }];
  if (cfg.threshold != null && cfg.x && cfg.x.length) {
    traces.push({ x: cfg.x, y: cfg.x.map(() => cfg.threshold), type: "scatter", mode: "lines", line: { color: "#f85149", width: 1, dash: "dot" }, hoverinfo: "skip", showlegend: false });
  }
  const key = `${cfg.x?.[0] || ""}|${cfg.x?.[cfg.x.length - 1] || ""}|${cfg.y?.[cfg.y.length - 1] ?? ""}|${cfg.threshold ?? ""}`;
  const el = document.getElementById(id);
  if (el.dataset.renderKey === key) return;
  el.dataset.renderKey = key;
  Plotly.react(id, traces, {
    margin: { l: 44, r: 10, t: 8, b: 35 },
    paper_bgcolor: "#111827",
    plot_bgcolor: "#0f1724",
    font: { color: "#8b949e", size: 10 },
    xaxis: { gridcolor: "#263244", title: { text: "recent samples", font: { size: 10 } }, showticklabels: false, fixedrange: true },
    yaxis: { gridcolor: "#263244", range: cfg.y_range, fixedrange: true },
    showlegend: false,
    uirevision: "incident-dashboard"
  }, { responsive: true, displayModeBar: false });
}
function renderRca(rca) {
  const top = rca.hypothesis;
  document.getElementById("rcaSummary").innerHTML = `<span class="badge ${esc(rca.status)}">${esc(rca.status)}</span> ${esc(top?.root_cause_service || "")}`;
  if (rca.status === "none") {
    document.getElementById("rca").innerHTML = '<div class="empty">RCA is empty until cart pressure and detector evidence arrive.</div>';
    return;
  }
  const timeline = (rca.timeline || []).map(row => `<div class="step"><strong>${esc(row.stage)} - ${esc(row.service)}</strong><span>${esc(row.timestamp)}</span><div>${esc(row.evidence)}</div></div>`).join("");
  document.getElementById("rca").innerHTML = `<div><span class="badge ${esc(rca.status)}">${esc(rca.confidence)}</span> ${esc(top.root_cause)}</div><div class="timeline" style="margin-top:10px">${timeline || '<div class="empty">Waiting for correlated timeline evidence.</div>'}</div>`;
}
function render(data) {
  latestStatus = data.session.status;
  document.getElementById("status").innerHTML = `<span class="badge ${esc(data.session.status)}">${esc(data.session.status)}</span> ${esc(data.session.speed)}x`;
  document.getElementById("cursor").textContent = data.session.cursor;
  document.getElementById("alertCount").textContent = data.counts.alerts_visible;
  document.getElementById("speed").value = String(data.session.speed);
  document.getElementById("pause").textContent = data.session.status === "paused" ? "Resume" : "Pause";
  ensureCharts(data.charts);
  for (const [id, cfg] of Object.entries(data.charts)) drawChart(id, cfg);
  document.getElementById("alerts").innerHTML = table(data.active_alerts, ["timestamp", "alert_type", "service", "metric", "severity", "summary"]);
  document.getElementById("events").innerHTML = table(data.recent_events, ["timestamp", "event_type", "service", "metric", "value", "threshold"]);
  document.getElementById("logs").innerHTML = table(data.recent_logs, ["timestamp", "service", "level", "message"]);
  renderRca(data.rca);
}
async function api(path, body) {
  const res = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || "request failed");
  render(data);
}
async function load() {
  if (stateLoading) return;
  stateLoading = true;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const res = await fetch("/api/state", { signal: controller.signal });
    render(await res.json());
  } catch (err) {
    document.getElementById("status").innerHTML = '<span class="badge failed">state delayed</span>';
  } finally {
    clearTimeout(timeout);
    stateLoading = false;
  }
}
document.getElementById("simulate").onclick = () => api("/api/simulate");
document.getElementById("reset").onclick = () => api("/api/reset");
document.getElementById("pause").onclick = () => api(latestStatus === "paused" ? "/api/resume" : "/api/pause");
document.getElementById("speed").onchange = e => api("/api/speed", { speed: Number(e.target.value) });
ensureCharts(DEFAULT_CHARTS);
load();
setInterval(load, 2000);
</script>
</body>
</html>"""


def write_static_snapshot() -> None:
    REALTIME_OUT.mkdir(parents=True, exist_ok=True)
    (REALTIME_OUT / "dashboard.html").write_text(dashboard_html(), encoding="utf-8")


class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args) -> None:
        return

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path in {"/", "/dashboard.html"}:
            raw = dashboard_html().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return
        if path == "/api/state":
            with SESSION_LOCK:
                payload = SESSION.state()
            _json_response(self, payload)
            return
        if path == "/static/plotly.min.js":
            if not PLOTLY_PATH.exists():
                self.send_error(404, "plotly.min.js is missing")
                return
            raw = PLOTLY_PATH.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", mimetypes.guess_type(PLOTLY_PATH.name)[0] or "application/javascript")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return
        self.send_error(404)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        try:
            with SESSION_LOCK:
                if path == "/api/simulate":
                    SESSION.simulate()
                elif path == "/api/pause":
                    SESSION.pause()
                elif path == "/api/resume":
                    SESSION.resume()
                elif path == "/api/reset":
                    SESSION.reset()
                elif path == "/api/speed":
                    SESSION.set_speed(int(_read_body_json(self).get("speed")))
                else:
                    self.send_error(404)
                    return
                payload = SESSION.state()
            _json_response(self, payload)
        except Exception as exc:
            _json_response(self, {"error": str(exc)}, 400)


def verify_dashboard_snapshot() -> None:
    write_static_snapshot()
    dashboard = (REALTIME_OUT / "dashboard.html").read_text(encoding="utf-8")
    for marker in ["Simulate Incident", "Pause", "Reset", "/api/state", "/static/plotly.min.js", "RCA Evidence Gate"]:
        if marker not in dashboard:
            raise RuntimeError(f"dashboard missing marker: {marker}")
    if re.search(r"https?://", dashboard, flags=re.I):
        raise RuntimeError("real-time dashboard contains an external URL")
    if not PLOTLY_PATH.exists() or PLOTLY_PATH.stat().st_size < 100_000:
        raise RuntimeError(f"missing vendored Plotly asset: {PLOTLY_PATH}")


def main() -> None:
    verify_dashboard_snapshot()
    server = ThreadingHTTPServer((HOST, PORT), DashboardHandler)
    print(f"Dashboard server: http://{HOST}:{PORT}")
    print("Open the dashboard and click 'Simulate Incident' to replay live detector evidence.")
    server.serve_forever()


if __name__ == "__main__":
    main()
