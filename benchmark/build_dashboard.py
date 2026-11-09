#!/usr/bin/env python3
"""Reads results.db and writes a self-contained dashboard.html.

The output file embeds all data as JSON and pulls Chart.js from a CDN so
it opens with a double-click. No server required.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bedrock_batch.storage import all_events, all_fallbacks, all_jobs

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "results.db"
OUT_PATH = PROJECT_ROOT / "dashboard.html"


def summarize(jobs: list[dict], fallbacks: list[dict], cfg: dict) -> dict:
    """Flatten jobs + per-run computed fields the dashboard needs."""
    pricing = cfg["pricing"]
    ond_in = pricing["ondemand_input_per_1m"]
    ond_out = pricing["ondemand_output_per_1m"]
    discount = pricing["batch_discount"]

    enriched = []
    for j in jobs:
        duration = None
        if j.get("completed_at") and j.get("submitted_at"):
            duration = j["completed_at"] - j["submitted_at"]
        exec_duration = None
        if j.get("completed_at") and j.get("started_at"):
            exec_duration = j["completed_at"] - j["started_at"]

        succeeded = j.get("records_succeeded") or 0
        throughput = (succeeded / exec_duration) if exec_duration and exec_duration > 0 else None

        in_tok = j.get("input_tokens") or 0
        out_tok = j.get("output_tokens") or 0
        batch_cost = (in_tok * ond_in + out_tok * ond_out) / 1_000_000 * (1 - discount)
        ondemand_cost = (in_tok * ond_in + out_tok * ond_out) / 1_000_000
        cost_per_1k = (batch_cost / succeeded * 1000) if succeeded else None

        enriched.append({
            **j,
            "duration_seconds": duration,
            "exec_duration_seconds": exec_duration,
            "throughput_rps": throughput,
            "batch_cost_usd": batch_cost,
            "ondemand_cost_usd": ondemand_cost,
            "cost_per_1k_usd": cost_per_1k,
        })

    # Recommendation: among jobs that finished within the SLA with zero
    # failures, pick the one with the highest throughput. Batch pricing is
    # per-token so $/1k is identical across sizes on the same workload —
    # the real optimization dimension is throughput (fewer jobs to manage,
    # lower end-to-end wall-clock for a fixed total corpus).
    sla = cfg["timeout_hours"] * 3600
    successful = [
        j for j in enriched
        if j["status"] == "Completed"
        and (j.get("records_failed") or 0) == 0
        and j.get("duration_seconds") is not None
        and j["duration_seconds"] <= sla
        and j.get("throughput_rps") is not None
    ]
    recommendation = None
    if successful:
        best = max(successful, key=lambda j: j["throughput_rps"])
        recommendation = {
            "region": best["region"],
            "batch_size": best["batch_size"],
            "cost_per_1k_usd": best["cost_per_1k_usd"],
            "duration_seconds": best["duration_seconds"],
            "throughput_rps": best["throughput_rps"],
        }

    return {
        "jobs": enriched,
        "fallbacks": fallbacks,
        "events": [{**e, "latency_seconds": (e["received_at"] - e["event_time"])} for e in all_events(DB_PATH)],
        "recommendation": recommendation,
        "config": {
            "model_id": cfg["model_id"],
            "timeout_hours": cfg["timeout_hours"],
            "regions": cfg["regions"],
            "batch_sizes": cfg["batch_sizes"],
            "workload": cfg["workload"],
            "pricing": cfg["pricing"],
        },
    }


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Bedrock Batch Benchmark</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0f172a;
    --panel: #1e293b;
    --panel-2: #273449;
    --fg: #e2e8f0;
    --muted: #94a3b8;
    --accent-east: #38bdf8;
    --accent-west: #fbbf24;
    --danger: #f87171;
    --ok: #4ade80;
    --border: #334155;
  }
  html, body {
    margin: 0; padding: 0;
    background: var(--bg); color: var(--fg);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  }
  header {
    padding: 24px 32px; border-bottom: 1px solid var(--border);
    display: flex; align-items: baseline; justify-content: space-between;
  }
  header h1 { margin: 0; font-size: 20px; font-weight: 600; }
  header .meta { color: var(--muted); font-size: 13px; }
  main { padding: 24px 32px; max-width: 1400px; margin: 0 auto; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
  .card {
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 10px; padding: 20px;
  }
  .card.full { grid-column: 1 / -1; }
  .card h2 {
    margin: 0 0 16px; font-size: 14px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted);
  }
  .recommend {
    background: linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%);
    border: none;
  }
  .recommend h2 { color: rgba(255,255,255,0.9); }
  .recommend .big { font-size: 32px; font-weight: 700; margin: 8px 0; }
  .recommend .sub { color: rgba(255,255,255,0.85); font-size: 14px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding: 8px 12px; text-align: right; border-bottom: 1px solid var(--border); }
  th:first-child, td:first-child { text-align: left; }
  th { color: var(--muted); font-weight: 600; text-transform: uppercase; font-size: 11px; letter-spacing: 0.05em; }
  tr:hover td { background: var(--panel-2); }
  .status { padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
  .status.ok { background: rgba(74, 222, 128, 0.15); color: var(--ok); }
  .status.bad { background: rgba(248, 113, 113, 0.15); color: var(--danger); }
  canvas { max-height: 320px; }
  .empty { color: var(--muted); font-style: italic; }
</style>
</head>
<body>
<header>
  <h1>Bedrock Batch Benchmark</h1>
  <div class="meta" id="meta"></div>
</header>
<main>
  <div class="grid">
    <div class="card recommend full" id="recommendation"></div>
    <div class="card"><h2>Throughput vs. batch size</h2><canvas id="throughput"></canvas></div>
    <div class="card"><h2>Wall-clock duration vs. batch size</h2><canvas id="duration"></canvas></div>
    <div class="card"><h2>Cost per 1k records (batch)</h2><canvas id="cost"></canvas></div>
    <div class="card"><h2>Batch vs. on-demand cost</h2><canvas id="costCompare"></canvas></div>
    <div class="card full"><h2>All runs</h2><div id="jobsTable"></div></div>
    <div class="card full"><h2>Fallback incidents (24h timeout)</h2><div id="fallbackTable"></div></div>
    <div class="card full"><h2>EventBridge state-change log</h2><div id="eventsTable"></div></div>
  </div>
</main>
<script>
const DATA = __DATA__;

function fmtDuration(s) {
  if (s == null) return '-';
  if (s < 60) return s.toFixed(1) + 's';
  if (s < 3600) return (s/60).toFixed(1) + 'm';
  return (s/3600).toFixed(2) + 'h';
}
function fmtMoney(n) { return n == null ? '-' : '$' + n.toFixed(4); }
function fmtNum(n, d=1) { return n == null ? '-' : n.toLocaleString(undefined, {maximumFractionDigits: d}); }

document.getElementById('meta').textContent =
  `${DATA.config.model_id} • regions: ${DATA.config.regions.join(', ')} • workload: ${DATA.config.workload} • timeoutDurationInHours=${DATA.config.timeout_hours}`;

const rec = DATA.recommendation;
const recCard = document.getElementById('recommendation');
if (rec) {
  recCard.innerHTML = `
    <h2>Recommendation</h2>
    <div class="big">${fmtNum(rec.batch_size, 0)} records / job in <span style="text-transform:uppercase">${rec.region}</span></div>
    <div class="sub">
      Highest throughput within the ${DATA.config.timeout_hours}h SLA: ${fmtNum(rec.throughput_rps, 1)} rec/s •
      Wall-clock ${fmtDuration(rec.duration_seconds)} •
      ${fmtMoney(rec.cost_per_1k_usd)} per 1k records
    </div>`;
} else {
  recCard.innerHTML = `<h2>Recommendation</h2><div class="empty">No successful runs within the SLA yet.</div>`;
}

const byRegion = {};
for (const r of DATA.config.regions) byRegion[r] = [];
for (const j of DATA.jobs) if (byRegion[j.region]) byRegion[j.region].push(j);
for (const r of DATA.config.regions) byRegion[r].sort((a,b) => a.batch_size - b.batch_size);

const colors = { 'us-east-1': '#38bdf8', 'us-west-2': '#fbbf24' };

function lineChart(canvasId, yField, yLabel) {
  const datasets = DATA.config.regions.map(r => ({
    label: r,
    data: byRegion[r].map(j => ({x: j.batch_size, y: j[yField]})),
    borderColor: colors[r] || '#a78bfa',
    backgroundColor: colors[r] || '#a78bfa',
    tension: 0.3,
    spanGaps: true,
  }));
  new Chart(document.getElementById(canvasId), {
    type: 'line',
    data: { datasets },
    options: {
      responsive: true,
      scales: {
        x: { type: 'logarithmic', title: { display: true, text: 'Batch size (records)', color: '#94a3b8' }, ticks: { color: '#94a3b8' }, grid: { color: '#334155' } },
        y: { title: { display: true, text: yLabel, color: '#94a3b8' }, ticks: { color: '#94a3b8' }, grid: { color: '#334155' } },
      },
      plugins: { legend: { labels: { color: '#e2e8f0' } } },
    },
  });
}

lineChart('throughput', 'throughput_rps', 'Records / second');
lineChart('duration', 'duration_seconds', 'Wall-clock seconds');
lineChart('cost', 'cost_per_1k_usd', 'USD per 1k records');

// Batch vs on-demand side by side (bar) for successful jobs only
const sizes = [...new Set(DATA.jobs.map(j => j.batch_size))].sort((a,b) => a - b);
const barDatasets = [];
for (const r of DATA.config.regions) {
  barDatasets.push({
    label: `${r} • batch`,
    backgroundColor: colors[r],
    data: sizes.map(s => {
      const j = byRegion[r].find(x => x.batch_size === s);
      return j ? j.batch_cost_usd : null;
    }),
  });
  barDatasets.push({
    label: `${r} • on-demand`,
    backgroundColor: colors[r] + '66',
    data: sizes.map(s => {
      const j = byRegion[r].find(x => x.batch_size === s);
      return j ? j.ondemand_cost_usd : null;
    }),
  });
}
new Chart(document.getElementById('costCompare'), {
  type: 'bar',
  data: { labels: sizes.map(String), datasets: barDatasets },
  options: {
    responsive: true,
    scales: {
      x: { title: { display: true, text: 'Batch size (records)', color: '#94a3b8' }, ticks: { color: '#94a3b8' }, grid: { color: '#334155' } },
      y: { title: { display: true, text: 'Total cost (USD)', color: '#94a3b8' }, ticks: { color: '#94a3b8' }, grid: { color: '#334155' } },
    },
    plugins: { legend: { labels: { color: '#e2e8f0' } } },
  },
});

function renderTable(targetId, rows, cols) {
  const t = document.getElementById(targetId);
  if (!rows.length) { t.innerHTML = '<div class="empty">No rows.</div>'; return; }
  let html = '<table><thead><tr>';
  for (const c of cols) html += `<th>${c.label}</th>`;
  html += '</tr></thead><tbody>';
  for (const r of rows) {
    html += '<tr>';
    for (const c of cols) html += `<td>${c.render(r)}</td>`;
    html += '</tr>';
  }
  html += '</tbody></table>';
  t.innerHTML = html;
}

renderTable('jobsTable', DATA.jobs, [
  { label: 'Region', render: r => r.region },
  { label: 'Batch size', render: r => fmtNum(r.batch_size, 0) },
  { label: 'Status', render: r => `<span class="status ${r.status === 'Completed' ? 'ok' : 'bad'}">${r.status}</span>` },
  { label: 'Wall-clock', render: r => fmtDuration(r.duration_seconds) },
  { label: 'Throughput', render: r => fmtNum(r.throughput_rps, 1) + ' rec/s' },
  { label: 'Succeeded', render: r => fmtNum(r.records_succeeded, 0) },
  { label: 'Failed', render: r => fmtNum(r.records_failed, 0) },
  { label: 'Batch $', render: r => fmtMoney(r.batch_cost_usd) },
  { label: '$ / 1k', render: r => fmtMoney(r.cost_per_1k_usd) },
]);

renderTable('eventsTable', DATA.events, [
  { label: 'Event time', render: r => new Date(r.event_time * 1000).toISOString().replace('T',' ').substring(0,19) },
  { label: 'Region', render: r => r.region },
  { label: 'Batch size', render: r => fmtNum(r.batch_size, 0) },
  { label: 'Old → new', render: r => `${r.old_status || '—'} → <strong>${r.new_status}</strong>` },
  { label: 'Push latency', render: r => fmtDuration(r.latency_seconds) },
  { label: 'Source', render: r => r.source },
]);

renderTable('fallbackTable', DATA.fallbacks, [
  { label: 'Region', render: r => r.region },
  { label: 'Batch size', render: r => fmtNum(r.batch_size, 0) },
  { label: 'Records recovered', render: r => fmtNum(r.records_recovered, 0) },
  { label: 'On-demand duration', render: r => fmtDuration(r.ondemand_duration_seconds) },
  { label: 'Input tokens', render: r => fmtNum(r.ondemand_input_tokens, 0) },
  { label: 'Output tokens', render: r => fmtNum(r.ondemand_output_tokens, 0) },
  { label: 'Reason', render: r => r.reason || '-' },
]);
</script>
</body>
</html>
"""


def main() -> int:
    cfg_path = PROJECT_ROOT / "config.yaml"
    with cfg_path.open() as fh:
        cfg = yaml.safe_load(fh)

    jobs = all_jobs(DB_PATH)
    fallbacks = all_fallbacks(DB_PATH)
    data = summarize(jobs, fallbacks, cfg)

    html = HTML_TEMPLATE.replace("__DATA__", json.dumps(data))
    OUT_PATH.write_text(html)
    print(f"Wrote {OUT_PATH} ({len(jobs)} jobs, {len(fallbacks)} fallbacks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
