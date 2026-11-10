"""Lambda: generate the benchmark dashboard and write it to S3.

Triggered manually or by an EventBridge rule after jobs complete.
Reads all data from DynamoDB, renders an HTML dashboard, and uploads
it to S3.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from html import escape as esc

import boto3

import storage_ddb as store

DASHBOARD_BUCKET = os.environ.get("DASHBOARD_BUCKET", "bedrock-batch-dashboard")
DASHBOARD_KEY = os.environ.get("DASHBOARD_KEY", "dashboard.html")
PRICING_INPUT_PER_1M = float(os.environ.get("PRICING_INPUT_PER_1M", "1.00"))
PRICING_OUTPUT_PER_1M = float(os.environ.get("PRICING_OUTPUT_PER_1M", "5.00"))
BATCH_DISCOUNT = float(os.environ.get("BATCH_DISCOUNT", "0.50"))


def _fmt_duration(seconds):
    if seconds is None:
        return "—"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m"
    return f"{m}m {s}s"


def _fmt_time(ts):
    if ts is None:
        return "—"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _build_html(jobs, events, fallbacks):
    completed = [j for j in jobs if j.get("status") == "Completed"]
    failed = [j for j in jobs if j.get("status") in ("Failed", "Expired", "PartiallyCompleted", "Stopped")]

    now = time.time()
    rows = []
    for j in sorted(jobs, key=lambda x: (x.get("region", ""), x.get("batch_size", 0))):
        duration = None
        if j.get("submitted_at") and j.get("completed_at"):
            duration = j["completed_at"] - j["submitted_at"]
        elif j.get("submitted_at"):
            duration = now - j["submitted_at"]
        rows.append(f"""
        <tr>
            <td>{esc(str(j.get('region', '—')))}</td>
            <td>{esc(str(j.get('batch_size', '—')))}</td>
            <td>{_fmt_time(j.get('submitted_at'))}</td>
            <td class="status-{esc(j.get('status', 'Unknown').lower())}">{esc(j.get('status', '—'))}</td>
            <td>{_fmt_duration(duration)}</td>
            <td>{j.get('records_succeeded', 0)}</td>
            <td>{j.get('records_failed', 0)}</td>
            <td>{esc(str(j.get('failure_reason') or '—'))}</td>
        </tr>""")

    sorted_events = sorted(events, key=lambda x: x.get("event_time", 0))
    last_event_time = {}
    event_rows = []
    for e in sorted_events:
        job_id = e.get("job_id", "")
        event_time = e.get("event_time")
        transition_time = ""
        if event_time and job_id in last_event_time:
            diff = event_time - last_event_time[job_id]
            if diff < 60:
                transition_time = f"{diff:.1f}s"
            elif diff < 3600:
                transition_time = f"{int(diff // 60)}m {int(diff % 60)}s"
            else:
                h = int(diff // 3600)
                m = int((diff % 3600) // 60)
                transition_time = f"{h}h {m}m"
        if event_time:
            last_event_time[job_id] = event_time
        event_rows.append(f"""
        <tr>
            <td>{_fmt_time(e.get('event_time'))}</td>
            <td>{esc(str(e.get('region', '—')))}</td>
            <td>{esc(e.get('job_id', '—').split('/')[-1])}</td>
            <td>{esc(str(e.get('old_status', '—')))}</td>
            <td>{esc(str(e.get('new_status', '—')))}</td>
            <td>{transition_time}</td>
        </tr>""")

    fallback_rows = []
    for f in sorted(fallbacks, key=lambda x: x.get("triggered_at", 0)):
        fallback_rows.append(f"""
        <tr>
            <td>{esc(f.get('job_id', '—').split('/')[-1])}</td>
            <td>{f.get('records_recovered', 0)}</td>
            <td>{_fmt_duration(f.get('ondemand_duration_seconds'))}</td>
            <td>{esc(str(f.get('reason', '—')))}</td>
        </tr>""")

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Bedrock Batch Dashboard</title>
<style>
  body {{ font-family: -apple-system, sans-serif; margin: 2rem; background: #f8f9fa; }}
  h1 {{ color: #1a1a2e; }}
  h2 {{ color: #16213e; margin-top: 2rem; }}
  table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; background: white; }}
  th, td {{ padding: 0.5rem 1rem; text-align: left; border-bottom: 1px solid #e0e0e0; }}
  th {{ background: #1a1a2e; color: white; }}
  .summary {{ display: flex; gap: 1rem; margin: 1rem 0; }}
  .card {{ background: white; padding: 1rem 1.5rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .card h3 {{ margin: 0 0 0.5rem 0; color: #666; font-size: 0.9rem; }}
  .card .value {{ font-size: 1.8rem; font-weight: bold; color: #1a1a2e; }}
  .status-completed {{ color: #2d7d46; font-weight: bold; }}
  .status-failed, .status-expired, .status-stopped {{ color: #d32f2f; font-weight: bold; }}
  .status-partiallycompleted {{ color: #e65100; font-weight: bold; }}
  .footer {{ color: #999; margin-top: 2rem; font-size: 0.85rem; }}
</style>
</head>
<body>
<h1>Bedrock Batch Dashboard</h1>

<div class="summary">
  <div class="card"><h3>Total Jobs</h3><div class="value">{len(jobs)}</div></div>
  <div class="card"><h3>Completed</h3><div class="value">{len(completed)}</div></div>
  <div class="card"><h3>Failed / Expired</h3><div class="value">{len(failed)}</div></div>
  <div class="card"><h3>Recoveries</h3><div class="value">{len(fallbacks)}</div></div>
  <div class="card"><h3>Events</h3><div class="value">{len(events)}</div></div>
</div>

<h2>Jobs</h2>
<table>
<tr><th>Region</th><th>Batch Size</th><th>Submitted</th><th>Status</th><th>Duration</th><th>Succeeded</th><th>Failed</th><th>Failure Reason</th></tr>
{"".join(rows) if rows else "<tr><td colspan='8'>No jobs recorded</td></tr>"}
</table>

<h2>Events</h2>
<table>
<tr><th>Time</th><th>Region</th><th>Job</th><th>From</th><th>To</th><th>Duration</th></tr>
{"".join(event_rows) if event_rows else "<tr><td colspan='6'>No events recorded</td></tr>"}
</table>

<h2>Fallback Recoveries</h2>
<table>
<tr><th>Job</th><th>Records Recovered</th><th>Duration</th><th>Reason</th></tr>
{"".join(fallback_rows) if fallback_rows else "<tr><td colspan='4'>No fallbacks triggered</td></tr>"}
</table>

<div class="footer">Generated {generated} by build_dashboard Lambda</div>
</body>
</html>"""


def handler(event, context):
    jobs = store.all_jobs()
    events = store.all_events()
    fallbacks = store.all_fallbacks()

    print(f"Building dashboard: {len(jobs)} jobs, {len(events)} events, {len(fallbacks)} fallbacks")

    html = _build_html(jobs, events, fallbacks)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=DASHBOARD_BUCKET,
        Key=DASHBOARD_KEY,
        Body=html,
        ContentType="text/html",
        ServerSideEncryption="AES256",
    )

    print(f"Dashboard written to s3://{DASHBOARD_BUCKET}/{DASHBOARD_KEY}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "jobs": len(jobs),
            "events": len(events),
            "fallbacks": len(fallbacks),
            "s3_uri": f"s3://{DASHBOARD_BUCKET}/{DASHBOARD_KEY}",
        }),
    }
