"""Dry-run simulator: synthesize plausible benchmark data without AWS.

Models three effects:
  * Larger batches have higher throughput up to a saturation point
  * Larger batches wait longer in the queue before starting
  * One region modeled slightly faster, to produce a non-trivial comparison
  * The largest batch in the slower region occasionally expires at 24h

Deterministic given (region, batch_size) so results are reproducible.
"""

from __future__ import annotations

import random
import time
import uuid


def simulate_job(region: str, batch_size: int, model_id: str, workload: str,
                 bucket_prefix: str, timeout_hours: int) -> dict:
    rng = random.Random(f"{region}:{batch_size}")
    region_factor = 1.00 if region == "us-east-1" else 0.92

    queue_wait = (30 + 0.004 * batch_size) * region_factor * rng.uniform(0.9, 1.2)

    sat = 40.0
    throughput = sat * (batch_size / (batch_size + 1500)) * rng.uniform(0.9, 1.1)
    throughput = max(throughput, 1.5)
    exec_seconds = batch_size / throughput * region_factor

    will_timeout = (
        batch_size >= 20000
        and region == "us-east-1"
        and rng.random() < 0.6
    )
    if will_timeout:
        exec_seconds = timeout_hours * 3600 + rng.uniform(60, 600)
        status = "Expired"
        failed = int(batch_size * rng.uniform(0.05, 0.15))
        succeeded = batch_size - failed - int(batch_size * rng.uniform(0.1, 0.3))
        failure_reason = f"Job exceeded timeout of {timeout_hours}h"
    else:
        status = "Completed"
        succeeded = batch_size
        failed = 0
        failure_reason = None

    submitted_at = time.time()
    started_at = submitted_at + queue_wait
    completed_at = started_at + exec_seconds

    in_per = 50 if workload == "short" else 500
    out_per = 10 if workload == "short" else 150
    input_tokens = succeeded * in_per
    output_tokens = succeeded * out_per

    return {
        "job_id": f"arn:aws:bedrock:{region}:000000000000:model-invocation-job/{uuid.uuid4()}",
        "region": region,
        "batch_size": batch_size,
        "model_id": model_id,
        "workload": workload,
        "submitted_at": submitted_at,
        "started_at": started_at,
        "completed_at": completed_at,
        "status": status,
        "records_succeeded": succeeded,
        "records_failed": failed,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "s3_input_uri": f"s3://{bucket_prefix}-{region}/inputs/sim-{batch_size}.jsonl",
        "s3_output_uri": f"s3://{bucket_prefix}-{region}/outputs/sim-{batch_size}/",
        "failure_reason": failure_reason,
    }


def synthesize_events(jobs: list[dict]) -> list[dict]:
    """Build fake state-change events for simulated jobs.

    One event per job's terminal state. Shaped like a real EventBridge
    event so the benchmark's event handler is exercised unchanged.
    """
    out = []
    for j in jobs:
        if not j.get("status"):
            continue
        event_time = j.get("completed_at") or time.time()
        out.append({
            "version": "0",
            "id": str(uuid.uuid4()),
            "source": "aws.bedrock",
            "detail-type": "Batch Inference Job State Change",
            "region": j["region"],
            "time": event_time,
            "detail": {
                "jobArn": j["job_id"],
                "status": j["status"],
                "modelId": j["model_id"],
                "previousStatus": "InProgress",
                "message": j.get("failure_reason") or "",
            },
        })
    return out


def simulate_recovery(job: dict, concurrency: int) -> dict:
    """Fake metrics for the dry-run fallback path."""
    rng = random.Random(f"recover:{job['job_id']}")
    pending = job["batch_size"] - job.get("records_succeeded", 0) - job.get("records_failed", 0)
    pending = max(pending + job.get("records_failed", 0), 0)
    rec_per_sec = concurrency * rng.uniform(1.8, 2.4)
    duration = pending / max(rec_per_sec, 0.1)
    in_per = 50 if job["workload"] == "short" else 500
    out_per = 10 if job["workload"] == "short" else 150
    return {
        "records_recovered": pending,
        "duration_seconds": duration,
        "input_tokens": pending * in_per,
        "output_tokens": pending * out_per,
    }
