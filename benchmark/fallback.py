#!/usr/bin/env python3
"""Defensive sweep: catches jobs the event path missed.

Scans the jobs table and runs recovery for any job that is in a
non-success terminal state or that exceeded the configured timeout.
Idempotent: skips jobs that already have a row in `fallbacks`.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bedrock_batch import make_session, print_identity, recover_job
from bedrock_batch.storage import (
    all_jobs,
    fallback_exists,
    init as store_init,
    record_fallback,
)

from simulator import simulate_recovery

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "results.db"

TIMEOUT_STATUSES = {"Expired", "Failed", "Stopped", "PartiallyCompleted"}


def load_config(path: Path) -> dict:
    with path.open() as fh:
        return yaml.safe_load(fh)


def should_fallback(job: dict, timeout_hours: int) -> bool:
    if job["status"] == "Completed":
        return False
    if job["status"] in TIMEOUT_STATUSES:
        return True
    if job.get("completed_at") and job.get("submitted_at"):
        if (job["completed_at"] - job["submitted_at"]) >= timeout_hours * 3600:
            return True
    reason = (job.get("failure_reason") or "").lower()
    return "timeout" in reason or "expired" in reason


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--live", action="store_true")
    args = parser.parse_args()

    cfg = load_config(PROJECT_ROOT / args.config)
    dry_run = args.dry_run or (cfg.get("dry_run", False) and not args.live)

    store_init(DB_PATH)

    session = None
    if not dry_run:
        session = make_session(cfg.get("aws_profile"))
        print_identity(session, label="fallback")

    jobs = all_jobs(DB_PATH)
    candidates = [j for j in jobs if should_fallback(j, cfg["timeout_hours"])]
    print(f"Found {len(candidates)} job(s) requiring on-demand fallback")

    for job in candidates:
        if fallback_exists(DB_PATH, job["job_id"]):
            print(f"  skip size={job['batch_size']} region={job['region']} (already recovered)")
            continue

        if dry_run:
            metrics = simulate_recovery(job, cfg["ondemand_concurrency"])
            tag = "sim"
        else:
            metrics = recover_job(
                session=session,
                region=job["region"],
                model_id=job["model_id"],
                input_s3_uri=job["s3_input_uri"],
                output_s3_uri=job["s3_output_uri"],
                concurrency=cfg["ondemand_concurrency"],
            )
            tag = "live"

        record_fallback(DB_PATH, {
            "job_id": job["job_id"],
            "triggered_at": time.time(),
            "records_recovered": metrics["records_recovered"],
            "ondemand_duration_seconds": metrics["duration_seconds"],
            "ondemand_input_tokens": metrics["input_tokens"],
            "ondemand_output_tokens": metrics["output_tokens"],
            "reason": job.get("failure_reason") or job["status"],
        })
        print(f"  [{tag}] recovered {metrics['records_recovered']} records "
              f"for size={job['batch_size']} region={job['region']}")

    print("Fallback pass complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
