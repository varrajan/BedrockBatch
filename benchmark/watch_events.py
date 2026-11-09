#!/usr/bin/env python3
"""Benchmark event consumer.

A thin shim over bedrock_batch.consume_events: provisions the pipeline
per region, drains the SQS queues, and for each event:
  * writes a row into the `events` table
  * updates the job row in `jobs`
  * triggers on-demand recovery for terminal non-success events

Dry-run mode synthesizes events from the simulated jobs in results.db
so the full pipeline is exercised without AWS.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import sys
import time
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bedrock_batch import (
    RECOVERABLE_STATUSES,
    consume_events,
    ensure_event_pipeline,
    make_session,
    print_identity,
    recover_job,
)
from bedrock_batch.storage import (
    all_jobs,
    fallback_exists,
    get_job,
    init as store_init,
    record_event,
    record_fallback,
    update_job,
)

from simulator import simulate_recovery, synthesize_events

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "results.db"


def load_config(path: Path) -> dict:
    with path.open() as fh:
        return yaml.safe_load(fh)


def _parse_time(value) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    from datetime import datetime
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def handle_event(event: dict, cfg: dict, dry_run: bool, session=None) -> None:
    detail = event.get("detail", {})
    job_arn = detail.get("batchJobArn")
    new_status = detail.get("status")
    if not job_arn or not new_status:
        print(f"  skipping malformed event: {event.get('id')}")
        print(f"    raw event: {json.dumps(event, indent=2, default=str)}")
        return

    received_at = time.time()
    event_time = _parse_time(event.get("time"))

    existing = get_job(DB_PATH, job_arn)
    old_status = existing["status"] if existing else None

    record_event(DB_PATH, {
        "job_id": job_arn,
        "region": event.get("region") or (existing or {}).get("region") or "",
        "old_status": old_status,
        "new_status": new_status,
        "event_time": event_time,
        "received_at": received_at,
        "source": event.get("source", "aws.bedrock"),
        "raw_json": json.dumps(detail),
    })

    if existing:
        updates = {"status": new_status, "completed_at": event_time}
        if detail.get("message"):
            updates["failure_reason"] = detail["message"]
        update_job(DB_PATH, job_arn, updates)
        existing = {**existing, **updates}

    latency = received_at - event_time
    print(f"  event: {new_status} for {job_arn.split('/')[-1][:12]}... "
          f"(latency {latency:.1f}s)")

    if new_status in RECOVERABLE_STATUSES and existing:
        if fallback_exists(DB_PATH, job_arn):
            print("    recovery already recorded; skipping")
            return

        if dry_run:
            metrics = simulate_recovery(existing, cfg["ondemand_concurrency"])
        else:
            metrics = recover_job(
                session=session,
                region=existing["region"],
                model_id=existing["model_id"],
                input_s3_uri=existing["s3_input_uri"],
                output_s3_uri=existing["s3_output_uri"],
                concurrency=cfg["ondemand_concurrency"],
            )

        record_fallback(DB_PATH, {
            "job_id": job_arn,
            "triggered_at": time.time(),
            "records_recovered": metrics["records_recovered"],
            "ondemand_duration_seconds": metrics["duration_seconds"],
            "ondemand_input_tokens": metrics["input_tokens"],
            "ondemand_output_tokens": metrics["output_tokens"],
            "reason": existing.get("failure_reason") or new_status,
        })
        print(f"    on-demand recovered {metrics['records_recovered']} records")


def run_live(cfg: dict, follow: bool) -> int:
    session = make_session(cfg.get("aws_profile"))
    print_identity(session, label="watch_events")

    pipelines = {}
    for region in cfg["regions"]:
        info = ensure_event_pipeline(session, region)
        pipelines[region] = info
        print(f"  pipeline ready in {region}: {info['queue_url']}")

    def consume_region(region: str) -> int:
        return consume_events(
            session=session,
            region=region,
            queue_url=pipelines[region]["queue_url"],
            handler=lambda ev: handle_event(ev, cfg, dry_run=False, session=session),
            stop_when_idle=not follow,
        )

    with cf.ThreadPoolExecutor(max_workers=len(cfg["regions"])) as ex:
        futures = {ex.submit(consume_region, r): r for r in cfg["regions"]}
        total = 0
        for f in cf.as_completed(futures):
            region = futures[f]
            try:
                n = f.result()
                total += n
                print(f"  {region}: processed {n} event(s)")
            except Exception as e:
                print(f"  {region} consumer error: {e}", file=sys.stderr)
    print(f"Done. Total events processed: {total}")
    return 0


def run_dry(cfg: dict) -> int:
    jobs = all_jobs(DB_PATH)
    synth = synthesize_events(jobs)
    print(f"Synthesized {len(synth)} event(s) from {len(jobs)} job(s)")
    for ev in synth:
        handle_event(ev, cfg, dry_run=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--live", action="store_true")
    parser.add_argument("--follow", action="store_true",
                        help="(live only) keep polling after queues go idle")
    args = parser.parse_args()

    cfg = load_config(PROJECT_ROOT / args.config)
    dry_run = args.dry_run or (cfg.get("dry_run", False) and not args.live)

    store_init(DB_PATH)

    if dry_run:
        return run_dry(cfg)
    return run_live(cfg, follow=args.follow)


if __name__ == "__main__":
    sys.exit(main())
