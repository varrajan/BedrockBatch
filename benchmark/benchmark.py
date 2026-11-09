#!/usr/bin/env python3
"""Sizing-benchmark driver.

For each (region x batch_size) combination in config.yaml, submits a
Bedrock batch job (or simulates one in dry-run mode) and records the
result into results.db. Uses the bedrock_batch framework for the
submission + poll flow; everything benchmark-specific (workload
generation, simulator, config) lives in this directory.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import yaml

# Make the parent repo importable so we can `import bedrock_batch`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bedrock_batch import (
    ensure_bucket,
    ensure_role,
    make_session,
    poll_job,
    print_identity,
    submit_job,
)
from bedrock_batch.storage import init as store_init, record_job, update_job

from simulator import simulate_job
from workload import write_jsonl

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "results.db"
INPUT_DIR = PROJECT_ROOT / "inputs"


@dataclass
class JobSpec:
    region: str
    batch_size: int
    model_id: str
    workload: str
    input_path: Path


def load_config(path: Path) -> dict:
    with path.open() as fh:
        return yaml.safe_load(fh)


def run_live(spec: JobSpec, cfg: dict, session) -> dict:
    bucket = f"{cfg['s3']['bucket_prefix']}-{spec.region}"
    if cfg["s3"]["create_buckets"]:
        ensure_bucket(session, spec.region, bucket)

    role_arn = cfg.get("iam_role_arn")
    if not role_arn:
        bucket_arns = [f"arn:aws:s3:::{cfg['s3']['bucket_prefix']}-{r}" for r in cfg["regions"]]
        role_arn = ensure_role(session, "BedrockBatchBenchmarkRole", bucket_arns)

    job_tag = f"size{spec.batch_size}-{uuid.uuid4().hex[:8]}"
    input_key = f"inputs/{job_tag}.jsonl"
    output_prefix = f"outputs/{job_tag}/"
    s3 = session.client("s3", region_name=spec.region)
    s3.upload_file(str(spec.input_path), bucket, input_key)

    submitted_at = time.time()
    job_arn = submit_job(
        session=session,
        region=spec.region,
        model_id=spec.model_id,
        input_s3_uri=f"s3://{bucket}/{input_key}",
        output_s3_prefix=f"s3://{bucket}/{output_prefix}",
        role_arn=role_arn,
        timeout_hours=int(cfg["timeout_hours"]),
        job_name=f"benchmark-{spec.region}-{job_tag}",
    )

    row = {
        "job_id": job_arn,
        "region": spec.region,
        "batch_size": spec.batch_size,
        "model_id": spec.model_id,
        "workload": spec.workload,
        "submitted_at": submitted_at,
        "status": "Submitted",
        "s3_input_uri": f"s3://{bucket}/{input_key}",
        "s3_output_uri": f"s3://{bucket}/{output_prefix}",
    }
    record_job(DB_PATH, row)
    print(f"[live] submitted {spec.region} size={spec.batch_size} arn={job_arn}")

    poll_result = poll_job(
        session, spec.region, job_arn, interval=cfg["poll_interval_seconds"]
    )
    update_job(DB_PATH, job_arn, poll_result)
    print(f"[live] finished {spec.region} size={spec.batch_size} status={poll_result['status']}")
    return {**row, **poll_result}


def run_one(spec: JobSpec, cfg: dict, dry_run: bool, session=None) -> dict:
    if dry_run:
        row = simulate_job(
            region=spec.region,
            batch_size=spec.batch_size,
            model_id=spec.model_id,
            workload=spec.workload,
            bucket_prefix=cfg["s3"]["bucket_prefix"],
            timeout_hours=cfg["timeout_hours"],
        )
        record_job(DB_PATH, row)
        print(f"[sim] {spec.region} size={spec.batch_size} status={row['status']}")
        return row

    return run_live(spec, cfg, session)


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
    INPUT_DIR.mkdir(exist_ok=True)

    session = None
    if not dry_run:
        session = make_session(cfg.get("aws_profile"))
        print_identity(session, label="benchmark")

    specs: list[JobSpec] = []
    for region in cfg["regions"]:
        for size in cfg["batch_sizes"]:
            path = INPUT_DIR / f"{cfg['workload']}-{size}.jsonl"
            if not dry_run and not path.exists():
                write_jsonl(path, size, cfg["workload"])
            specs.append(JobSpec(
                region=region,
                batch_size=size,
                model_id=cfg["model_id"],
                workload=cfg["workload"],
                input_path=path,
            ))

    print(f"Submitting {len(specs)} jobs (dry_run={dry_run})")
    with cf.ThreadPoolExecutor(max_workers=min(len(specs), 8)) as ex:
        futures = [ex.submit(run_one, s, cfg, dry_run, session) for s in specs]
        for f in cf.as_completed(futures):
            try:
                f.result()
            except Exception as e:
                print(f"  job failed: {e}", file=sys.stderr)

    print(f"Done. Results in {DB_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
