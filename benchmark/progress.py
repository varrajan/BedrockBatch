#!/usr/bin/env python3
"""Live per-record progress across all in-progress batch jobs.

Prints a one-shot table of every job in results.db whose status is
Submitted or InProgress, with per-record counts derived from the
output S3 prefix. Pass --watch to refresh every N seconds.

Dry-run jobs (status Completed/Expired in results.db but produced by
the simulator) don't have real S3 output; this tool skips them unless
--include-terminal is set. For simulated runs the dashboard already
shows final counts.

Usage:
    python3 progress.py                   # one-shot, in-progress only
    python3 progress.py --watch 30        # refresh every 30s
    python3 progress.py --include-terminal
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bedrock_batch import get_job_progress, make_session, print_identity
from bedrock_batch.storage import all_jobs, init as store_init

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "results.db"

IN_FLIGHT = {"Submitted", "InProgress"}


def load_config(path: Path) -> dict:
    with path.open() as fh:
        return yaml.safe_load(fh)


def fmt_pct(p: float | None) -> str:
    return "—" if p is None else f"{p:5.1f}%"


def fmt_num(n: int | None) -> str:
    return "—" if n is None else f"{n:>8,d}"


def print_table(rows: list[dict]) -> None:
    if not rows:
        print("No in-progress jobs.")
        return

    header = f"{'REGION':<12} {'SIZE':>8} {'STATUS':<16} {'PROCESSED':>10} {'SUCC':>8} {'ERR':>6} {'TOTAL':>8} {'PCT':>7}"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r['region']:<12} {r['batch_size']:>8,d} {r['status']:<16} "
            f"{fmt_num(r['processed'])} {fmt_num(r['succeeded'])} {fmt_num(r['errored'])} "
            f"{fmt_num(r['total'])} {fmt_pct(r['percent_complete'])}"
        )


def gather_progress(session, jobs: list[dict]) -> list[dict]:
    out = []
    for j in jobs:
        try:
            progress = get_job_progress(
                session=session,
                region=j["region"],
                output_s3_uri=j["s3_output_uri"],
                input_s3_uri=j.get("s3_input_uri"),
                total_records=j.get("batch_size"),
            )
        except Exception as e:
            # S3 may 404 if the job hasn't written anything yet.
            progress = {
                "processed": 0, "succeeded": 0, "errored": 0,
                "total": j.get("batch_size"),
                "pending": j.get("batch_size"),
                "percent_complete": 0.0,
                "errors_sample": [], "sampled_at": time.time(),
                "note": f"read error: {e}",
            }
        out.append({**j, **progress})
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--watch", type=int, metavar="SECONDS",
                        help="Refresh every N seconds (ctrl-c to stop)")
    parser.add_argument("--include-terminal", action="store_true",
                        help="Also show jobs in terminal states (Completed/Failed/etc.)")
    args = parser.parse_args()

    cfg = load_config(PROJECT_ROOT / args.config)
    store_init(DB_PATH)

    session = make_session(cfg.get("aws_profile"))
    print_identity(session, label="progress")

    def once():
        jobs = all_jobs(DB_PATH)
        if not args.include_terminal:
            jobs = [j for j in jobs if j["status"] in IN_FLIGHT]
        rows = gather_progress(session, jobs)
        print(f"\n[{time.strftime('%H:%M:%S')}] {len(rows)} job(s)")
        print_table(rows)

    once()
    if args.watch:
        try:
            while True:
                time.sleep(args.watch)
                once()
        except KeyboardInterrupt:
            print("\nStopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
