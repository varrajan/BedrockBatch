# `benchmark/` — Bedrock batch sizing experiment

A test harness that uses the [`bedrock_batch`](../bedrock_batch/) framework
to find the optimal (region, batch_size) for a given Bedrock model, with
an HTML dashboard summarizing the results. Not production code; a
one-shot experiment you re-run when you want to re-validate your sizing
choice.

## What it does

1. Generates a fixed JSONL workload (`workload.py`), uploads it to S3.
2. Submits a batch job per (region × batch_size) listed in `config.yaml`.
3. Watches state changes via EventBridge → SQS and triggers on-demand
   recovery for any job that expires or fails (`watch_events.py`).
4. Runs a defensive sweep for anything the event path missed
   (`fallback.py`).
5. Renders `dashboard.html` with throughput/duration/cost charts, a
   per-run table, an event log, and a recommendation card
   (`build_dashboard.py`).

A dry-run mode (`simulator.py`) synthesizes plausible data and events
so the full pipeline is testable without AWS.

## Files

| File | Purpose |
| --- | --- |
| `config.yaml` | Model, regions, batch sizes, timeout, pricing, dry-run flag |
| `benchmark.py` | Submits + polls one job per (region × size) |
| `watch_events.py` | Consumes EventBridge events, triggers recovery |
| `fallback.py` | Defensive sweep for jobs the event path missed |
| `build_dashboard.py` | Reads `results.db` → writes `dashboard.html` |
| `progress.py` | Live per-record progress across in-progress jobs (optional `--watch N`) |
| `whoami.py` | Pre-flight: prints the AWS identity to be used |
| `check_permissions.sh` | Simulates every IAM action the benchmark needs (no resources created) |
| `workload.py` | Synthetic JSONL payload generator |
| `simulator.py` | Dry-run data + event generator |
| `results.db` | SQLite (created on first run) |

## Dry run (no AWS)

```bash
cd benchmark/
python3 benchmark.py --dry-run
python3 watch_events.py --dry-run
python3 fallback.py --dry-run
python3 build_dashboard.py
open dashboard.html
```

Twelve simulated jobs across two regions; one of them deliberately
expires so the fallback path is exercised.

## Live run

1. Edit `config.yaml`: set `dry_run: false`.
2. Confirm credentials:

   ```bash
   python3 whoami.py
   ```

3. Provision EventBridge + SQS infrastructure (from the project root):

   ```bash
   python3 setup_events.py
   ```

   This creates per region: an SQS queue, a dead-letter queue (3 max
   receives before DLQ), an EventBridge rule matching Bedrock batch
   state changes, and the target + policy wiring. Idempotent — safe to
   re-run.

4. Open three terminals (or three tabs).

   **Terminal A** (long-running event consumer):

   ```bash
   python3 watch_events.py --live --follow
   ```

   **Terminal B** (submit the sweep):

   ```bash
   python3 benchmark.py --live
   ```

   **Terminal C** (live progress across all in-progress jobs):

   ```bash
   python3 progress.py --watch 30
   ```

   `progress.py` scans each in-progress job's S3 output prefix every 30
   seconds and prints a table of per-record counts (processed /
   succeeded / errored / pending / percent). Fills the gap that
   `GetModelInvocationJob` leaves — you can see whether a 20-hour job is
   at 10% or 90% without waiting for completion.

5. When the benchmark finishes:

   ```bash
   python3 fallback.py --live       # defensive sweep
   python3 build_dashboard.py
   open dashboard.html
   ```

All three CLIs (`watch_events.py`, `fallback.py`, `progress.py`) are
idempotent and read-only against job state — safe to rerun or run
concurrently.

## Monitoring a run in flight

While the sweep is running, `progress.py` gives you a live view:

```bash
python3 progress.py                 # one-shot snapshot
python3 progress.py --watch 30      # refresh every 30 seconds
python3 progress.py --include-terminal   # also show completed/expired jobs
```

Sample output against an in-flight sweep:

```
AWS identity [progress]: account=... arn=...

[14:22:03] 3 job(s)
REGION           SIZE STATUS            PROCESSED     SUCC    ERR    TOTAL     PCT
----------------------------------------------------------------------------------
us-east-1      10,000 InProgress            7,841    7,829     12   10,000  78.4%
us-west-2       5,000 InProgress            4,220    4,220      0    5,000  84.4%
us-west-2      20,000 InProgress            1,105    1,101      4   20,000   5.5%
```

The counts come from scanning each job's S3 output prefix — Bedrock
writes records as they complete, so this is live. Jobs where S3 output
hasn't started yet (or where the output bucket isn't reachable) show
as zero processed with a note; they'll fill in once records start
landing. Error counts > 0 while a job is `InProgress` are early
warnings worth investigating before the job finishes.

## How to read the dashboard

- **Recommendation card** — the (region, batch_size) with the highest
  throughput among jobs that completed within the SLA with zero
  failures.
- **Throughput vs. batch size** — look for the knee of the curve. Past
  the knee, the marginal gain from a bigger batch is small and the
  risk of hitting the timeout grows.
- **Wall-clock duration vs. batch size** — this is what the 24h
  timeout acts on. A duration curve approaching the SLA line is your
  signal to shrink.
- **Push latency (event log)** — the gap between `event_time` and
  `received_at`. If this creeps up during a run, the event consumer
  is falling behind.
- **Fallback incidents** — any job that got routed through on-demand
  recovery. If the same (region, batch_size) shows up repeatedly, it
  is too large for that region's current capacity.

Note: `$/1k` is identical across batch sizes on an identical workload
because Bedrock bills per token. Don't optimize for cost; optimize for
throughput within SLA.

## Configuration reference

| Field | Purpose |
| --- | --- |
| `model_id` | Bedrock model id (default: Claude Haiku 4.5) |
| `aws_profile` | Named AWS profile. `null` = default credential chain |
| `regions` | List of regions. Each gets its own S3 bucket, SQS queue, EventBridge rule |
| `batch_sizes` | Record counts to test. Bedrock's minimum is 100 |
| `timeout_hours` | Passed to Bedrock as `timeoutDurationInHours`. Must be 24–168 |
| `poll_interval_seconds` | How often `poll_job` polls (events are faster) |
| `ondemand_concurrency` | Parallel workers for the recovery pool |
| `s3.bucket_prefix` | Bucket name is `{prefix}-{region}` |
| `s3.create_buckets` | If true, create buckets on first use |
| `iam_role_arn` | Pre-existing role Bedrock can assume. `null` = create one |
| `workload` | `short` or `medium`. See `workload.py` |
| `pricing.*` | For dashboard cost estimates (not used in submission) |
| `dry_run` | If true, no AWS calls — synthesize everything |

## Rerun cadence

Capacity shifts. Rerun this benchmark on a monthly cadence; the
"optimal" (region, batch_size) from one month will not necessarily
hold the next.
