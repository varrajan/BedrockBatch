# Bedrock Batch Toolkit

A production framework for Amazon Bedrock batch inference, a sizing
benchmark harness, and a serverless (Lambda) deployment option. Closes
the four operational gaps Bedrock batch leaves open: failure detection,
partial-job recovery, in-flight progress visibility, and observability.

---

## Table of contents

- [Project layout](#project-layout)
- [How it works](#how-it-works)
- [Quick start](#quick-start)
- [Framework (`bedrock_batch/`)](#framework-bedrock_batch)
- [Benchmark (`benchmark/`)](#benchmark-benchmark)
- [Serverless deployment (`lambda/`)](#serverless-deployment-lambda)
- [Infrastructure setup (`setup_events.py`)](#infrastructure-setup-setup_eventspy)
- [EventBridge event format](#eventbridge-event-format)
- [IAM permissions](#iam-permissions)
- [Configuration reference](#configuration-reference)
- [Design decisions](#design-decisions)

---

## Project layout

```
BedrockBatch/
├── bedrock_batch/              # Production framework (import this)
│   ├── aws.py                  # Shared boto3 session + STS identity check
│   ├── jobs.py                 # submit_job, poll_job, get_job_progress,
│   │                           #   ensure_bucket, ensure_role
│   ├── events.py               # EventBridge → SQS pipeline setup + consumer
│   ├── recovery.py             # On-demand recovery for timed-out/failed records
│   └── storage.py              # Reference SQLite persistence layer
│
├── benchmark/                  # Sizing experiment (uses the framework)
│   ├── config.yaml             # Model, regions, batch sizes, timeout, pricing
│   ├── benchmark.py            # Submits + polls one job per (region × size)
│   ├── watch_events.py         # Consumes EventBridge events; triggers recovery
│   ├── fallback.py             # Defensive sweep for anything events missed
│   ├── build_dashboard.py      # Reads results.db → writes dashboard.html
│   ├── progress.py             # Live per-record progress for in-flight jobs
│   ├── whoami.py               # Pre-flight: prints current AWS identity
│   ├── check_permissions.sh    # IAM permission validator (no resources created)
│   ├── workload.py             # Synthetic JSONL payload generator
│   └── simulator.py            # Dry-run data + event synthesizer
│
├── lambda/                     # Serverless deployment (SAM)
│   ├── template.yaml           # CloudFormation/SAM template for all resources
│   ├── shared/python/
│   │   └── storage_ddb.py      # DynamoDB storage layer (replaces SQLite)
│   ├── submit_jobs/app.py      # Lambda: submit batch jobs
│   ├── handle_event/app.py     # Lambda: SQS-triggered event handler + recovery
│   ├── fallback_sweep/app.py   # Lambda: scheduled defensive sweep
│   └── build_dashboard/app.py  # Lambda: generate HTML dashboard to S3
│
├── setup_events.py             # Provisions EventBridge + SQS infra per region
├── blog.md                     # Operational write-up with sizing numbers
├── blog.docx                   # Word rendering of the blog
├── generate_blog_docx.py       # Rebuilds blog.docx from blog.md
└── requirements.txt            # boto3, pyyaml, python-docx
```

---

## How it works

Bedrock batch inference is best-effort — jobs run on spare capacity,
there is no completion SLA, and the service does not notify you on
failure or expose per-record progress. This toolkit closes four gaps:

### 1. Failure detection (EventBridge → SQS)

Bedrock emits state-change events on the default EventBridge bus
whenever a batch job transitions (Submitted → InProgress → Completed,
Failed, Expired, etc.). `events.py` creates an EventBridge rule that
matches these events and routes them to a durable SQS queue (14-day
retention). The consumer long-polls SQS and calls your handler for
each event.

```
Bedrock job state change
    → EventBridge (default bus)
        → SQS queue (bedrock-batch-events)
            → your handler (or Lambda trigger)
```

This is superior to polling `GetModelInvocationJob` because:
- Events arrive within seconds of the state change
- Events are durable — a watcher outage does not lose them
- No missed transitions between poll intervals

### 2. Partial-job recovery

When a job expires (at the 24-hour minimum timeout), fails, or
completes partially, `recovery.py` automatically:

1. Lists all `*.jsonl.out` files under the job's S3 output prefix
2. Collects the set of `recordId`s that have output
3. Reads the original input JSONL
4. Computes the diff — records that were submitted but not processed
5. Fans pending records out via on-demand `InvokeModel` with bounded
   concurrency

The caller owns idempotency: check a persistent store before calling
`recover_job()`, and record the result after.

### 3. In-flight progress visibility

`GetModelInvocationJob` returns only job-level status — not how many
records have been processed. `get_job_progress()` fills this gap by
scanning the batch's S3 output prefix (Bedrock writes records as they
complete) and returns:

```python
{
    "processed": 12873,         # succeeded + errored
    "succeeded": 12854,
    "errored": 19,
    "errors_sample": [...],     # first 10 error details for triage
    "total": 20000,
    "pending": 7127,
    "percent_complete": 64.4,
    "sampled_at": 1763870000.1,
}
```

Safe to call during a job, after a job, or before the job has started
writing (returns zeros). Only requires S3 read permissions.

### 4. Observability

Every job submission, event, and recovery is recorded in a storage
layer (SQLite for the benchmark, DynamoDB for Lambda). The benchmark
renders this into an HTML dashboard with throughput, duration, cost,
push latency, and fallback incident tables.

---

## Quick start

### Install

```bash
pip install -r requirements.txt
```

### Dry run (no AWS, no spend)

```bash
cd benchmark/
python3 benchmark.py --dry-run
python3 watch_events.py --dry-run
python3 fallback.py --dry-run
python3 build_dashboard.py
open dashboard.html
```

This synthesizes 12 jobs across two regions (one deliberately expires),
exercises the full event + recovery pipeline, and renders the
dashboard.

### Pre-flight checks

```bash
cd benchmark/
python3 whoami.py              # prints account + ARN
bash check_permissions.sh      # simulates every IAM action needed
```

### Provision infrastructure

```bash
python3 setup_events.py
```

Creates per region (idempotent, safe to re-run):
- SQS queue: `bedrock-batch-events` (14-day retention)
- SQS dead-letter queue: `bedrock-batch-events-dlq` (3 max receives)
- EventBridge rule: `bedrock-batch-rule` (matches `aws.bedrock` /
  `Batch Inference Job State Change`)
- EventBridge target: routes matched events to the SQS queue
- Queue policy: allows EventBridge to `sqs:SendMessage`

Validates all resources after creation and prints a summary.

### Live run (from laptop)

```bash
# Terminal A — long-running event consumer + auto-recovery
cd benchmark/
python3 watch_events.py --live --follow

# Terminal B — submit batch jobs
python3 benchmark.py --live

# Terminal C — live progress (optional)
python3 progress.py --watch 30

# After completion
python3 fallback.py --live       # defensive sweep
python3 build_dashboard.py
open dashboard.html
```

### Live run (serverless)

```bash
cd lambda/
sam build
sam deploy --guided              # first time
aws lambda invoke --function-name bedrock-batch-submit-jobs /dev/stdout
# Events are handled automatically via SQS trigger
# Fallback sweep runs automatically every 6 hours
aws lambda invoke --function-name bedrock-batch-build-dashboard /dev/stdout
```

---

## Framework (`bedrock_batch/`)

The core library. Stateless, session-parameterized, no opinion on
storage. Import it into your own pipeline.

### Modules

| Module | What it does |
|---|---|
| `aws.py` | `make_session(profile_name)` — returns a `boto3.Session`. `print_identity(session)` — calls STS, prints account/ARN, exits loudly on bad credentials. |
| `jobs.py` | `submit_job()` — creates a Bedrock batch job, validates timeout (24–168h). `poll_job()` — blocks until terminal status. `get_job_status()` — one-shot status check. `get_job_progress()` — per-record counts from S3 output. `count_input_records()` — counts JSONL lines in S3. `ensure_bucket()` — idempotent S3 bucket creation. `ensure_role()` — idempotent IAM role with S3 + `bedrock:InvokeModel` permissions. |
| `events.py` | `ensure_event_pipeline()` — idempotently creates EventBridge rule + SQS queue + target + policy. `consume_events()` — long-polls SQS and calls your handler per event. Exports `EVENT_PATTERN`, `TERMINAL_STATUSES`, `RECOVERABLE_STATUSES`. |
| `recovery.py` | `recover_job()` — diffs S3 output vs input, re-runs pending records via on-demand `InvokeModel`. Returns metrics (records recovered, tokens, duration). |
| `storage.py` | Reference SQLite layer with three tables: `jobs`, `events`, `fallbacks`. Thin CRUD functions. Swap for DynamoDB/Postgres in production. |

### Usage pattern

```python
from bedrock_batch import (
    make_session, print_identity,
    submit_job, poll_job, get_job_progress,
    ensure_bucket, ensure_role,
    ensure_event_pipeline, consume_events,
    recover_job, RECOVERABLE_STATUSES,
)

session = make_session()
print_identity(session)

# Setup
ensure_bucket(session, "us-east-1", "my-batch-bucket")
role_arn = ensure_role(session, "MyBatchRole",
    bucket_arns=["arn:aws:s3:::my-batch-bucket"])

# Submit
job_arn = submit_job(session, "us-east-1",
    model_id="us.anthropic.claude-haiku-4-5-20251001-v1:0",
    input_s3_uri="s3://my-batch-bucket/input.jsonl",
    output_s3_prefix="s3://my-batch-bucket/output/",
    role_arn=role_arn)

# Monitor progress while running
progress = get_job_progress(session, "us-east-1",
    output_s3_uri="s3://my-batch-bucket/output/",
    total_records=1000)

# Event-driven completion
pipeline = ensure_event_pipeline(session, "us-east-1")
def handle(event):
    status = event["detail"]["status"]
    if status in RECOVERABLE_STATUSES:
        metrics = recover_job(session, "us-east-1", ...)

consume_events(session, "us-east-1", pipeline["queue_url"], handle)
```

---

## Benchmark (`benchmark/`)

A sizing experiment that submits batch jobs across multiple
(region, batch_size) combinations and produces a dashboard comparing
throughput, duration, and cost.

### Scripts

| Script | What it does |
|---|---|
| `benchmark.py` | Submits one batch job per (region x batch_size) in `config.yaml`. In live mode, polls `GetModelInvocationJob` every 5 minutes until each job reaches a terminal state. Records results to `results.db`. |
| `watch_events.py` | Consumes Bedrock state-change events from SQS. For each event: records it in the DB, updates the job row, and triggers `recover_job()` for any recoverable terminal status (`Failed`, `Stopped`, `Expired`, `PartiallyCompleted`). In `--follow` mode, runs indefinitely. |
| `fallback.py` | Defensive sweep across all jobs in the DB. Finds jobs in non-success terminal states that don't already have a recovery row, and runs `recover_job()` on them. Catches anything EventBridge missed. |
| `progress.py` | Live per-record progress for in-flight jobs. Scans each job's S3 output prefix and prints processed/succeeded/errored/pending/percent. `--watch 30` refreshes every 30 seconds. |
| `build_dashboard.py` | Reads `results.db` and renders `dashboard.html` with summary cards, per-job table, event log, and fallback incidents. |
| `workload.py` | Generates synthetic JSONL input files. `short` = ~50 input tokens, ~10 output tokens per record. `medium` = ~500 input, ~150 output. |
| `simulator.py` | Dry-run engine. Produces plausible job data (larger batches = higher throughput up to saturation, one region slightly faster, largest batch occasionally expires) and EventBridge-shaped events. Deterministic given (region, batch_size). |
| `whoami.py` | Prints the AWS identity that will be used. Exits with code 2 if credentials are missing. |
| `check_permissions.sh` | Uses `iam:SimulatePrincipalPolicy` to validate every IAM action the benchmark needs, without creating resources. |

### How to read the dashboard

- **Recommendation card** — the (region, batch_size) with the highest
  throughput among jobs that completed within SLA with zero failures.
- **Throughput vs. batch size** — look for the knee of the curve. Past
  the knee, marginal gain is small and timeout risk grows.
- **Wall-clock duration** — a curve approaching the 24h SLA line means
  you should shrink the batch.
- **Push latency** — the gap between `event_time` and `received_at`.
  Creeping latency means the event consumer is falling behind.
- **Fallback incidents** — any job routed through on-demand recovery.
  Repeated (region, batch_size) entries mean that size is too large.

---

## Serverless deployment (`lambda/`)

A Lambda-based replacement for the laptop scripts. Same pipeline logic,
no long-running processes. Uses DynamoDB instead of SQLite.

### Architecture

```
┌──────────────┐
│ submit_jobs  │  (manual invoke or EventBridge schedule)
│   Lambda     │
└──────┬───────┘
       │ CreateModelInvocationJob
       ▼
┌───────────────┐
│    Bedrock    │
│  Batch Jobs   │
└───────┬───────┘
       │ state-change event
       ▼
┌─────────────────┐
│  EventBridge    │
│  (default bus)  │
└────────┬────────┘
       │ rule: aws.bedrock / Batch Inference Job State Change
       ▼
┌─────────────────┐       ┌─────────────────┐
│   SQS Queue     │──────▶│  handle_event   │
│ bedrock-batch-  │       │    Lambda        │
│ events          │       └────────┬────────┘
└─────────────────┘                │
                                   │ on recoverable status:
                                   │ diff S3 → InvokeModel
                                   ▼
                          ┌─────────────────┐
                          │   DynamoDB       │
                          │ jobs / events /  │
                          │ fallbacks        │
                          └─────────────────┘
                                   ▲
┌─────────────────┐                │
│ fallback_sweep  │────────────────┘
│ Lambda (cron)   │  every 6h: polls non-terminal jobs
└─────────────────┘

┌─────────────────┐
│ build_dashboard │  reads DynamoDB → writes HTML to S3
│     Lambda      │
└─────────────────┘
```

### Lambda functions

| Function | Trigger | What it does |
|---|---|---|
| `submit_jobs` | Manual invoke or schedule | Generates input JSONL, uploads to S3, submits a Bedrock batch job per region. Records each job in DynamoDB. Does not poll — completion is event-driven. |
| `handle_event` | SQS (automatic) | Processes each Bedrock state-change event: records the event, updates the job, and runs on-demand recovery for `Failed`/`Stopped`/`Expired`/`PartiallyCompleted` jobs. Uses `ReportBatchItemFailures` for partial-batch SQS retries. |
| `fallback_sweep` | EventBridge schedule (every 6h) | Safety net for the event-driven path. The primary path (`handle_event`) relies on EventBridge delivering every state-change event to SQS — but events can be lost (SQS retention expired, DLQ overflow, transient AWS failures). The fallback sweep catches these gaps: it scans DynamoDB for jobs still marked as non-terminal, calls `GetModelInvocationJob` to check their real status in Bedrock, updates any stale rows, and triggers on-demand recovery for jobs that finished in a recoverable state (`Failed`/`Expired`/`PartiallyCompleted`) but were never handled by `handle_event`. With a 24-hour minimum job timeout, running every 6 hours gives 4 chances to catch a missed event before anyone notices. |
| `build_dashboard` | Manual invoke | Reads all data from DynamoDB, renders an HTML dashboard, and writes it to S3. |

### DynamoDB tables

The Lambda deployment uses three DynamoDB tables (PAY_PER_REQUEST billing)
instead of the laptop mode's SQLite file. Lambda functions are stateless
and can run concurrently, so a shared, multi-writer-safe store is required.

| Table | Key | What it stores |
|---|---|---|
| **Jobs** | `job_id` (hash) | One row per batch job: region, model, batch size, status, submission/completion timestamps, records succeeded/failed, failure reason. Written by `submit_jobs`, updated by `handle_event` and `fallback_sweep`. |
| **Events** | `job_id` (hash) + `id` (range) | One row per EventBridge state-change event received: job ARN, old/new status, event timestamp, receive timestamp (for push-latency measurement). Written by `handle_event`. |
| **Fallbacks** | `job_id` (hash) + `id` (range) | One row per on-demand recovery attempt: trigger reason, records recovered, tokens used, duration. Used for idempotency (skip if already recovered) and dashboard reporting. Written by `handle_event` and `fallback_sweep`. |

### What changed vs laptop scripts

| Laptop | Lambda | Key difference |
|---|---|---|
| `benchmark.py` (submit + poll) | `submit_jobs` (submit only) | No blocking poll loop; completion is event-driven |
| `watch_events.py` (long-poll SQS) | `handle_event` (SQS trigger) | No running process; Lambda invoked per message batch |
| `fallback.py` (manual run) | `fallback_sweep` (cron) | Runs automatically every 6 hours |
| `build_dashboard.py` (local file) | `build_dashboard` (S3) | Dashboard written to S3, accessible via URL |
| `storage.py` (SQLite) | `storage_ddb.py` (DynamoDB) | Serverless-compatible, multi-writer safe |

### Deploy

```bash
cd lambda/
sam build
sam deploy --guided    # first time; prompts for parameters
sam deploy             # subsequent deploys
```

### SAM parameters

| Parameter | Required | Default | Description |
|---|---|---|---|
| `BedrockRoleArn` | Yes | — | ARN of the IAM role Bedrock assumes |
| `ModelId` | No | Claude Haiku 4.5 | Bedrock model ID |
| `Regions` | No | `["us-east-1","us-west-2"]` | JSON array of regions |
| `BatchSize` | No | 100 | Records per job |
| `TimeoutHours` | No | 24 | 24–168 (Bedrock's accepted range) |
| `BucketPrefix` | No | `bedrock-batch-benchmark` | S3 bucket naming |
| `DashboardBucket` | No | `bedrock-batch-dashboard` | Where HTML is written |
| `FallbackSchedule` | No | `rate(6 hours)` | Cron for the sweep |
| `OndemandConcurrency` | No | 10 | Parallel workers for recovery |

---

## Infrastructure setup (`setup_events.py`)

A standalone script that provisions the EventBridge + SQS plumbing
directly on your AWS account. Run it before the benchmark or as a
one-time setup for the Lambda deployment.

**What it creates per region:**

| Resource | Name | Purpose |
|---|---|---|
| SQS Queue | `bedrock-batch-events` | Receives Bedrock state-change events, 14-day retention |
| SQS DLQ | `bedrock-batch-events-dlq` | Catches messages that fail processing 3 times |
| EventBridge Rule | `bedrock-batch-rule` | Matches `aws.bedrock` / `Batch Inference Job State Change` |
| EventBridge Target | rule → queue | Routes matched events into the SQS queue |
| Queue Policy | on main queue | Allows EventBridge `sqs:SendMessage`, scoped to the rule ARN |

After creating resources, the script validates each region (rule
enabled, target wired, queue policy set, redrive policy present) and
prints a summary with all ARNs and URLs.

```bash
python3 setup_events.py
```

Reads `benchmark/config.yaml` for the region list and AWS profile.
Idempotent — safe to run repeatedly.

---

## EventBridge event format

Bedrock emits events with this structure (verified against live events):

```json
{
  "version": "0",
  "id": "d7cc3ccf-0dad-9df1-...",
  "detail-type": "Batch Inference Job State Change",
  "source": "aws.bedrock",
  "account": "123456789012",
  "time": "2026-11-02T18:57:44Z",
  "region": "us-east-1",
  "resources": [
    "arn:aws:bedrock:us-east-1:123456789012:model-invocation-job/5u9b33sh79aj"
  ],
  "detail": {
    "accountId": "123456789012",
    "version": "0.0",
    "batchJobName": "my-batch-job",
    "batchJobArn": "arn:aws:bedrock:us-east-1:123456789012:model-invocation-job/5u9b33sh79aj",
    "batchModelId": "arn:aws:bedrock:us-east-1:123456789012:inference-profile/us.anthropic.claude-haiku-4-5-20251001-v1:0",
    "status": "Submitted",
    "creationTime": "Nov 2, 2026, 6:57:44 PM"
  }
}
```

**Important:** The job ARN field is `batchJobArn` (not `jobArn`).
Status values include: `Submitted`, `Validating`, `Scheduled`,
`InProgress`, `Completed`, `Failed`, `Stopped`, `Expired`,
`PartiallyCompleted`.

---

## IAM permissions

### Caller identity (your laptop or Lambda execution role)

| Service | Actions | When needed |
|---|---|---|
| Bedrock | `CreateModelInvocationJob`, `GetModelInvocationJob`, `InvokeModel` | Always |
| S3 | `CreateBucket`, `HeadBucket`, `GetObject`, `PutObject`, `ListBucket` | Always |
| IAM | `CreateRole`, `GetRole`, `PutRolePolicy`, `PassRole` | Only if using `ensure_role()` |
| SQS | `CreateQueue`, `GetQueueAttributes`, `SetQueueAttributes`, `ReceiveMessage`, `DeleteMessage` | Only for the events path |
| EventBridge | `PutRule`, `PutTargets`, `DescribeRule`, `ListTargetsByRule` | Only for the events path |
| STS | `GetCallerIdentity` | For `print_identity()` |
| DynamoDB | `PutItem`, `GetItem`, `UpdateItem`, `Scan`, `Query` | Lambda deployment only |

### Bedrock-assumed role

The IAM role that Bedrock assumes to run inference (`ensure_role()`
creates this, or you provide it via `iam_role_arn`):

| Action | Resource |
|---|---|
| `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` | Your batch input/output S3 buckets |
| `bedrock:InvokeModel` | `*` (required for Bedrock to run model inference) |

The trust policy must allow `bedrock.amazonaws.com` to assume the role.

### Read-only progress monitoring

`get_job_progress()` only needs `s3:GetObject` and `s3:ListBucket` on
the output prefix. A read-only ops role is sufficient for dashboards
and alerting.

---

## Configuration reference

`benchmark/config.yaml` controls the benchmark. Lambda uses SAM
template parameters instead.

| Field | Default | Description |
|---|---|---|
| `model_id` | Claude Haiku 4.5 | Bedrock model ID for batch jobs |
| `aws_profile` | `null` | Named AWS profile; `null` = default credential chain |
| `regions` | `[us-east-1, us-west-2]` | Regions to benchmark (each gets its own S3 bucket, SQS queue, EventBridge rule) |
| `batch_sizes` | `[100]` | Record counts to test; Bedrock minimum is 100 |
| `timeout_hours` | `24` | Passed to `timeoutDurationInHours`; valid range 24–168 |
| `poll_interval_seconds` | `300` | How often `poll_job` checks status (events are faster) |
| `ondemand_concurrency` | `10` | Parallel workers for on-demand recovery |
| `s3.bucket_prefix` | `bedrock-batch-benchmark` | Bucket name = `{prefix}-{region}` |
| `s3.create_buckets` | `true` | Create buckets on first use |
| `iam_role_arn` | `null` | Pre-existing Bedrock role; `null` = create `BedrockBatchBenchmarkRole` |
| `workload` | `short` | `short` (~50 in / ~10 out tokens) or `medium` (~500 in / ~150 out) |
| `pricing.*` | Haiku 4.5 rates | For dashboard cost estimates only |
| `dry_run` | `false` | If true, no AWS calls; synthesize everything |

---

## Design decisions

- **Framework and benchmark are separate packages.** `bedrock_batch/`
  knows nothing about the benchmark. Import it directly into your own
  pipeline.
- **Framework is stateless where it can be.** `recover_job()` returns
  metrics; the caller owns idempotency and persistence. Swap SQLite for
  DynamoDB/Postgres without touching the framework.
- **Session is a parameter, not a global.** Every framework function
  takes a `boto3.Session`, so multi-account callers can hold multiple
  sessions concurrently.
- **Dry-run is first-class.** The simulator produces plausible data and
  EventBridge-shaped events so the full pipeline (including recovery)
  is testable without AWS or spend.
- **Two deployment modes.** The laptop scripts (benchmark/) are ideal
  for one-shot experiments. The Lambda deployment (lambda/) is for
  ongoing production use with no running processes.
- **DLQ for poison messages.** Both `setup_events.py` and the SAM
  template configure a dead-letter queue with 3 max receives, so a
  malformed event doesn't block the queue forever.
- **Event format verified against live Bedrock events.** The `detail`
  object uses `batchJobArn` (not `jobArn`) — confirmed by capturing
  real events from the SQS queue.
