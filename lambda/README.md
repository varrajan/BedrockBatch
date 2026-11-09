# `lambda/` — Serverless Bedrock batch pipeline

A Lambda-based replacement for the laptop scripts. Same pipeline logic,
no long-running processes. Uses DynamoDB instead of SQLite for
serverless-compatible, multi-writer-safe persistence.

## Architecture

```
                ┌──────────────┐
                │ submit_jobs  │  (manual invoke or schedule)
                │   Lambda     │
                └──────┬───────┘
                       │ upload JSONL + CreateModelInvocationJob
                       ▼
               ┌───────────────┐          ┌─────────────┐
               │    Bedrock    │─────────▶│  S3 Output  │
               │  Batch Jobs   │          │  *.jsonl.out│
               └───────┬───────┘          └──────┬──────┘
                       │ state-change event      │
                       ▼                         │ (read by recovery)
              ┌─────────────────┐                │
              │  EventBridge    │                │
              │  (default bus)  │                │
              └────────┬────────┘                │
                       │ rule match              │
                       ▼                         │
              ┌─────────────────┐       ┌────────┴────────┐
              │   SQS Queue     │──────▶│  handle_event   │
              │   + DLQ         │       │    Lambda        │
              └─────────────────┘       └────────┬────────┘
                                                 │
                                    ┌────────────┼────────────┐
                                    │ record     │ recover    │ write
                                    │ event      │ pending    │ results
                                    ▼            ▼            ▼
                          ┌──────────────┐  ┌──────────┐  ┌──────────┐
                          │  DynamoDB    │  │ Bedrock  │  │ S3       │
                          │  3 tables    │  │ Runtime  │  │ recovery │
                          │              │  │ Invoke   │  │ .jsonl   │
                          └──────────────┘  │ Model    │  │ .out     │
                                   ▲        └──────────┘  └──────────┘
              ┌─────────────────┐  │
              │ fallback_sweep  │──┘  every 6h: catches missed events
              │ Lambda (cron)   │     polls Bedrock → recover → write S3
              └─────────────────┘

              ┌─────────────────┐
              │ build_dashboard │  reads DynamoDB → writes HTML to S3
              │     Lambda      │
              └─────────────────┘
```

## Lambda functions

| Function | Trigger | What it does |
|---|---|---|
| `submit_jobs` | Manual invoke or schedule | Generates input JSONL, uploads to S3, calls `CreateModelInvocationJob` per region. All jobs in a single invocation share a `group_id` for race-to-start sibling cancellation. Records each job in the DynamoDB Jobs table. Does not poll — completion is event-driven via `handle_event`. |
| `handle_event` | SQS (automatic) | Processes Bedrock state-change events from SQS. For each event: records it in the Events table, updates the job's status in the Jobs table. On `InProgress`, triggers race-to-start sibling cancellation (stops jobs in other regions with the same `group_id`). For recoverable terminal statuses (`Failed`/`Stopped`/`Expired`/`PartiallyCompleted`) — runs parallel on-demand recovery. Recovery diffs the S3 output against the input to find pending records, calls `InvokeModel` for each via a thread pool, writes results to `<output-prefix>/recovery.jsonl.out` in S3, and records the recovery in the Fallbacks table. Uses `ReportBatchItemFailures` for partial-batch SQS retries. |
| `fallback_sweep` | EventBridge schedule (every 6h) | Safety net for the event-driven path. Events can be lost (SQS retention expired, DLQ overflow, transient failures). The sweep scans DynamoDB for jobs still marked non-terminal, calls `GetModelInvocationJob` to check their real status in Bedrock, updates stale rows, and triggers on-demand recovery (with S3 write-back) for any that reached a recoverable state without being handled. With a 24h minimum job timeout, running every 6h gives 4 chances to catch a missed event. |
| `build_dashboard` | Manual invoke | Reads all data from the three DynamoDB tables, renders an HTML dashboard with summary cards, job table, event log, and fallback incidents, and writes it to S3. |

## DynamoDB tables

Three tables store all pipeline state (PAY_PER_REQUEST billing). Lambda
functions are stateless and can run concurrently, so a shared,
multi-writer-safe store is required — SQLite cannot serve this role.

| Table | Key | What it stores |
|---|---|---|
| **Jobs** | `job_id` (hash) | One row per batch job: region, model, batch size, status, `group_id` (links sibling jobs across regions), submission/completion timestamps, records succeeded/failed, failure reason. Written by `submit_jobs`, updated by `handle_event` and `fallback_sweep`. |
| **Events** | `job_id` (hash) + `id` (range) | One row per EventBridge state-change event received: job ARN, old/new status, event timestamp, receive timestamp (for push-latency measurement). Written by `handle_event`. |
| **Fallbacks** | `job_id` (hash) + `id` (range) | One row per on-demand recovery attempt: trigger reason, records recovered, tokens used, duration. Used for idempotency (skip if already recovered) and dashboard reporting. Written by `handle_event` and `fallback_sweep`. |

## How recovery works

Bedrock batch inference requires every input record to have a unique
`recordId`. As Bedrock processes records, it writes results to
`*.jsonl.out` files under the job's S3 output prefix **incrementally**
— not all at once when the job finishes. Each output line contains the
same `recordId` from the input. This is what makes partial recovery
possible.

When a job reaches a recoverable terminal state (`Failed`, `Stopped`,
`Expired`, `PartiallyCompleted`), the recovery logic runs these steps:

1. **List processed records** — read all `*.jsonl.out` files under the
   job's S3 output prefix and collect every `recordId` into a set.
   These are the records Bedrock successfully processed before the job
   ended.

2. **Read the original input** — load the input JSONL file and collect
   all `recordId`s.

3. **Diff** — any input `recordId` not found in the output set is a
   pending record that Bedrock never processed. For example, if a job
   with 1,000 records expires at the 24-hour timeout having processed
   800, the diff produces the 200 unprocessed records.

4. **Invoke on-demand (parallel)** — call `bedrock-runtime:InvokeModel`
   for each pending record using a thread pool with configurable
   concurrency (`ONDEMAND_CONCURRENCY`, default 50). Each thread creates
   its own Bedrock Runtime client to avoid connection contention. With
   concurrency=50, recovery throughput is ~4.5 records/second —
   processing 100 records takes ~22 seconds. At this rate, 1,000
   pending records take ~3-4 minutes, and the full 50,000-record
   Bedrock batch limit takes ~3 hours (within Lambda's 15-minute
   timeout only for batches up to ~4,000 records).

5. **Write results to S3** — the recovered responses are written to
   `<output-prefix>/recovery.jsonl.out`, using the same
   `recordId` + `modelOutput` format as the batch output files.
   Downstream consumers can read all `*.jsonl.out` files (batch +
   recovery) uniformly without special handling.

6. **Record in DynamoDB** — a row in the Fallbacks table tracks the
   recovery (records recovered, tokens used, duration) for idempotency
   and dashboard reporting.

Both `handle_event` (inline, triggered by the SQS event) and
`fallback_sweep` (scheduled every 6h) use this same recovery logic.

## Files

```
lambda/
├── template.yaml                # SAM template (all infra + functions)
├── shared/
│   └── python/
│       └── storage_ddb.py       # DynamoDB storage layer (Lambda layer)
├── submit_jobs/
│   └── app.py                   # Submit batch jobs across regions
├── handle_event/
│   └── app.py                   # SQS-triggered: process events + recovery
├── fallback_sweep/
│   └── app.py                   # Scheduled sweep for missed events
├── build_dashboard/
│   └── app.py                   # Generate HTML dashboard to S3
├── setup_cross_region/
│   └── app.py                   # Custom resource: cross-region EventBridge forwarding
└── README.md
```

## Prerequisites

- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
  (or install via pip: `pip install aws-sam-cli`)
- The Bedrock-assumable IAM role (created by `ensure_role` or manually)
  must have `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`, and
  `bedrock:InvokeModel`
- An S3 bucket for the dashboard output

## Deploy

```bash
cd lambda/

# Build
sam build

# Deploy (first time — guided prompts for parameters)
sam deploy --guided

# Subsequent deploys
sam deploy
```

SAM will prompt for parameter values on the first guided deploy:

| Parameter | Required | Default | Description |
|---|---|---|---|
| `BedrockRoleArn` | Yes | — | ARN of the IAM role Bedrock assumes for S3 + InvokeModel |
| `ModelId` | No | Claude Haiku 4.5 | Bedrock model ID |
| `Regions` | No | `["us-east-1","us-west-2"]` | JSON array of regions to submit jobs in |
| `BatchSize` | No | 100 | Records per batch job |
| `TimeoutHours` | No | 24 | 24–168 (Bedrock's accepted range) |
| `BucketPrefix` | No | `bedrock-batch-benchmark` | S3 bucket naming prefix (`{prefix}-{region}`) |
| `DashboardBucket` | No | `bedrock-batch-dashboard` | S3 bucket where the HTML dashboard is written |
| `FallbackSchedule` | No | `rate(6 hours)` | EventBridge schedule for the fallback sweep |
| `OndemandConcurrency` | No | 50 | Parallel workers for on-demand recovery |

## Usage

**Submit jobs:**

```bash
aws lambda invoke --function-name bedrock-batch-submit-jobs /dev/stdout
```

**Events are handled automatically** — the SQS trigger invokes
`handle_event` whenever Bedrock emits a state-change event. If the job
is in a recoverable state, recovery runs inline and results are written
to S3.

**Fallback sweep runs automatically** on the configured schedule
(default: every 6 hours). Catches any jobs whose events were missed.

**Build dashboard:**

```bash
aws lambda invoke --function-name bedrock-batch-build-dashboard /dev/stdout
```

The dashboard is written to `s3://<DashboardBucket>/dashboard.html`.

## Cross-region event routing

The stack deploys all infrastructure (SQS queue, EventBridge rule,
Lambdas, DynamoDB tables) in a single region. When jobs run in other
regions, their EventBridge events need to reach the stack's SQS queue.

The template handles this automatically via a custom resource
(`setup_cross_region` Lambda) that runs at deploy time. For each remote
region in the `Regions` parameter, it creates an EventBridge rule that
forwards Bedrock batch state-change events to the stack region's default
event bus. The existing rule in the stack region then routes them to
SQS as normal.

```
us-west-2 EventBridge ──┐
                         ├──▶ us-east-1 EventBridge ──▶ SQS ──▶ handle_event Lambda
us-east-2 EventBridge ──┘
```

The forwarding rules are created/updated on every deploy and cleaned up
on stack deletion. An IAM role (`CrossRegionEventRole`) grants the
remote EventBridge services permission to put events on the stack
region's bus.

## Race-to-start (automatic sibling cancellation)

When jobs are submitted across multiple regions, they share a `group_id`
that links them as siblings of the same submission. The first job to
reach `InProgress` in any region wins — `handle_event` automatically
calls `StopModelInvocationJob` on all sibling jobs that haven't started
yet.

This avoids paying for duplicate batch processing across regions when
only one result set is needed. The pattern is:

1. `submit_jobs` generates a `group_id` (e.g. `group-a1b2c3d4`) and
   stores it on every job in the group.
2. EventBridge delivers an `InProgress` event from the winning region.
3. `handle_event` queries DynamoDB for all jobs with the same
   `group_id`, filters to non-terminal siblings, and stops each one
   via the Bedrock API in its respective region.
4. The stopped jobs emit their own `Stopped` events, which
   `handle_event` records normally (and triggers on-demand recovery
   for any partially-processed records if needed).
