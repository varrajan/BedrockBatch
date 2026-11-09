# `bedrock_batch` — production framework

Reusable building blocks for any pipeline that submits Amazon Bedrock
batch inference jobs. No benchmark-specific code lives here; the
sizing experiment that uses this framework is in
[`../benchmark/`](../benchmark/).

## What it provides

| Module | Responsibility |
| --- | --- |
| `aws.py` | Shared `boto3.Session` + STS identity check |
| `jobs.py` | `submit_job`, `poll_job`, `get_job_progress`, `ensure_bucket`, `ensure_role` |
| `events.py` | EventBridge rule + SQS queue setup and consumer |
| `recovery.py` | On-demand recovery of records that didn't complete |
| `storage.py` | Reference SQLite persistence (swap for your own) |

## Why it exists

Four operational problems that every Bedrock batch deployment has to
solve, and the way this framework solves them:

1. **Idempotent resource setup.** `ensure_bucket` and `ensure_role`
   create (or fetch) the S3 bucket and the Bedrock-assumable IAM role
   with the correct trust and policy. Safe to call on every run.
2. **Reliable failure detection.** Polling `GetModelInvocationJob`
   alone misses state changes between polls and during watcher
   outages. `ensure_event_pipeline` + `consume_events` give you
   push-based delivery via EventBridge → SQS, with 14-day durability
   on the queue.
3. **Partial-job recovery.** When a job expires at the 24-hour cutoff
   or finishes partially, `recover_job` diffs the input JSONL against
   the output prefix in S3, re-runs the pending records through
   on-demand `InvokeModel`, and writes the recovered results back to
   S3 at `<output-prefix>/recovery.jsonl.out` alongside the batch
   output — so downstream consumers read all results uniformly.
4. **In-flight progress visibility.** `GetModelInvocationJob` returns
   only job-level status, not per-record counts. `get_job_progress`
   scans the batch's S3 output prefix (Bedrock writes records as they
   complete) and returns `processed` / `succeeded` / `errored` /
   `pending` / `percent_complete`. Lets operators tell at any moment
   whether a 20-hour job is at 10% or 90% without waiting for the
   terminal event.

## Public API

```python
from bedrock_batch import (
    make_session, print_identity,
    submit_job, poll_job, get_job_status,
    get_job_progress, count_input_records,
    ensure_role, ensure_bucket,
    ensure_event_pipeline, consume_events,
    TERMINAL_STATUSES, RECOVERABLE_STATUSES, EVENT_PATTERN,
    recover_job,
)
```

### Credentials

```python
session = make_session(profile_name=None)   # or "my-profile"
print_identity(session, label="pipeline")   # prints account + ARN
```

Every subsequent call takes `session` so your whole pipeline uses one
credential chain.

### Submitting a job

```python
ensure_bucket(session, "us-east-1", "my-bedrock-batch")
role_arn = ensure_role(
    session, "BedrockBatchRole",
    bucket_arns=["arn:aws:s3:::my-bedrock-batch"],
)

# upload your JSONL to s3://my-bedrock-batch/inputs/foo.jsonl first
job_arn = submit_job(
    session,
    region="us-east-1",
    model_id="us.anthropic.claude-haiku-4-5-20251001-v1:0",
    input_s3_uri="s3://my-bedrock-batch/inputs/foo.jsonl",
    output_s3_prefix="s3://my-bedrock-batch/outputs/foo/",
    role_arn=role_arn,
    timeout_hours=24,
)
```

`timeout_hours` is validated against Bedrock's 24-168 range before
submission.

### Polling (simple case)

```python
result = poll_job(session, "us-east-1", job_arn, interval=300)
# {"status": "Completed", "started_at": ..., "completed_at": ...,
#  "failure_reason": None}
```

Polling is a fine choice for one-shot scripts. For production, prefer
events.

### Per-record progress (while a job is running)

`GetModelInvocationJob` returns job-level status only — not record
counts. `get_job_progress` fills that gap by scanning the batch's S3
output prefix (Bedrock writes records as they complete) and
classifying each as succeeded or errored:

```python
progress = get_job_progress(
    session,
    region="us-east-1",
    output_s3_uri="s3://my-bedrock-batch/outputs/foo/",
    input_s3_uri="s3://my-bedrock-batch/inputs/foo.jsonl",   # for total
    # or: total_records=20000,
)
# {
#   "processed": 12873, "succeeded": 12854, "errored": 19,
#   "total": 20000, "pending": 7127,
#   "percent_complete": 64.4,
#   "errors_sample": [{"recordId": "...", "error": {...}}, ...],
#   "sampled_at": 1763870000.1,
# }
```

Safe to call during a job (partial writes are skipped), after a job
(final counts), and against a job that hasn't started writing yet
(returns zeros). The `errors_sample` field gives the first ten
error details so an operator can triage without reading full output.

For long-running jobs, call this on a schedule and feed the counts to
your observability stack. The benchmark exposes this directly via
[`../benchmark/progress.py`](../benchmark/progress.py) — a live-progress
CLI across every in-progress job in the local DB, optionally refreshed
with `--watch N`.

### Events (recommended for production)

```python
pipeline = ensure_event_pipeline(session, "us-east-1")
# {"queue_url": "...", "queue_arn": "...", "rule_arn": "..."}

def handle(event):
    detail = event["detail"]
    print(detail["jobArn"], detail["status"], detail.get("message"))

consume_events(
    session, "us-east-1", pipeline["queue_url"], handle,
    stop_when_idle=False,   # run forever; True for one-shot drains
)
```

The handler receives the full EventBridge envelope. The `detail` object
uses Bedrock's actual field names:

```json
{
  "detail": {
    "accountId": "123456789012",
    "version": "0.0",
    "batchJobName": "my-batch-job",
    "batchJobArn": "arn:aws:bedrock:us-east-1:...:model-invocation-job/<id>",
    "batchModelId": "arn:aws:bedrock:us-east-1:...:inference-profile/...",
    "status": "Completed",
    "creationTime": "Nov 2, 2026, 6:57:44 PM"
  }
}
```

Note: the job ARN field is `batchJobArn` (not `jobArn`). Terminal
statuses are listed in `TERMINAL_STATUSES`; the subset you probably
want to trigger recovery on is `RECOVERABLE_STATUSES`
(`Failed`/`Stopped`/`Expired`/`PartiallyCompleted`).

### Recovery

```python
metrics = recover_job(
    session,
    region="us-east-1",
    model_id="us.anthropic.claude-haiku-4-5-20251001-v1:0",
    input_s3_uri="s3://my-bedrock-batch/inputs/foo.jsonl",
    output_s3_uri="s3://my-bedrock-batch/outputs/foo/",
    concurrency=10,
)
# {"records_recovered": 1234, "duration_seconds": 61.2,
#  "input_tokens": ..., "output_tokens": ...,
#  "pending_record_ids": [...]}
```

`recover_job` does three things:

1. **Diffs S3 output vs input** — reads all `*.jsonl.out` files under
   the output prefix to collect processed `recordId`s, reads the input
   JSONL, and computes the pending set.
2. **Calls `InvokeModel` for each pending record** — on-demand, with
   bounded concurrency via a thread pool.
3. **Writes recovered results to S3** — at
   `<output-prefix>/recovery.jsonl.out`, using the same
   `recordId` + `modelOutput` format as the batch output. Downstream
   consumers can read batch and recovery output files uniformly.

Idempotency is the caller's responsibility: check your own store before
invoking and record the result after. The reference pattern (and a
working example) is in
[`../benchmark/watch_events.py`](../benchmark/watch_events.py).

### Reference storage

`bedrock_batch.storage` is a thin SQLite layer with three tables
(`jobs`, `events`, `fallbacks`) you can use as-is for small-scale
operation or as a template for a DynamoDB/Postgres implementation.
Not required to use the rest of the framework.

## Required IAM permissions

The identity running these functions needs:

- Bedrock: `CreateModelInvocationJob`, `GetModelInvocationJob`,
  `InvokeModel`
- S3: `CreateBucket`, `HeadBucket`, `GetObject`, `PutObject`,
  `ListBucket` (plus `s3:ListAllMyBuckets` is helpful)
- IAM: `CreateRole`, `GetRole`, `PutRolePolicy` (only if you use
  `ensure_role`)
- SQS: `CreateQueue`, `GetQueueAttributes`, `SetQueueAttributes`,
  `ReceiveMessage`, `DeleteMessage` (only if you use the events path)
- EventBridge: `PutRule`, `PutTargets`, `DescribeRule`,
  `ListTargetsByRule` (only if you use the events path)
- STS: `GetCallerIdentity` (for `print_identity`)

The IAM role that Bedrock assumes (created by `ensure_role` or provided
via `iam_role_arn`) needs its own permissions:

- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on the batch
  input/output buckets
- `bedrock:InvokeModel` (required for Bedrock to run inference on the
  model)

`get_job_progress` is intentionally scoped to **S3 read only** —
`s3:GetObject` and `s3:ListBucket` on the output prefix. Dashboards or
observability callers can run it without any Bedrock, IAM, SQS, or
EventBridge privileges, so a read-only ops role is sufficient.

## Operating recommendations

See [`../blog.md`](../blog.md) for the full write-up. The one-paragraph
version:

Pick the largest batch size that finishes comfortably within your SLA.
Set `timeout_hours` to match your downstream deadline (24h minimum).
Run the event consumer as a long-running process so failures land in
your observability stack within seconds, not minutes. Keep a periodic
polling sweep as a backstop. Call `get_job_progress` on a cadence
(every 1-5 minutes is typical) for every in-flight job and feed the
counts into whatever your operators watch — an early-warning signal on
`errored > 0` catches a bad job before it has consumed the full
24-hour window. Store an idempotency marker (a row keyed on `job_id`)
before invoking `recover_job`, so re-running the sweep is safe.
