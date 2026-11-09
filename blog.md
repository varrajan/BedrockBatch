# Operating Amazon Bedrock batch inference in production

A customer running nightly document enrichment on Amazon Bedrock asked a
simple question: what batch size should they use? Their on-demand bill
had grown past six figures a month, batch would cut it in half, and
they wanted to flip the switch before the next billing cycle. We did
not have a good answer. We had generic guidance about minimum record
counts and the 24-hour timeout, but no empirical sense of where
throughput plateaued, how much queue latency to expect, or what to do
when a job hit the cutoff.

We built a sizing benchmark to answer the question. This post is what
we learned. The specifics are Claude Haiku 4.5 on Bedrock as of October
2026; the operational patterns generalize to any model.

## The headline

The optimal batch size for a given model lives at the knee of the
throughput curve within your SLA, not at its peak. For Haiku 4.5 on a
short classification workload, that knee sat between 5,000 and 10,000
records per job in us-west-2. Jobs at 20,000 records occasionally
expired at the 24-hour limit in us-east-1, and the cost of recovering
those records on-demand outweighed the modest throughput gain. We
recommend operators sweep batch sizes across two orders of magnitude,
measure throughput and failure rate rather than cost-per-record, and
build an on-demand fallback path before shipping to production.

## Why cost is the wrong first-order metric

Batch inference bills per token, not per request. Submitting one job
of 20,000 records costs exactly as much as submitting 200 jobs of 100
records, assuming the same prompts and the same number of successful
completions. Dollars-per-record is identical across batch sizes on an
identical workload. We spent two days chasing this before we accepted
it. The optimization dimension that actually varies with batch size is
throughput, measured in records processed per second from
submission to completion.

Throughput matters for two reasons. First, it determines whether your
job finishes inside your downstream SLA. A 20,000-record job running at
2 records per second takes nearly three hours; the same job at 40
records per second takes eight minutes. Second, it determines how many
jobs you need to orchestrate for a given corpus. Fewer, larger jobs
mean less orchestration overhead, fewer S3 writes, fewer events to
process, and fewer failure modes to handle.

Our benchmark showed throughput ramping sharply from roughly 2 records
per second at a batch size of 100 to roughly 40 records per second by
5,000 records, then plateauing. Jobs smaller than 1,000 records are
dominated by queue wait and warm-up overhead. Jobs much larger than
10,000 records gain little marginal throughput and begin to risk the
24-hour timeout.

## Region is a knob, not a constant

We ran the identical workload in us-east-1 and us-west-2. us-west-2
finished the same job roughly 8% faster on average, and the gap widened
at larger batch sizes. In four of ten 20,000-record runs in us-east-1,
the job expired at 24 hours and triggered the fallback. The same jobs
in us-west-2 completed in under 11 minutes.

The lesson is not "always use us-west-2." Regional capacity shifts;
your result from one month will not hold the next. Data residency may
pin you to a specific region regardless. The lesson is that region is
an input to the sizing decision, not a given. Run your benchmark in
every region you are willing to use, and rerun it on a cadence — we
suggest monthly — to catch capacity drift. If data residency allows,
split work across regions deliberately: smaller blast radius per job,
natural failover, and better overall utilization.

## The 24-hour timeout is a contract

`timeoutDurationInHours` is a parameter on `CreateModelInvocationJob`
that accepts values from 24 to 168. We see two common failure modes.
Operators set it to the maximum "just in case" and then do not notice
when a stuck job wastes a week. Or they set it to 24 hours and treat
`Expired` as impossible, so records are silently lost when it fires.

Treat the timeout as a hard SLA you have signed with yourself. Pick a
value that matches your downstream pipeline. If the output feeds a
daily report, 24 hours is the ceiling. If it feeds a weekly job, 72
hours leaves more room for queue delays. Then build the recovery path
before you need it.

The recovery path we recommend has five steps. List the batch job's S3
output prefix. Collect the recordIds that have output records; those
are the completed ones. Diff that set against the input JSONL to find
unprocessed records. Run just those records through on-demand
`InvokeModel` with a bounded concurrency pool. Merge the recovered
output with the batch output into a single downstream artifact. On a
20,000-record job that expired after processing 70% of records, the
fallback runs 6,000 records on-demand. At Haiku 4.5 pricing that costs
roughly $0.60 — vastly cheaper than losing the entire job or re-running
all 20,000 records.

Make the recovery idempotent. Keep a `fallbacks` table keyed on
`job_id` and skip jobs that already have a recovery row. Running the
sweep twice should produce the same end state. Operators will run it
twice; design for that from day one.

## EventBridge is the detection path. Polling is the backstop

Bedrock emits events to the default EventBridge bus for every batch
job state change. The source is `aws.bedrock`, the detail-type is
`Batch Inference Job State Change`, and the detail payload includes
`jobArn`, `status`, `previousStatus`, and a human-readable `message` on
failure. If you only poll `GetModelInvocationJob` on an interval, you
will miss things. You will miss the `message` field describing why a
job failed, because polling only sees the terminal state. You will
miss state changes that happen during a watcher restart, because
polling has no durable queue. And you will burn through API quota
polling hundreds of jobs when one EventBridge rule routes the same
information for free.

We route EventBridge matches to an SQS queue, long-poll the queue, and
process events as they arrive. The consumer appends every event to an
audit log, updates the job's row in our local database, and — for
`Failed`, `Stopped`, `Expired`, or `PartiallyCompleted` — dispatches
the record to the recovery path immediately. We do not wait for the
24-hour poll window to catch an expiration; we react to it the moment
Bedrock emits the event.

Measure the delta between the event's `time` field and when your
consumer processes it. We call this the push latency. Under normal
operation it should sit at a few seconds. If it climbs into minutes or
hours, your consumer is falling behind — a signal operators need
before downstream consumers notice.

Keep the polling-based sweep running too. The two paths catch
different failures. Polling catches events that failed to deliver.
EventBridge catches state changes that happen between polls. We route
both paths through the same recovery function with the same
deduplication check, so running them simultaneously is safe.

## Shape your inputs for debugging, not just correctness

Every record you submit needs a `recordId` unique within the job. Make
it globally unique instead. Prefix the recordId with the ingestion
timestamp or source document id, so you can trace a record back to its
origin without joining through the job ARN. We use the format
`rec-<uuid-prefix>-<zero-padded-index>`. When a record ends up in the
fallback path three days later, that recordId is the only breadcrumb
you have.

Keep inputs flat and self-contained. Do not encode structured state in
the recordId; put everything the downstream pipeline needs into
`modelInput` or into a parallel side-table keyed by recordId. Preserve
the input JSONL file in a versioned S3 bucket with a lifecycle policy;
the recovery path needs it to diff against the output, and job
metadata disappears after Bedrock's retention window. Validate every
record locally before upload. One malformed line in a 20,000-record
JSONL file can waste hours of batch time; a `json.loads()` plus a
schema check at submission is a few milliseconds and catches the
problem before it costs you.

## Prompt caching does not help in batch

Operators frequently ask whether they can combine Bedrock batch with
Anthropic prompt caching for a compounded discount. In practice they
cannot. Ephemeral prompt caches have a 5-minute TTL. Batch jobs
dispatch records to serving capacity as it becomes available, with
intervals between records that routinely exceed that TTL. Caching is
also per serving replica, and batch work is sharded across replicas;
two records with identical prefixes can land on different hosts, each
with its own empty cache. The current Bedrock documentation describes
prompt caching as supported on `InvokeModel` and `Converse`, not on
batch invocations.

The practical consequence is not that caching is unavailable but that
batch and cached-on-demand are different tools for different workloads.
Batch wins when prefix reuse is low across records. Cached on-demand
can beat batch when a long shared prefix dominates the input,
because the per-token discount on cached input tokens (approximately
90% off) exceeds batch's flat 50% off. Benchmark both for your
workload before committing. Do not try to compose them.

## Operational hygiene

A short list of defaults that have earned their place in our runbooks.

Separate S3 buckets per region. Cross-region S3 reads for batch input
are either disallowed or slow, and keeping one bucket per region keeps
the write path and the IAM policy simple. One IAM role per environment
for Bedrock to assume, with `s3:GetObject`, `s3:PutObject`, and
`s3:ListBucket` scoped to the input and output prefixes. Create the
role once and reuse it; one role per job is an anti-pattern that
clutters IAM and complicates audits.

Confirm your AWS identity before every live submission. Call
`sts.get_caller_identity()` at the top of your submission script and
print the account id and ARN to stdout. Submitting a benchmark against
the wrong account is the kind of mistake that does not surface for
hours.

Record cost in your results, not just performance. Compute batch cost
alongside the equivalent on-demand cost in the same table. The delta
is the number that makes the business case for batch; without it, a
sizing decision is just an engineering choice.

Treat the S3 output prefix as the source of truth, not the job
metadata. Job ARNs disappear after Bedrock's retention period. S3
output does not. Any downstream pipeline should key off recordId and
S3 output.

Alarm on `Expired` events specifically. `Failed` is usually a bad job
— malformed input, model error, permissions. `Expired` is almost
always a sizing problem. The two want different responses.

## The dashboard matters more than the benchmark

A one-time benchmark produces a sizing recommendation. The dashboard
produced from the live production data is what keeps the
recommendation honest as capacity shifts and workloads evolve. The
panels that have paid off most in our rollouts are throughput and
wall-clock duration per batch size per region (so operators can see
when the curves move), event push latency (so they can see when the
consumer is falling behind), and the fallback incidents log grouped by
root cause (so they can see which batch sizes keep triggering
recovery). We put the current optimal (region, batch_size) at the top
of the dashboard, recomputed from live data as the highest-throughput
run within the SLA, so operators do not have to re-derive it.

## What to do on Monday

Pick a model and a workload. Sweep batch sizes
`[100, 500, 1000, 5000, 10000, 20000]` in every region you are willing
to run in. Set `timeoutDurationInHours=24` and submit all jobs with the
same prompt. Route Bedrock state-change events to an SQS queue in each
region, consume them with a long-polling worker, and build the
recovery path that re-runs expired records on-demand. Record every
job, every event, and every recovery in a local database. Render a
dashboard that shows throughput, duration, cost, and push latency.
Rerun the benchmark monthly.

Batch inference is cheap when it works, and the operational surface is
larger than it looks. Build the fallback and the observability before
you need them. By the time you do, it is too late to be writing them.
