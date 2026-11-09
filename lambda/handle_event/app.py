"""Lambda: SQS-triggered handler for Bedrock batch state-change events.

Replaces the long-polling watch_events.py. SQS invokes this Lambda
for each batch of messages. For each event:
  - Records the event in DynamoDB
  - Updates the job row
  - Triggers on-demand recovery for terminal non-success statuses
"""

from __future__ import annotations

import json
import os
import time

import boto3

import storage_ddb as store

RECOVERABLE_STATUSES = {"Failed", "Stopped", "Expired", "PartiallyCompleted"}
TERMINAL_STATUSES = {"Completed", "Failed", "Stopped", "Expired", "PartiallyCompleted"}
NON_STARTED_STATUSES = {"Submitted", "Validating", "Scheduled"}
ONDEMAND_CONCURRENCY = int(os.environ.get("ONDEMAND_CONCURRENCY", "10"))


def _parse_time(value) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    from datetime import datetime
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _count_output_records(region: str, output_s3_uri: str) -> tuple[int, int]:
    s3 = boto3.client("s3", region_name=region)
    bucket, _, prefix = output_s3_uri.replace("s3://", "").partition("/")
    succeeded = errored = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".jsonl.out"):
                continue
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode()
            for line in body.splitlines():
                try:
                    record = json.loads(line)
                    if "error" in record:
                        errored += 1
                    else:
                        succeeded += 1
                except Exception:
                    continue
    return succeeded, errored


def _invoke_ondemand(client, model_id: str, record: dict) -> tuple[dict, int, int]:
    body = json.dumps(record["modelInput"])
    resp = client.invoke_model(modelId=model_id, body=body)
    payload = json.loads(resp["body"].read())
    usage = payload.get("usage", {}) or {}
    return (
        {"recordId": record["recordId"], "modelOutput": payload},
        int(usage.get("input_tokens", 0)),
        int(usage.get("output_tokens", 0)),
    )


def _recover_job(region: str, model_id: str, input_s3_uri: str, output_s3_uri: str) -> dict:
    """Inline recovery: diff S3 output vs input, re-run pending via on-demand, write results to S3."""
    session = boto3.Session()
    s3 = session.client("s3", region_name=region)
    runtime = session.client("bedrock-runtime", region_name=region)

    # Collect processed record IDs from output
    out_bucket, _, out_prefix = output_s3_uri.replace("s3://", "").partition("/")
    processed_ids: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=out_bucket, Prefix=out_prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".jsonl.out"):
                continue
            body = s3.get_object(Bucket=out_bucket, Key=obj["Key"])["Body"].read().decode()
            for line in body.splitlines():
                try:
                    processed_ids.add(json.loads(line)["recordId"])
                except Exception:
                    continue

    # Read input records
    in_bucket, _, in_key = input_s3_uri.replace("s3://", "").partition("/")
    in_body = s3.get_object(Bucket=in_bucket, Key=in_key)["Body"].read().decode()
    all_records = [json.loads(l) for l in in_body.splitlines() if l.strip()]

    pending = [r for r in all_records if r["recordId"] not in processed_ids]
    if not pending:
        return {"records_recovered": 0, "duration_seconds": 0.0, "input_tokens": 0, "output_tokens": 0}

    start = time.time()
    in_tokens = out_tokens = 0
    recovered_outputs = []

    from concurrent.futures import ThreadPoolExecutor, as_completed
    concurrency = ONDEMAND_CONCURRENCY

    def _invoke_one(record):
        thread_runtime = session.client("bedrock-runtime", region_name=region)
        return _invoke_ondemand(thread_runtime, model_id, record)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_invoke_one, r): r for r in pending}
        for future in as_completed(futures):
            record = futures[future]
            try:
                output, i, o = future.result()
                in_tokens += i
                out_tokens += o
                recovered_outputs.append(output)
            except Exception as e:
                print(f"  recovery error for {record['recordId']}: {e}")

    if recovered_outputs:
        recovery_key = out_prefix.rstrip("/") + "/recovery.jsonl.out"
        recovery_body = "\n".join(json.dumps(r) for r in recovered_outputs)
        s3.put_object(Bucket=out_bucket, Key=recovery_key, Body=recovery_body.encode())
        print(f"  Wrote {len(recovered_outputs)} recovered results to s3://{out_bucket}/{recovery_key}")

    return {
        "records_recovered": len(recovered_outputs),
        "duration_seconds": time.time() - start,
        "input_tokens": in_tokens,
        "output_tokens": out_tokens,
    }


def _stop_sibling_jobs(current_job: dict) -> None:
    """When a job starts (InProgress), stop sibling jobs in other regions that haven't started yet."""
    group_id = current_job.get("group_id")
    if not group_id:
        return
    current_job_id = current_job["job_id"]
    all_jobs = store.all_jobs()
    siblings = [
        j for j in all_jobs
        if j.get("group_id") == group_id
        and j["job_id"] != current_job_id
        and j.get("status") not in TERMINAL_STATUSES
    ]
    for sibling in siblings:
        region = sibling["region"]
        job_arn = sibling["job_id"]
        try:
            bedrock = boto3.client("bedrock", region_name=region)
            bedrock.stop_model_invocation_job(jobIdentifier=job_arn)
            print(f"  Stopped sibling job {job_arn.split('/')[-1]} in {region}")
        except Exception as e:
            print(f"  Could not stop sibling {job_arn.split('/')[-1]} in {region}: {e}")


def _handle_one(event_body: dict) -> None:
    detail = event_body.get("detail", {})
    job_arn = detail.get("batchJobArn")
    new_status = detail.get("status")

    if not job_arn or not new_status:
        print(f"Skipping malformed event: {event_body.get('id')}")
        print(f"  raw detail keys: {list(detail.keys())}")
        return

    received_at = time.time()
    event_time = _parse_time(event_body.get("time", received_at))

    existing = store.get_job(job_arn)
    old_status = existing["status"] if existing else None

    if old_status == new_status:
        print(f"Skipping duplicate status event: {new_status} for {job_arn.split('/')[-1]}")
        return

    store.record_event({
        "job_id": job_arn,
        "region": event_body.get("region") or (existing or {}).get("region", ""),
        "old_status": old_status or "—",
        "new_status": new_status,
        "event_time": event_time,
        "received_at": received_at,
        "source": event_body.get("source", "aws.bedrock"),
        "raw_json": json.dumps(detail),
    })

    if existing:
        updates = {"status": new_status}
        if new_status in TERMINAL_STATUSES:
            updates["completed_at"] = event_time
            region = event_body.get("region") or existing.get("region", "")
            try:
                bedrock = boto3.client("bedrock", region_name=region)
                job_detail = bedrock.get_model_invocation_job(jobIdentifier=job_arn)
                stats = job_detail.get("statistics", {})
                if stats:
                    updates["records_succeeded"] = stats.get("processedRecordCount", 0) - stats.get("erroredRecordCount", 0)
                    updates["records_failed"] = stats.get("erroredRecordCount", 0)
                else:
                    output_uri = existing.get("s3_output_uri", "")
                    if output_uri:
                        succeeded, errored = _count_output_records(region, output_uri)
                        updates["records_succeeded"] = succeeded
                        updates["records_failed"] = errored
            except Exception as e:
                print(f"  Could not fetch job stats: {e}")
        if detail.get("message"):
            updates["failure_reason"] = detail["message"]
        store.update_job(job_arn, updates)
        existing = {**existing, **updates}

    latency = received_at - event_time
    print(f"Event: {new_status} for {job_arn.split('/')[-1][:12]}... (latency {latency:.1f}s)")

    if new_status == "InProgress" and existing:
        _stop_sibling_jobs(existing)

    if new_status in RECOVERABLE_STATUSES and existing:
        if store.fallback_exists(job_arn):
            print("  Recovery already recorded; skipping")
            return

        model_id = existing.get("model_id")
        if not model_id:
            print("  No model_id on job record; cannot recover")
            return

        metrics = _recover_job(
            region=existing["region"],
            model_id=model_id,
            input_s3_uri=existing["s3_input_uri"],
            output_s3_uri=existing["s3_output_uri"],
        )

        store.record_fallback({
            "job_id": job_arn,
            "triggered_at": time.time(),
            "records_recovered": metrics["records_recovered"],
            "ondemand_duration_seconds": metrics["duration_seconds"],
            "ondemand_input_tokens": metrics["input_tokens"],
            "ondemand_output_tokens": metrics["output_tokens"],
            "reason": existing.get("failure_reason") or new_status,
        })
        print(f"  Recovered {metrics['records_recovered']} records on-demand")


def handler(event, context):
    records = event.get("Records", [])
    print(f"Processing {len(records)} SQS message(s)")

    failures = []
    for i, record in enumerate(records):
        try:
            body = json.loads(record["body"])
            _handle_one(body)
        except Exception as e:
            print(f"Error processing record {i}: {e}")
            failures.append({"itemIdentifier": record["messageId"]})

    if failures:
        return {"batchItemFailures": failures}
    return {"batchItemFailures": []}
