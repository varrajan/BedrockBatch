"""Lambda: scheduled defensive sweep for jobs the event path missed.

Runs on an EventBridge schedule (e.g. every 6 hours). Scans all jobs in
DynamoDB that are still non-terminal, polls Bedrock for their current
status, and triggers recovery for any that have reached a recoverable
terminal state without an event being processed.
"""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

import storage_ddb as store

TERMINAL_STATUSES = {"Completed", "Failed", "Stopped", "Expired", "PartiallyCompleted"}
RECOVERABLE_STATUSES = {"Failed", "Stopped", "Expired", "PartiallyCompleted"}
ONDEMAND_CONCURRENCY = int(os.environ.get("ONDEMAND_CONCURRENCY", "10"))


def _recover_job(region: str, model_id: str, input_s3_uri: str, output_s3_uri: str) -> dict:
    session = boto3.Session()
    s3 = session.client("s3", region_name=region)
    runtime = session.client("bedrock-runtime", region_name=region)

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

    in_bucket, _, in_key = input_s3_uri.replace("s3://", "").partition("/")
    in_body = s3.get_object(Bucket=in_bucket, Key=in_key)["Body"].read().decode()
    all_records = [json.loads(l) for l in in_body.splitlines() if l.strip()]

    pending = [r for r in all_records if r["recordId"] not in processed_ids]
    if not pending:
        return {"records_recovered": 0, "duration_seconds": 0.0, "input_tokens": 0, "output_tokens": 0}

    start = time.time()
    in_tokens = out_tokens = 0
    recovered_outputs = []

    def _invoke_one(record):
        thread_runtime = session.client("bedrock-runtime", region_name=region)
        body = json.dumps(record["modelInput"])
        resp = thread_runtime.invoke_model(modelId=model_id, body=body)
        payload = json.loads(resp["body"].read())
        usage = payload.get("usage", {}) or {}
        return (
            {"recordId": record["recordId"], "modelOutput": payload},
            int(usage.get("input_tokens", 0)),
            int(usage.get("output_tokens", 0)),
        )

    with ThreadPoolExecutor(max_workers=ONDEMAND_CONCURRENCY) as pool:
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
        import uuid as _uuid
        recovery_key = out_prefix.rstrip("/") + f"/recovery-{_uuid.uuid4().hex[:8]}.jsonl.out"
        recovery_body = "\n".join(json.dumps(r) for r in recovered_outputs)
        s3.put_object(Bucket=out_bucket, Key=recovery_key, Body=recovery_body.encode())
        print(f"  Wrote {len(recovered_outputs)} recovered results to s3://{out_bucket}/{recovery_key}")

    return {
        "records_recovered": len(recovered_outputs),
        "duration_seconds": time.time() - start,
        "input_tokens": in_tokens,
        "output_tokens": out_tokens,
    }


def handler(event, context):
    jobs = store.all_jobs()
    non_terminal = [j for j in jobs if j.get("status") not in TERMINAL_STATUSES]
    print(f"Sweep: {len(non_terminal)} non-terminal job(s) out of {len(jobs)} total")

    swept = 0
    recovered = 0

    for job in non_terminal:
        job_id = job["job_id"]
        region = job["region"]

        try:
            bedrock = boto3.client("bedrock", region_name=region)
            resp = bedrock.get_model_invocation_job(jobIdentifier=job_id)
            current_status = resp["status"]
        except Exception as e:
            print(f"  Could not poll {job_id}: {e}")
            continue

        if current_status == job.get("status"):
            continue

        updates = {"status": current_status, "failure_reason": resp.get("message")}
        if current_status in TERMINAL_STATUSES:
            end_time = resp.get("endTime")
            if end_time:
                from datetime import datetime
                updates["completed_at"] = datetime.fromisoformat(str(end_time).replace("Z", "+00:00")).timestamp()
            else:
                updates["completed_at"] = time.time()
        store.update_job(job_id, updates)
        swept += 1
        print(f"  Updated {job_id.split('/')[-1][:12]}... -> {current_status}")

        if current_status in RECOVERABLE_STATUSES:
            if store.fallback_exists(job_id):
                print("    Recovery already recorded; skipping")
                continue

            model_id = job.get("model_id")
            if not model_id or not job.get("s3_input_uri") or not job.get("s3_output_uri"):
                print("    Missing model_id or S3 URIs; cannot recover")
                continue

            metrics = _recover_job(
                region=region,
                model_id=model_id,
                input_s3_uri=job["s3_input_uri"],
                output_s3_uri=job["s3_output_uri"],
            )

            store.record_fallback({
                "job_id": job_id,
                "triggered_at": time.time(),
                "records_recovered": metrics["records_recovered"],
                "ondemand_duration_seconds": metrics["duration_seconds"],
                "ondemand_input_tokens": metrics["input_tokens"],
                "ondemand_output_tokens": metrics["output_tokens"],
                "reason": resp.get("message") or current_status,
            })
            recovered += 1
            print(f"    Recovered {metrics['records_recovered']} records")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "jobs_scanned": len(non_terminal),
            "jobs_updated": swept,
            "jobs_recovered": recovered,
        }),
    }
