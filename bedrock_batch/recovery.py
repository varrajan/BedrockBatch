"""On-demand recovery for records that didn't complete in batch.

When a batch job expires, fails, or completes partially, call
`recover_job(...)`:

    1. Lists the batch's S3 output prefix, reads every *.jsonl.out file,
       and collects the set of `recordId`s that have output records.
    2. Reads the input JSONL.
    3. The diff is the pending set.
    4. Fans those pending records out across a bounded concurrency pool
       calling bedrock-runtime.InvokeModel. Token totals come from the
       `usage` field in each response.
    5. Returns a metrics dict the caller can persist.

Callers are expected to make `recover_job` idempotent on their side --
check a persistent store before calling, and record the result after.
This module intentionally has no opinion about storage.
"""

from __future__ import annotations

import concurrent.futures as cf
import json
import time


def list_processed_record_ids(session, output_s3_uri: str, region: str) -> set[str]:
    """Read every *.jsonl.out under the output prefix and collect recordIds."""
    bucket, _, key_prefix = output_s3_uri.replace("s3://", "").partition("/")
    s3 = session.client("s3", region_name=region)
    processed: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".jsonl.out"):
                continue
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode()
            for line in body.splitlines():
                try:
                    processed.add(json.loads(line)["recordId"])
                except Exception:
                    continue
    return processed


def read_input_records(session, input_s3_uri: str, region: str) -> list[dict]:
    """Load the JSONL input file."""
    bucket, _, key = input_s3_uri.replace("s3://", "").partition("/")
    s3 = session.client("s3", region_name=region)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    return [json.loads(l) for l in body.splitlines() if l.strip()]


def invoke_ondemand(
    session, region: str, model_id: str, record: dict
) -> tuple[dict, int, int]:
    """Call InvokeModel for one record. Returns (output, in_tokens, out_tokens)."""
    client = session.client("bedrock-runtime", region_name=region)
    body = json.dumps(record["modelInput"])
    resp = client.invoke_model(modelId=model_id, body=body)
    payload = json.loads(resp["body"].read())
    usage = payload.get("usage", {}) or {}
    return (
        {"recordId": record["recordId"], "modelOutput": payload},
        int(usage.get("input_tokens", 0)),
        int(usage.get("output_tokens", 0)),
    )


def recover_job(
    session,
    region: str,
    model_id: str,
    input_s3_uri: str,
    output_s3_uri: str,
    concurrency: int = 10,
) -> dict:
    """End-to-end recovery of unprocessed records.

    Returns:
      {
        "records_recovered": int,
        "duration_seconds": float,
        "input_tokens": int,
        "output_tokens": int,
        "pending_record_ids": list[str],   # the IDs recovered
      }
    """
    processed = list_processed_record_ids(session, output_s3_uri, region)
    inputs = read_input_records(session, input_s3_uri, region)
    pending = [r for r in inputs if r["recordId"] not in processed]

    if not pending:
        return {
            "records_recovered": 0,
            "duration_seconds": 0.0,
            "input_tokens": 0,
            "output_tokens": 0,
            "pending_record_ids": [],
        }

    start = time.time()
    in_tokens = out_tokens = 0
    recovered_outputs = []
    with cf.ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [
            ex.submit(invoke_ondemand, session, region, model_id, r)
            for r in pending
        ]
        for f in cf.as_completed(futures):
            output, i, o = f.result()
            in_tokens += i
            out_tokens += o
            recovered_outputs.append(output)

    # Write recovered results to S3 alongside the batch output
    if recovered_outputs:
        bucket, _, prefix = output_s3_uri.replace("s3://", "").partition("/")
        recovery_key = prefix.rstrip("/") + "/recovery.jsonl.out"
        body = "\n".join(json.dumps(r) for r in recovered_outputs)
        s3 = session.client("s3", region_name=region)
        s3.put_object(Bucket=bucket, Key=recovery_key, Body=body.encode())

    return {
        "records_recovered": len(recovered_outputs),
        "duration_seconds": time.time() - start,
        "input_tokens": in_tokens,
        "output_tokens": out_tokens,
        "pending_record_ids": [r["recordId"] for r in pending],
    }
