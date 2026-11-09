"""Lambda: submit Bedrock batch jobs.

Triggered manually or by an EventBridge schedule. Reads config from
environment variables, submits one job per region, and records each in
DynamoDB. Does NOT poll — completion is handled by the SQS-triggered
handle_event Lambda.
"""

from __future__ import annotations

import json
import os
import time
import uuid

import boto3

import storage_ddb as store

MODEL_ID = os.environ["MODEL_ID"]
REGIONS = json.loads(os.environ["REGIONS"])
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
TIMEOUT_HOURS = int(os.environ.get("TIMEOUT_HOURS", "24"))
BUCKET_PREFIX = os.environ.get("BUCKET_PREFIX", "bedrock-batch-benchmark")
ROLE_ARN = os.environ["BEDROCK_ROLE_ARN"]
WORKLOAD = os.environ.get("WORKLOAD", "short")


def _build_input_record(record_id: str) -> dict:
    if WORKLOAD == "short":
        return {
            "recordId": record_id,
            "modelInput": {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 64,
                "messages": [{"role": "user", "content": "Say hello in one sentence."}],
            },
        }
    return {
        "recordId": record_id,
        "modelInput": {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 256,
            "messages": [{"role": "user", "content": "Explain cloud computing in a paragraph."}],
        },
    }


def _generate_and_upload_input(s3_client, bucket: str, key: str, count: int) -> None:
    lines = []
    for i in range(count):
        record = _build_input_record(f"rec-{i:06d}")
        lines.append(json.dumps(record))
    s3_client.put_object(Bucket=bucket, Key=key, Body="\n".join(lines))


def _ensure_bucket(s3_client, region: str, bucket: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket)
        return
    except s3_client.exceptions.NoSuchBucket:
        pass
    except Exception:
        pass
    kwargs = {"Bucket": bucket}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    try:
        s3_client.create_bucket(**kwargs)
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass


def handler(event, context):
    results = []
    group_id = f"group-{uuid.uuid4().hex[:8]}"

    for region in REGIONS:
        session = boto3.Session()
        s3 = session.client("s3", region_name=region)
        bedrock = session.client("bedrock", region_name=region)

        bucket = f"{BUCKET_PREFIX}-{region}"
        _ensure_bucket(s3, region, bucket)

        job_tag = f"size{BATCH_SIZE}-{uuid.uuid4().hex[:8]}"
        input_key = f"inputs/{job_tag}.jsonl"
        output_prefix = f"outputs/{job_tag}/"

        _generate_and_upload_input(s3, bucket, input_key, BATCH_SIZE)

        submitted_at = time.time()
        resp = bedrock.create_model_invocation_job(
            jobName=f"lambda-{region}-{job_tag}",
            roleArn=ROLE_ARN,
            modelId=MODEL_ID,
            inputDataConfig={
                "s3InputDataConfig": {
                    "s3Uri": f"s3://{bucket}/{input_key}",
                    "s3InputFormat": "JSONL",
                }
            },
            outputDataConfig={"s3OutputDataConfig": {"s3Uri": f"s3://{bucket}/{output_prefix}"}},
            timeoutDurationInHours=TIMEOUT_HOURS,
        )
        job_arn = resp["jobArn"]

        store.record_job({
            "job_id": job_arn,
            "region": region,
            "batch_size": BATCH_SIZE,
            "model_id": MODEL_ID,
            "workload": WORKLOAD,
            "submitted_at": submitted_at,
            "status": "Submitted",
            "group_id": group_id,
            "s3_input_uri": f"s3://{bucket}/{input_key}",
            "s3_output_uri": f"s3://{bucket}/{output_prefix}",
        })

        results.append({"region": region, "job_arn": job_arn, "status": "Submitted"})
        print(f"Submitted {region} size={BATCH_SIZE} arn={job_arn}")

    return {"statusCode": 200, "body": json.dumps(results)}
