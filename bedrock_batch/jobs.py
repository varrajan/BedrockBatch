"""Bedrock batch job submission, polling, progress, and resource setup.

Core lifecycle:

    submit_job(session, region, model_id, input_s3_uri, output_s3_prefix,
               role_arn, timeout_hours=24, job_name=None) -> job_arn
    poll_job(session, region, job_arn, interval=300) -> status dict
    get_job_status(session, region, job_arn) -> raw GetModelInvocationJob dict

Progress (fills a Bedrock API gap: GetModelInvocationJob returns only
job-level status, so we derive per-record counts by scanning the output
prefix in S3 as records are written):

    get_job_progress(session, region, output_s3_uri, input_s3_uri=None,
                     total_records=None) -> progress dict
    count_input_records(session, region, input_s3_uri) -> int

Idempotent setup:

    ensure_bucket(session, region, bucket_name)
    ensure_role(session, role_name, bucket_arns) -> role_arn
"""

from __future__ import annotations

import json
import time
import uuid

TERMINAL_STATUSES = {"Completed", "Failed", "Stopped", "Expired", "PartiallyCompleted"}


def ensure_bucket(session, region: str, bucket: str) -> None:
    """Idempotently create an S3 bucket in the given region."""
    s3 = session.client("s3", region_name=region)
    try:
        s3.head_bucket(Bucket=bucket)
        return
    except Exception:
        pass
    kwargs = {"Bucket": bucket}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3.create_bucket(**kwargs)


def ensure_role(session, role_name: str, bucket_arns: list[str]) -> str:
    """Create (or fetch) an IAM role Bedrock can assume to read/write S3.

    The role trusts `bedrock.amazonaws.com` and gets a scoped inline
    policy granting S3 GetObject/PutObject/ListBucket on the bucket ARNs
    passed in. Returns the role ARN. IAM is eventually consistent, so a
    newly created role is given a short delay before this returns.
    """
    iam = session.client("iam")
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "bedrock.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }
    try:
        resp = iam.get_role(RoleName=role_name)
        return resp["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        pass

    resp = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(trust),
        Description="Bedrock batch inference role",
    )
    role_arn = resp["Role"]["Arn"]

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                "Resource": bucket_arns + [a + "/*" for a in bucket_arns],
            },
            {
                "Effect": "Allow",
                "Action": "bedrock:InvokeModel",
                "Resource": "arn:aws:bedrock:*::foundation-model/*",
            },
        ],
    }
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="BedrockBatchS3Access",
        PolicyDocument=json.dumps(policy),
    )
    time.sleep(12)
    return role_arn


def submit_job(
    session,
    region: str,
    model_id: str,
    input_s3_uri: str,
    output_s3_prefix: str,
    role_arn: str,
    timeout_hours: int = 24,
    job_name: str | None = None,
) -> str:
    """Submit one Bedrock batch inference job. Returns the job ARN.

    Validates `timeout_hours` is in Bedrock's 24-168 accepted range.
    Routes to the configured region and the given IAM role.
    """
    if not 24 <= timeout_hours <= 168:
        raise ValueError(
            f"timeout_hours={timeout_hours} is outside Bedrock's accepted "
            f"range (24-168 hours for timeoutDurationInHours)."
        )

    bedrock = session.client("bedrock", region_name=region)
    name = job_name or f"batch-{uuid.uuid4().hex[:12]}"
    resp = bedrock.create_model_invocation_job(
        jobName=name,
        roleArn=role_arn,
        modelId=model_id,
        inputDataConfig={
            "s3InputDataConfig": {
                "s3Uri": input_s3_uri,
                "s3InputFormat": "JSONL",
            }
        },
        outputDataConfig={"s3OutputDataConfig": {"s3Uri": output_s3_prefix}},
        timeoutDurationInHours=timeout_hours,
    )
    return resp["jobArn"]


def get_job_status(session, region: str, job_arn: str) -> dict:
    """One-shot GetModelInvocationJob."""
    bedrock = session.client("bedrock", region_name=region)
    return bedrock.get_model_invocation_job(jobIdentifier=job_arn)


def count_input_records(session, region: str, input_s3_uri: str) -> int:
    """Count the number of JSONL records in the batch input file."""
    bucket, _, key = input_s3_uri.replace("s3://", "").partition("/")
    s3 = session.client("s3", region_name=region)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    return sum(1 for line in body.splitlines() if line.strip())


def _classify_output_line(line: str) -> tuple[str | None, str | None, dict | None]:
    """Returns (recordId, bucket, error_detail).

    `bucket` is "succeeded" or "errored" based on which field is set on the
    output record; None if the line is unparseable (partial write, etc).
    """
    try:
        obj = json.loads(line)
    except Exception:
        return None, None, None

    record_id = obj.get("recordId")
    if "error" in obj and obj["error"]:
        return record_id, "errored", obj["error"]
    if "modelOutput" in obj and obj["modelOutput"] is not None:
        return record_id, "succeeded", None
    # Record present but neither succeeded nor errored shape — treat as
    # unknown; don't count toward progress.
    return record_id, None, None


def get_job_progress(
    session,
    region: str,
    output_s3_uri: str,
    input_s3_uri: str | None = None,
    total_records: int | None = None,
) -> dict:
    """Compute per-record progress for a batch job in flight (or completed).

    Scans every ``*.jsonl.out`` object under ``output_s3_uri`` and classifies
    each record as succeeded (has ``modelOutput``) or errored (has
    ``error``). Fills a Bedrock gap: ``GetModelInvocationJob`` only
    returns job-level status.

    If ``total_records`` is given, uses it as the denominator. Otherwise,
    if ``input_s3_uri`` is given, counts records in the input file. If
    neither, ``total`` / ``pending`` / ``percent_complete`` are None.

    Returns::

        {
            "processed": int,              # succeeded + errored
            "succeeded": int,
            "errored": int,
            "errors_sample": list[dict],   # first 10 error details for triage
            "total": int | None,
            "pending": int | None,
            "percent_complete": float | None,
            "sampled_at": float,
        }

    Partial writes mid-flight are skipped (unparseable lines don't count).
    """
    bucket, _, key_prefix = output_s3_uri.replace("s3://", "").partition("/")
    s3 = session.client("s3", region_name=region)

    succeeded = 0
    errored = 0
    errors_sample: list[dict] = []
    seen_ids: set[str] = set()

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".jsonl.out"):
                continue
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode()
            for line in body.splitlines():
                rec_id, kind, err = _classify_output_line(line)
                if not rec_id or not kind:
                    continue
                if rec_id in seen_ids:
                    # Guard against duplicate IDs across output files.
                    continue
                seen_ids.add(rec_id)
                if kind == "succeeded":
                    succeeded += 1
                else:
                    errored += 1
                    if len(errors_sample) < 10 and err:
                        errors_sample.append({"recordId": rec_id, "error": err})

    processed = succeeded + errored

    total: int | None = total_records
    if total is None and input_s3_uri:
        total = count_input_records(session, region, input_s3_uri)

    pending = (total - processed) if total is not None else None
    percent = (processed / total * 100.0) if total else None

    return {
        "processed": processed,
        "succeeded": succeeded,
        "errored": errored,
        "errors_sample": errors_sample,
        "total": total,
        "pending": pending,
        "percent_complete": percent,
        "sampled_at": time.time(),
    }


def poll_job(
    session,
    region: str,
    job_arn: str,
    interval: int = 300,
    on_tick=None,
) -> dict:
    """Block until the job reaches a terminal state.

    Returns {status, started_at, completed_at, failure_reason}. Callers
    that need push-based delivery should use EventBridge instead; polling
    exists for a backstop and simple scripts.
    """
    started_at = None
    while True:
        resp = get_job_status(session, region, job_arn)
        status = resp["status"]
        if on_tick:
            on_tick(status, resp)
        if status == "InProgress" and not started_at:
            started_at = time.time()
        if status in TERMINAL_STATUSES:
            return {
                "status": status,
                "started_at": started_at,
                "completed_at": time.time(),
                "failure_reason": resp.get("message") if status != "Completed" else None,
            }
        time.sleep(interval)
