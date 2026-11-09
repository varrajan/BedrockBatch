#!/usr/bin/env python3
"""Provision EventBridge + SQS infrastructure for Bedrock batch event delivery.

Creates per-region:
  - SQS dead-letter queue (bedrock-batch-events-dlq)
  - SQS main queue (bedrock-batch-events) with DLQ redrive policy
  - EventBridge rule matching Bedrock batch job state changes
  - EventBridge target routing events to the SQS queue
  - Queue policy allowing EventBridge to publish

Idempotent: safe to run repeatedly. Existing resources are updated in place.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bedrock_batch import make_session, print_identity
from bedrock_batch.events import DEFAULT_QUEUE_NAME, DEFAULT_RULE_NAME, EVENT_PATTERN

PROJECT_ROOT = Path(__file__).resolve().parent
DLQ_NAME = f"{DEFAULT_QUEUE_NAME}-dlq"
MAX_RECEIVE_COUNT = 3
MESSAGE_RETENTION_SECONDS = 1_209_600  # 14 days


def setup_region(session, region: str) -> dict:
    """Provision the full event pipeline in one region. Returns resource ARNs."""
    sqs = session.client("sqs", region_name=region)
    events = session.client("events", region_name=region)

    # 1. Dead-letter queue
    dlq_url = sqs.create_queue(
        QueueName=DLQ_NAME,
        Attributes={"MessageRetentionPeriod": str(MESSAGE_RETENTION_SECONDS)},
    )["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    print(f"  DLQ ready: {dlq_url}")

    # 2. Main queue with redrive to DLQ
    redrive_policy = json.dumps({
        "deadLetterTargetArn": dlq_arn,
        "maxReceiveCount": str(MAX_RECEIVE_COUNT),
    })
    queue_url = sqs.create_queue(
        QueueName=DEFAULT_QUEUE_NAME,
        Attributes={
            "MessageRetentionPeriod": str(MESSAGE_RETENTION_SECONDS),
            "RedrivePolicy": redrive_policy,
        },
    )["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    print(f"  Queue ready: {queue_url}")

    # 3. EventBridge rule
    rule_arn = events.put_rule(
        Name=DEFAULT_RULE_NAME,
        EventPattern=json.dumps(EVENT_PATTERN),
        State="ENABLED",
        Description="Bedrock batch inference job state-change events",
    )["RuleArn"]
    print(f"  Rule ready: {rule_arn}")

    # 4. Queue policy — allow EventBridge to send messages
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "AllowEventBridgeSend",
            "Effect": "Allow",
            "Principal": {"Service": "events.amazonaws.com"},
            "Action": "sqs:SendMessage",
            "Resource": queue_arn,
            "Condition": {"ArnEquals": {"aws:SourceArn": rule_arn}},
        }],
    }
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(policy)},
    )
    print(f"  Queue policy set (EventBridge -> SQS)")

    # 5. Wire rule to queue
    events.put_targets(
        Rule=DEFAULT_RULE_NAME,
        Targets=[{"Id": "batch-queue", "Arn": queue_arn}],
    )
    print(f"  Target wired: rule -> queue")

    return {
        "region": region,
        "queue_url": queue_url,
        "queue_arn": queue_arn,
        "dlq_url": dlq_url,
        "dlq_arn": dlq_arn,
        "rule_arn": rule_arn,
    }


def validate_region(session, region: str, resources: dict) -> bool:
    """Verify the pipeline is functional."""
    sqs = session.client("sqs", region_name=region)
    events = session.client("events", region_name=region)
    ok = True

    # Check rule exists and is enabled
    try:
        rule = events.describe_rule(Name=DEFAULT_RULE_NAME)
        if rule["State"] != "ENABLED":
            print(f"  WARNING: rule exists but State={rule['State']}")
            ok = False
    except Exception as e:
        print(f"  ERROR: rule not found: {e}")
        ok = False

    # Check targets
    try:
        targets = events.list_targets_by_rule(Rule=DEFAULT_RULE_NAME)
        target_arns = [t["Arn"] for t in targets.get("Targets", [])]
        if resources["queue_arn"] not in target_arns:
            print(f"  WARNING: queue not in rule targets")
            ok = False
    except Exception as e:
        print(f"  ERROR: could not list targets: {e}")
        ok = False

    # Check queue attributes
    try:
        attrs = sqs.get_queue_attributes(
            QueueUrl=resources["queue_url"],
            AttributeNames=["QueueArn", "RedrivePolicy", "Policy"],
        )["Attributes"]
        if "RedrivePolicy" not in attrs:
            print(f"  WARNING: no redrive policy on main queue")
            ok = False
        if "Policy" not in attrs:
            print(f"  WARNING: no resource policy on main queue")
            ok = False
    except Exception as e:
        print(f"  ERROR: could not read queue attributes: {e}")
        ok = False

    return ok


def main() -> int:
    cfg_path = PROJECT_ROOT / "benchmark" / "config.yaml"
    with cfg_path.open() as fh:
        cfg = yaml.safe_load(fh)

    session = make_session(cfg.get("aws_profile"))
    print_identity(session, label="setup_events")
    print()

    regions = cfg["regions"]
    all_resources = []

    for region in regions:
        print(f"Setting up {region}:")
        resources = setup_region(session, region)
        all_resources.append(resources)
        print()

    print("Validating...")
    all_ok = True
    for resources in all_resources:
        region = resources["region"]
        print(f"  {region}:")
        if validate_region(session, region, resources):
            print(f"    OK")
        else:
            all_ok = False
            print(f"    ISSUES FOUND (see above)")
    print()

    print("=" * 60)
    print("Summary")
    print("=" * 60)
    for r in all_resources:
        print(f"\n  {r['region']}:")
        print(f"    Queue URL:  {r['queue_url']}")
        print(f"    Queue ARN:  {r['queue_arn']}")
        print(f"    DLQ URL:    {r['dlq_url']}")
        print(f"    DLQ ARN:    {r['dlq_arn']}")
        print(f"    Rule ARN:   {r['rule_arn']}")

    print(f"\nEvent pattern: {json.dumps(EVENT_PATTERN, indent=2)}")
    print(f"\nDLQ max receive count: {MAX_RECEIVE_COUNT}")
    print(f"Message retention: {MESSAGE_RETENTION_SECONDS // 86400} days")

    if all_ok:
        print("\nAll regions validated successfully.")
    else:
        print("\nSome regions had validation issues — check output above.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
