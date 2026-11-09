"""EventBridge → SQS pipeline for Bedrock batch state-change events.

Bedrock emits events on the default event bus whenever a batch job
changes state:

    {
      "source": "aws.bedrock",
      "detail-type": "Batch Inference Job State Change",
      "detail": {
        "accountId": "123456789012",
        "version": "0.0",
        "batchJobName": "my-batch-job",
        "batchJobArn": "arn:aws:bedrock:us-east-1:...:model-invocation-job/<id>",
        "batchModelId": "arn:aws:bedrock:us-east-1:...:inference-profile/...",
        "status": "Failed",
        "creationTime": "Nov 2, 2026, 6:57:44 PM"
      },
      "time": "2026-10-18T12:34:56Z",
      ...
    }

Polling misses failures that happen between polls or during a watcher
outage. An EventBridge rule routed to SQS gives durable push
notifications you drain at your own pace.

Public API:
    ensure_event_pipeline(session, region, queue_name=..., rule_name=...)
        -> {queue_url, queue_arn, rule_arn}
    consume_events(session, region, queue_url, handler, stop_when_idle=False)
        -> int (events processed)
"""

from __future__ import annotations

import json
from typing import Callable

DEFAULT_QUEUE_NAME = "bedrock-batch-events"
DEFAULT_RULE_NAME = "bedrock-batch-rule"

EVENT_PATTERN = {
    "source": ["aws.bedrock"],
    "detail-type": ["Batch Inference Job State Change"],
}

TERMINAL_STATUSES = {"Completed", "Failed", "Stopped", "Expired", "PartiallyCompleted"}
RECOVERABLE_STATUSES = {"Failed", "Stopped", "Expired", "PartiallyCompleted"}


def ensure_event_pipeline(
    session,
    region: str,
    queue_name: str = DEFAULT_QUEUE_NAME,
    rule_name: str = DEFAULT_RULE_NAME,
    message_retention_seconds: int = 1_209_600,  # 14 days
) -> dict:
    """Idempotently create the rule, queue, and target in `region`.

    Returns {"queue_url": ..., "queue_arn": ..., "rule_arn": ...}.
    """
    sqs = session.client("sqs", region_name=region)
    events = session.client("events", region_name=region)

    # 1. SQS queue (14-day retention by default)
    queue_url = sqs.create_queue(
        QueueName=queue_name,
        Attributes={"MessageRetentionPeriod": str(message_retention_seconds)},
    )["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    # 2. EventBridge rule
    rule_arn = events.put_rule(
        Name=rule_name,
        EventPattern=json.dumps(EVENT_PATTERN),
        State="ENABLED",
        Description="Bedrock batch inference job state-change events",
    )["RuleArn"]

    # 3. Allow EventBridge to publish to this queue
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "AllowEventBridge",
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

    # 4. Wire the rule to the queue
    events.put_targets(
        Rule=rule_name,
        Targets=[{"Id": "batch-queue", "Arn": queue_arn}],
    )

    return {"queue_url": queue_url, "queue_arn": queue_arn, "rule_arn": rule_arn}


def consume_events(
    session,
    region: str,
    queue_url: str,
    handler: Callable[[dict], None],
    max_messages: int = 10,
    wait_seconds: int = 20,
    stop_when_idle: bool = False,
    idle_rounds: int = 2,
) -> int:
    """Long-poll SQS and call `handler(event)` for each decoded event.

    Returns the number of events processed. If `stop_when_idle` is set,
    the loop exits after `idle_rounds` consecutive empty receives --
    useful for one-shot drains inside a batch pipeline.

    If `handler` raises, the message is NOT deleted; SQS visibility
    timeout retries it. Handler failures are logged to stdout.
    """
    sqs = session.client("sqs", region_name=region)
    processed = 0
    empty = 0

    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_seconds,
        )
        messages = resp.get("Messages", [])
        if not messages:
            empty += 1
            if stop_when_idle and empty >= idle_rounds:
                return processed
            continue
        empty = 0

        for msg in messages:
            try:
                event = json.loads(msg["Body"])
                handler(event)
                processed += 1
            except Exception as e:
                print(f"  event handler error: {e}")
                continue
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
