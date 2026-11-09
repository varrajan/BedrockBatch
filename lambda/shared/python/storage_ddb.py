"""DynamoDB storage layer — replaces the SQLite reference layer for Lambda.

Three tables mirror the SQLite schema: Jobs, Events, Fallbacks.
Table names are read from environment variables so the SAM template
controls naming.
"""

from __future__ import annotations

import os
import time
from decimal import Decimal
from typing import Any

import boto3

JOBS_TABLE = os.environ.get("JOBS_TABLE", "bedrock-batch-jobs")
EVENTS_TABLE = os.environ.get("EVENTS_TABLE", "bedrock-batch-events")
FALLBACKS_TABLE = os.environ.get("FALLBACKS_TABLE", "bedrock-batch-fallbacks")

_ddb = None


def _resource():
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb")
    return _ddb


def _sanitize(item: dict) -> dict:
    """DynamoDB doesn't accept float — convert to Decimal. Drop None values."""
    out = {}
    for k, v in item.items():
        if v is None:
            continue
        if isinstance(v, float):
            out[k] = Decimal(str(v))
        else:
            out[k] = v
    return out


def _deserialize(item: dict) -> dict:
    """Convert Decimals back to int/float for downstream consumers."""
    out = {}
    for k, v in item.items():
        if isinstance(v, Decimal):
            out[k] = int(v) if v == int(v) else float(v)
        else:
            out[k] = v
    return out


def record_job(row: dict) -> None:
    table = _resource().Table(JOBS_TABLE)
    table.put_item(Item=_sanitize(row))


def update_job(job_id: str, updates: dict) -> None:
    table = _resource().Table(JOBS_TABLE)
    expr_parts = []
    values = {}
    names = {}
    for i, (k, v) in enumerate(updates.items()):
        attr = f"#a{i}"
        val = f":v{i}"
        expr_parts.append(f"{attr} = {val}")
        names[attr] = k
        if isinstance(v, float):
            values[val] = Decimal(str(v))
        else:
            values[val] = v
    table.update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET " + ", ".join(expr_parts),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def get_job(job_id: str) -> dict | None:
    table = _resource().Table(JOBS_TABLE)
    resp = table.get_item(Key={"job_id": job_id})
    item = resp.get("Item")
    return _deserialize(item) if item else None


def all_jobs() -> list[dict]:
    table = _resource().Table(JOBS_TABLE)
    items = []
    resp = table.scan()
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))
    return [_deserialize(i) for i in items]


def record_event(row: dict) -> None:
    table = _resource().Table(EVENTS_TABLE)
    if "id" not in row:
        row = {**row, "id": f"{row['job_id']}#{row.get('event_time', time.time())}"}
    table.put_item(Item=_sanitize(row))


def all_events() -> list[dict]:
    table = _resource().Table(EVENTS_TABLE)
    items = []
    resp = table.scan()
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))
    return [_deserialize(i) for i in items]


def record_fallback(row: dict) -> None:
    table = _resource().Table(FALLBACKS_TABLE)
    if "id" not in row:
        row = {**row, "id": f"{row['job_id']}#{row.get('triggered_at', time.time())}"}
    table.put_item(Item=_sanitize(row))


def fallback_exists(job_id: str) -> bool:
    table = _resource().Table(FALLBACKS_TABLE)
    resp = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("job_id").eq(job_id),
        Limit=1,
    )
    return len(resp.get("Items", [])) > 0


def all_fallbacks() -> list[dict]:
    table = _resource().Table(FALLBACKS_TABLE)
    items = []
    resp = table.scan()
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))
    return [_deserialize(i) for i in items]
