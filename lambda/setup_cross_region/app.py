"""CloudFormation custom resource: create EventBridge rules in remote regions
that forward Bedrock batch job state-change events to the stack's region.

On Create/Update: for each region in the Regions list (excluding the stack region),
creates an EventBridge rule + target that forwards matching events to the default
event bus in the stack region.

On Delete: removes those rules from each remote region.
"""

from __future__ import annotations

import json
import urllib.request
import boto3


RULE_NAME_PREFIX = "bedrock-batch-xregion-fwd"


def _send_response(event, context, status, data=None):
    body = json.dumps({
        "Status": status,
        "Reason": f"See CloudWatch Log Stream: {context.log_stream_name}",
        "PhysicalResourceId": context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": data or {},
    })
    req = urllib.request.Request(
        event["ResponseURL"],
        data=body.encode("utf-8"),
        headers={"Content-Type": ""},
        method="PUT",
    )
    urllib.request.urlopen(req)


def handler(event, context):
    try:
        props = event["ResourceProperties"]
        regions = json.loads(props["Regions"])
        stack_region = props["StackRegion"]
        account_id = props["AccountId"]
        role_arn = props["ForwardingRoleArn"]
        request_type = event["RequestType"]

        remote_regions = [r for r in regions if r != stack_region]
        target_bus_arn = f"arn:aws:events:{stack_region}:{account_id}:event-bus/default"
        rule_name = f"{RULE_NAME_PREFIX}-to-{stack_region}"

        if request_type in ("Create", "Update"):
            for region in remote_regions:
                client = boto3.client("events", region_name=region)
                client.put_rule(
                    Name=rule_name,
                    EventPattern=json.dumps({
                        "source": ["aws.bedrock"],
                        "detail-type": ["Batch Inference Job State Change"],
                    }),
                    State="ENABLED",
                    Description=f"Forward Bedrock batch events to {stack_region}",
                )
                client.put_targets(
                    Rule=rule_name,
                    Targets=[{
                        "Id": "cross-region-bus",
                        "Arn": target_bus_arn,
                        "RoleArn": role_arn,
                    }],
                )
                print(f"Created rule {rule_name} in {region} -> {stack_region}")

        elif request_type == "Delete":
            for region in remote_regions:
                client = boto3.client("events", region_name=region)
                try:
                    client.remove_targets(Rule=rule_name, Ids=["cross-region-bus"])
                    client.delete_rule(Name=rule_name)
                    print(f"Deleted rule {rule_name} in {region}")
                except client.exceptions.ResourceNotFoundException:
                    print(f"Rule {rule_name} not found in {region}, skipping")

        _send_response(event, context, "SUCCESS", {
            "RemoteRegions": ",".join(remote_regions),
            "RuleName": rule_name,
        })

    except Exception as e:
        print(f"Error: {e}")
        _send_response(event, context, "FAILED", {"Error": str(e)})
