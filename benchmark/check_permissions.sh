#!/bin/bash
# Check whether the current AWS identity has the permissions the benchmark
# needs. Uses iam:simulate-principal-policy so nothing is created.
#
# Run from a shell where `aws sts get-caller-identity` returns the identity
# you plan to use for the benchmark (your terminal, not Claude Code's).

set -euo pipefail

PRINCIPAL_ARN=$(aws sts get-caller-identity --query Arn --output text)
echo "Principal: $PRINCIPAL_ARN"
echo

# IAM policy simulator wants a user/role ARN, not an assumed-role session ARN.
# Convert sts::.../assumed-role/<role>/<session> -> iam::.../role/<role>.
if [[ "$PRINCIPAL_ARN" == *":assumed-role/"* ]]; then
  ACCOUNT=$(echo "$PRINCIPAL_ARN" | cut -d: -f5)
  ROLE=$(echo "$PRINCIPAL_ARN" | sed -E 's|.*:assumed-role/([^/]+)/.*|\1|')
  SIM_ARN="arn:aws:iam::${ACCOUNT}:role/${ROLE}"
  echo "Simulating as role: $SIM_ARN"
else
  SIM_ARN="$PRINCIPAL_ARN"
fi
echo

ACTIONS=(
  # Bedrock submission + polling
  "bedrock:CreateModelInvocationJob"
  "bedrock:GetModelInvocationJob"
  "bedrock:InvokeModel"
  # S3 (bucket setup + input/output)
  "s3:CreateBucket"
  "s3:HeadBucket"
  "s3:GetObject"
  "s3:PutObject"
  "s3:ListBucket"
  # IAM (only needed on first run to create the Bedrock-assumable role)
  "iam:CreateRole"
  "iam:GetRole"
  "iam:PutRolePolicy"
  "iam:PassRole"
  # SQS (events pipeline)
  "sqs:CreateQueue"
  "sqs:GetQueueAttributes"
  "sqs:SetQueueAttributes"
  "sqs:ReceiveMessage"
  "sqs:DeleteMessage"
  # EventBridge (events pipeline)
  "events:PutRule"
  "events:PutTargets"
  # STS
  "sts:GetCallerIdentity"
)

printf "%-45s %s\n" "ACTION" "DECISION"
printf "%-45s %s\n" "------" "--------"

for action in "${ACTIONS[@]}"; do
  decision=$(aws iam simulate-principal-policy \
    --policy-source-arn "$SIM_ARN" \
    --action-names "$action" \
    --query 'EvaluationResults[0].EvalDecision' \
    --output text 2>&1 || echo "ERROR")
  if [[ "$decision" == "allowed" ]]; then
    printf "%-45s \033[32m%s\033[0m\n" "$action" "$decision"
  else
    printf "%-45s \033[31m%s\033[0m\n" "$action" "$decision"
  fi
done
