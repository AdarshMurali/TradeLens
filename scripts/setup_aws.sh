#!/usr/bin/env bash
# One-time AWS foundation for TradeLens. Sets the billing alarm FIRST, then
# creates S3 buckets, the Glue database, the EMR Serverless job role, and the
# EMR Serverless application. Idempotent: safe to re-run.
#
# Requires: aws cli configured with the profile named in .env's AWS_PROFILE.
set -euo pipefail
source .env
export AWS_PROFILE="${AWS_PROFILE:?Set AWS_PROFILE in .env}"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo ">> AWS profile '${AWS_PROFILE}' -> account ${ACCOUNT_ID}, region ${AWS_REGION}"

echo ">> Billing alarm: ${TRADELENS_BUDGET_NAME} (\$${TRADELENS_BUDGET_LIMIT_USD}/month)"
if [ -z "${TRADELENS_BILLING_ALERT_EMAIL}" ]; then
  echo "   TRADELENS_BILLING_ALERT_EMAIL is empty in .env - skipping. Set it and re-run, or create the budget in the console (infra/README.md SS0)."
else
  BUDGET_JSON=$(cat <<JSON
{"BudgetName":"${TRADELENS_BUDGET_NAME}","BudgetLimit":{"Amount":"${TRADELENS_BUDGET_LIMIT_USD}","Unit":"USD"},"TimeUnit":"MONTHLY","BudgetType":"COST"}
JSON
)
  NOTIFICATIONS_JSON=$(cat <<JSON
[
  {"Notification":{"NotificationType":"ACTUAL","ComparisonOperator":"GREATER_THAN","Threshold":80},"Subscribers":[{"SubscriptionType":"EMAIL","Address":"${TRADELENS_BILLING_ALERT_EMAIL}"}]},
  {"Notification":{"NotificationType":"ACTUAL","ComparisonOperator":"GREATER_THAN","Threshold":100},"Subscribers":[{"SubscriptionType":"EMAIL","Address":"${TRADELENS_BILLING_ALERT_EMAIL}"}]}
]
JSON
)
  aws budgets create-budget \
    --account-id "${ACCOUNT_ID}" \
    --budget "${BUDGET_JSON}" \
    --notifications-with-subscribers "${NOTIFICATIONS_JSON}" \
    2>/dev/null && echo "   Created." || echo "   (already exists)"
fi

echo ">> Creating S3 buckets"
aws s3 mb "s3://${TRADELENS_RAW_BUCKET}"     --region "${AWS_REGION}" || true
aws s3 mb "s3://${TRADELENS_CURATED_BUCKET}" --region "${AWS_REGION}" || true
aws s3 mb "s3://${TRADELENS_CODE_BUCKET}"    --region "${AWS_REGION}" || true

echo ">> Creating Glue database"
aws glue create-database --database-input "Name=${TRADELENS_GLUE_DB}" --region "${AWS_REGION}" \
  2>/dev/null && echo "   Created." || echo "   (already exists)"

echo ">> Creating IAM role ${TRADELENS_EMR_JOB_ROLE_NAME}"
TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"emr-serverless.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
if aws iam get-role --role-name "${TRADELENS_EMR_JOB_ROLE_NAME}" >/dev/null 2>&1; then
  echo "   (already exists)"
else
  aws iam create-role \
    --role-name "${TRADELENS_EMR_JOB_ROLE_NAME}" \
    --assume-role-policy-document "${TRUST_POLICY}" \
    --description "TradeLens EMR Serverless job execution role" >/dev/null
  echo "   Created."
fi

PERMISSIONS_POLICY=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadRawAndCode",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${TRADELENS_RAW_BUCKET}", "arn:aws:s3:::${TRADELENS_RAW_BUCKET}/*",
        "arn:aws:s3:::${TRADELENS_CODE_BUCKET}", "arn:aws:s3:::${TRADELENS_CODE_BUCKET}/*"
      ]
    },
    {
      "Sid": "ReadWriteCurated",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${TRADELENS_CURATED_BUCKET}", "arn:aws:s3:::${TRADELENS_CURATED_BUCKET}/*"
      ]
    },
    {
      "Sid": "GlueCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
        "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
        "glue:GetPartition", "glue:GetPartitions", "glue:CreatePartition", "glue:BatchCreatePartition"
      ],
      "Resource": "*"
    }
  ]
}
JSON
)
aws iam put-role-policy \
  --role-name "${TRADELENS_EMR_JOB_ROLE_NAME}" \
  --policy-name "${TRADELENS_EMR_JOB_ROLE_NAME}-policy" \
  --policy-document "${PERMISSIONS_POLICY}"
ROLE_ARN=$(aws iam get-role --role-name "${TRADELENS_EMR_JOB_ROLE_NAME}" --query 'Role.Arn' --output text)
echo "   Role ARN: ${ROLE_ARN}"

echo ">> Creating EMR Serverless application '${TRADELENS_EMR_APP_NAME}'"
EXISTING_APP_ID=$(aws emr-serverless list-applications --region "${AWS_REGION}" \
  --query "applications[?name=='${TRADELENS_EMR_APP_NAME}' && state!='TERMINATED'].id | [0]" --output text)
if [ -n "${EXISTING_APP_ID}" ] && [ "${EXISTING_APP_ID}" != "None" ]; then
  APP_ID="${EXISTING_APP_ID}"
  echo "   (already exists: ${APP_ID})"
else
  APP_ID=$(aws emr-serverless create-application \
    --name "${TRADELENS_EMR_APP_NAME}" \
    --type SPARK \
    --release-label "${TRADELENS_EMR_RELEASE_LABEL}" \
    --region "${AWS_REGION}" \
    --query 'applicationId' --output text)
  echo "   Created: ${APP_ID}"
fi

echo ">> Writing role ARN + application ID into .env"
sed -i.bak "s#^TRADELENS_EMR_APP_ID=.*#TRADELENS_EMR_APP_ID=${APP_ID}#" .env
sed -i.bak "s#^TRADELENS_EMR_JOB_ROLE_ARN=.*#TRADELENS_EMR_JOB_ROLE_ARN=${ROLE_ARN}#" .env
rm -f .env.bak

echo ">> Done. Buckets, Glue DB, IAM role, and EMR Serverless app (\$0 while idle) are ready."
