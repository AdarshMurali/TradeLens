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

# EMR Serverless applications created WITHOUT an explicit VPC use AWS-managed
# default networking — on this account/region that networking could not reach
# S3 at all: spark.read.parquet() hung indefinitely (10+ minutes, confirmed
# down to a single fully-qualified file, ruling out partition-discovery
# overhead) with no error, just silence. Explicit VPC networking + an S3
# gateway endpoint fixed it completely (same read: ~5 seconds). Using the
# account's default VPC/subnets/security group rather than requiring the user
# to provision a dedicated one for a portfolio project.
echo ">> Finding default VPC for EMR Serverless networking"
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" --region "${AWS_REGION}" \
  --query 'Vpcs[0].VpcId' --output text)
if [ -z "${VPC_ID}" ] || [ "${VPC_ID}" = "None" ]; then
  echo "   No default VPC found — create one (or a VPC of your own with an S3"
  echo "   gateway endpoint) and adapt this script before continuing." >&2
  exit 1
fi
SUBNET_IDS=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=${VPC_ID}" --region "${AWS_REGION}" \
  --query 'Subnets[].SubnetId' --output text | tr '\t' ',')
SG_ID=$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=${VPC_ID}" "Name=group-name,Values=default" \
  --region "${AWS_REGION}" --query 'SecurityGroups[0].GroupId' --output text)
echo "   VPC=${VPC_ID} subnets=${SUBNET_IDS} sg=${SG_ID}"

echo ">> Ensuring an S3 gateway VPC endpoint exists"
EXISTING_ENDPOINT=$(aws ec2 describe-vpc-endpoints --region "${AWS_REGION}" \
  --filters "Name=vpc-id,Values=${VPC_ID}" "Name=service-name,Values=com.amazonaws.${AWS_REGION}.s3" \
  --query 'VpcEndpoints[0].VpcEndpointId' --output text)
if [ -n "${EXISTING_ENDPOINT}" ] && [ "${EXISTING_ENDPOINT}" != "None" ]; then
  echo "   (already exists: ${EXISTING_ENDPOINT})"
else
  MAIN_ROUTE_TABLE=$(aws ec2 describe-route-tables --filters "Name=vpc-id,Values=${VPC_ID}" "Name=association.main,Values=true" \
    --region "${AWS_REGION}" --query 'RouteTables[0].RouteTableId' --output text)
  aws ec2 create-vpc-endpoint --vpc-id "${VPC_ID}" --service-name "com.amazonaws.${AWS_REGION}.s3" \
    --route-table-ids "${MAIN_ROUTE_TABLE}" --vpc-endpoint-type Gateway --region "${AWS_REGION}" >/dev/null
  echo "   Created on route table ${MAIN_ROUTE_TABLE}."
fi

echo ">> Creating EMR Serverless application '${TRADELENS_EMR_APP_NAME}'"
EXISTING_APP_ID=$(aws emr-serverless list-applications --region "${AWS_REGION}" \
  --query "applications[?name=='${TRADELENS_EMR_APP_NAME}' && state!='TERMINATED'].id | [0]" --output text)
if [ -n "${EXISTING_APP_ID}" ] && [ "${EXISTING_APP_ID}" != "None" ]; then
  APP_ID="${EXISTING_APP_ID}"
  echo "   (already exists: ${APP_ID})"
else
  SUBNET_IDS_JSON=$(python -c "import sys,json; print(json.dumps(sys.argv[1].split(',')))" "${SUBNET_IDS}")
  NETWORK_CONFIG="{\"subnetIds\":${SUBNET_IDS_JSON},\"securityGroupIds\":[\"${SG_ID}\"]}"
  APP_ID=$(aws emr-serverless create-application \
    --name "${TRADELENS_EMR_APP_NAME}" \
    --type SPARK \
    --release-label "${TRADELENS_EMR_RELEASE_LABEL}" \
    --network-configuration "${NETWORK_CONFIG}" \
    --region "${AWS_REGION}" \
    --query 'applicationId' --output text)
  echo "   Created: ${APP_ID}"
fi

echo ">> Writing role ARN + application ID into .env"
sed -i.bak "s#^TRADELENS_EMR_APP_ID=.*#TRADELENS_EMR_APP_ID=${APP_ID}#" .env
sed -i.bak "s#^TRADELENS_EMR_JOB_ROLE_ARN=.*#TRADELENS_EMR_JOB_ROLE_ARN=${ROLE_ARN}#" .env
rm -f .env.bak

echo ">> Done. Buckets, Glue DB, IAM role, and EMR Serverless app (\$0 while idle) are ready."
