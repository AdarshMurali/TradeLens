#!/usr/bin/env bash
# One-time AWS foundation for TradeLens. SET THE BILLING ALARM FIRST (infra/README.md).
# Requires: aws cli configured. Loads names from .env.
set -euo pipefail
source .env

echo ">> Creating S3 buckets"
aws s3 mb "s3://${TRADELENS_RAW_BUCKET}"     --region "${AWS_REGION}" || true
aws s3 mb "s3://${TRADELENS_CURATED_BUCKET}" --region "${AWS_REGION}" || true
aws s3 mb "s3://${TRADELENS_CODE_BUCKET}"    --region "${AWS_REGION}" || true

echo ">> Creating Glue database"
aws glue create-database --database-input "Name=${TRADELENS_GLUE_DB}" || true

echo ">> TODO: create IAM role ${TRADELENS_EMR_JOB_ROLE_ARN##*/} with S3 + Glue access"
echo ">> TODO: create EMR Serverless application (type=SPARK) and put its id in .env"
echo "   aws emr-serverless create-application --name tradelens --type SPARK --release-label emr-7.1.0"
echo ">> Done. Verify billing alarm exists before running jobs."
