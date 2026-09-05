#!/usr/bin/env bash
# Sync the local raw dataset (from `make ingest`) to the raw S3 bucket, for
# the batch job to read on EMR Serverless. Run after setup_aws.sh.
set -euo pipefail
source .env
export AWS_PROFILE="${AWS_PROFILE:?Set AWS_PROFILE in .env}"

echo ">> Syncing data/raw -> s3://${TRADELENS_RAW_BUCKET}/raw"
aws s3 sync data/raw "s3://${TRADELENS_RAW_BUCKET}/raw" --region "${AWS_REGION}"
echo ">> Done."
