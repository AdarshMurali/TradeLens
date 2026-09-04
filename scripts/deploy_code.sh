#!/usr/bin/env bash
# Package src/ + config and sync to the code bucket for EMR Serverless.
set -euo pipefail
source .env

echo ">> Zipping source"
( cd src && zip -r ../tradelens_src.zip tradelens >/dev/null )

echo ">> Uploading to s3://${TRADELENS_CODE_BUCKET}/"
aws s3 cp tradelens_src.zip "s3://${TRADELENS_CODE_BUCKET}/code/tradelens_src.zip"
aws s3 cp src/tradelens/jobs/run_pipeline.py "s3://${TRADELENS_CODE_BUCKET}/code/run_pipeline.py"
aws s3 cp config/config.yaml "s3://${TRADELENS_CODE_BUCKET}/code/config.yaml"
echo ">> Done."
