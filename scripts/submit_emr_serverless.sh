#!/usr/bin/env bash
# Submit the batch pipeline to EMR Serverless.
set -euo pipefail
source .env

aws emr-serverless start-job-run \
  --application-id "${TRADELENS_EMR_APP_ID}" \
  --execution-role-arn "${TRADELENS_EMR_JOB_ROLE_ARN}" \
  --name "tradelens-batch-$(date +%Y%m%d-%H%M%S)" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"s3://${TRADELENS_CODE_BUCKET}/code/run_pipeline.py\",
      \"sparkSubmitParameters\": \"--py-files s3://${TRADELENS_CODE_BUCKET}/code/tradelens_src.zip --conf spark.jars.packages=io.delta:delta-spark_2.12:3.2.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog\"
    }
  }"
echo ">> Submitted. Track status in the EMR Serverless console; the app scales to zero when idle."
