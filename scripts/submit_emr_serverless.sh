#!/usr/bin/env bash
# Submit the batch pipeline to EMR Serverless.
set -euo pipefail
source .env
export AWS_PROFILE="${AWS_PROFILE:?Set AWS_PROFILE in .env}"

# --files stages config.yaml into the job's flat working directory (no
# subdirectory structure survives staging), so TRADELENS_CONFIG_PATH points
# common/config.py at the bare filename instead of the local "config/config.yaml"
# default. TRADELENS_ENV=aws + the bucket names switch config.yaml onto its
# s3a:// paths (see config.yaml's paths.aws block).
#
# --jars (not spark.jars.packages): EMR Serverless applications have no
# internet egress by default, so resolving io.delta:delta-spark_2.12:3.2.0
# from Maven Central at job-submit time times out and fails the job. The
# same three jars local Ivy resolution needs (delta-spark, delta-storage,
# antlr4-runtime — see common/spark_session.py) are pre-staged to S3 instead.
JARS="s3://${TRADELENS_CODE_BUCKET}/code/jars/delta-spark_2.12-3.2.0.jar"
JARS+=",s3://${TRADELENS_CODE_BUCKET}/code/jars/delta-storage-3.2.0.jar"
JARS+=",s3://${TRADELENS_CODE_BUCKET}/code/jars/antlr4-runtime-4.9.3.jar"

PY_FILES="s3://${TRADELENS_CODE_BUCKET}/code/tradelens_src.zip"
PY_FILES+=",s3://${TRADELENS_CODE_BUCKET}/code/python_deps.zip"

SPARK_SUBMIT_PARAMS="--files s3://${TRADELENS_CODE_BUCKET}/code/config.yaml"
SPARK_SUBMIT_PARAMS+=" --py-files ${PY_FILES}"
SPARK_SUBMIT_PARAMS+=" --jars ${JARS}"
SPARK_SUBMIT_PARAMS+=" --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
SPARK_SUBMIT_PARAMS+=" --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
SPARK_SUBMIT_PARAMS+=" --conf spark.emr-serverless.driverEnv.TRADELENS_ENV=aws"
SPARK_SUBMIT_PARAMS+=" --conf spark.emr-serverless.driverEnv.TRADELENS_CONFIG_PATH=config.yaml"
SPARK_SUBMIT_PARAMS+=" --conf spark.emr-serverless.driverEnv.TRADELENS_RAW_BUCKET=${TRADELENS_RAW_BUCKET}"
SPARK_SUBMIT_PARAMS+=" --conf spark.emr-serverless.driverEnv.TRADELENS_CURATED_BUCKET=${TRADELENS_CURATED_BUCKET}"
# Explicit sizing, kept under this account's EMR Serverless vCPU quota
# ("Max concurrent vCPUs per account" — check with:
#   aws service-quotas list-service-quotas --service-code emr-serverless
# ours is 16 in ap-south-1 as of 2026-09-05, quota increase to 64 requested
# but still pending). driver(4) + 2*executor(4) = 12, leaving headroom.
# spark.executor.instances only sets the INITIAL count under dynamic
# allocation (on by default here) — without an explicit max, Spark scaled
# past it once real work (the bronze write, needing shuffle.partitions=64's
# worth of tasks — that base config is sized for a real cluster, not this
# quota) started, re-hit the same ServiceQuotaExceededException loop, and
# that cascaded into the whole SparkContext shutting down mid-write. Capping
# maxExecutors and cutting shuffle.partitions down to match this capacity
# fixes both: never asks for more than the quota, and doesn't over-partition
# for the cores actually available.
SPARK_SUBMIT_PARAMS+=" --conf spark.driver.cores=4 --conf spark.driver.memory=16g"
SPARK_SUBMIT_PARAMS+=" --conf spark.executor.cores=4 --conf spark.executor.memory=16g"
SPARK_SUBMIT_PARAMS+=" --conf spark.executor.instances=2"
# EMR Serverless defaults spark.dynamicAllocation.initialExecutors to 3
# regardless of spark.executor.instances (confirmed via stateDetails on a
# failed run: "...initialExecutors: 3 must be between...maxExecutors 2") —
# override it explicitly or it conflicts with maxExecutors below.
SPARK_SUBMIT_PARAMS+=" --conf spark.dynamicAllocation.initialExecutors=2"
SPARK_SUBMIT_PARAMS+=" --conf spark.dynamicAllocation.maxExecutors=2"
SPARK_SUBMIT_PARAMS+=" --conf spark.sql.shuffle.partitions=16"

# Logs to a bucket we can read via CLI, not just EMR's browser-only managed
# dashboard — so a run can be watched (aws s3 sync + grep) instead of
# blind-polling job state with no visibility into whether it's stuck.
CONFIG_OVERRIDES="{\"monitoringConfiguration\":{\"s3MonitoringConfiguration\":{\"logUri\":\"s3://${TRADELENS_CURATED_BUCKET}/emr-logs/\"}}}"

RESULT=$(aws emr-serverless start-job-run \
  --application-id "${TRADELENS_EMR_APP_ID}" \
  --execution-role-arn "${TRADELENS_EMR_JOB_ROLE_ARN}" \
  --region "${AWS_REGION}" \
  --name "tradelens-batch-$(date +%Y%m%d-%H%M%S)" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"s3://${TRADELENS_CODE_BUCKET}/code/run_pipeline.py\",
      \"sparkSubmitParameters\": \"${SPARK_SUBMIT_PARAMS}\"
    }
  }" \
  --configuration-overrides "${CONFIG_OVERRIDES}")

echo "${RESULT}"
JOB_RUN_ID=$(echo "${RESULT}" | python -c "import sys,json; print(json.load(sys.stdin)['jobRunId'])")
echo "${JOB_RUN_ID}" > .last_job_run_id
echo ">> Submitted (jobRunId=${JOB_RUN_ID}, saved to .last_job_run_id)."
echo ">> Logs will appear under s3://${TRADELENS_CURATED_BUCKET}/emr-logs/applications/${TRADELENS_EMR_APP_ID}/jobs/${JOB_RUN_ID}/"
echo ">> The app scales to zero when idle."
