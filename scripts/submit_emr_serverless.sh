#!/usr/bin/env bash
# Submit the batch pipeline to EMR Serverless, using the application's
# current custom image (see Dockerfile + scripts/deploy_docker.sh /
# .github/workflows/deploy.yml, which builds it and points the application
# at it via `aws emr-serverless update-application --image-configuration`).
#
# Everything the job needs — Delta's JARs, the delta/yaml/dotenv Python
# packages, the application code and config.yaml — is already baked into
# that image, so unlike earlier revisions of this script there is no
# --jars/--py-files/--files to pass: entryPoint is a path inside the image.
set -euo pipefail
source .env
export AWS_PROFILE="${AWS_PROFILE:?Set AWS_PROFILE in .env}"

# Explicit sizing, kept under this account's EMR Serverless vCPU quota
# ("Max concurrent vCPUs per account" — check with:
#   aws service-quotas list-service-quotas --service-code emr-serverless
# ours is 16 in ap-south-1 as of 2026-09-05, quota increase to 64 requested
# but still pending). driver(4) + 2*executor(4) = 12, leaving headroom.
# initialExecutors must be set explicitly alongside maxExecutors — EMR
# Serverless silently defaults it to 3 regardless of spark.executor.instances,
# which otherwise conflicts with a lower maxExecutors and fails submission.
# shuffle.partitions is cut from the cluster-sized default (64) to match the
# cores actually available, not left at the default sized for a real cluster.
SPARK_SUBMIT_PARAMS="--conf spark.driver.cores=4 --conf spark.driver.memory=16g"
SPARK_SUBMIT_PARAMS+=" --conf spark.executor.cores=4 --conf spark.executor.memory=16g"
SPARK_SUBMIT_PARAMS+=" --conf spark.executor.instances=2"
SPARK_SUBMIT_PARAMS+=" --conf spark.dynamicAllocation.initialExecutors=2"
SPARK_SUBMIT_PARAMS+=" --conf spark.dynamicAllocation.maxExecutors=2"
SPARK_SUBMIT_PARAMS+=" --conf spark.sql.shuffle.partitions=16"

# The bucket names are job/environment-specific, not something to bake into
# the (otherwise portable) image — config.yaml's ${TRADELENS_RAW_BUCKET} /
# ${TRADELENS_CURATED_BUCKET} substitution needs these as real env vars on
# the driver, or it fails immediately: `s3a://${TRADELENS_RAW_BUCKET}/raw`
# (the literal, unexpanded string) -> "IllegalArgumentException: bucket is
# null/empty". Confirmed by trial the first time this got dropped while
# simplifying this script for the custom-image switch.
SPARK_SUBMIT_PARAMS+=" --conf spark.emr-serverless.driverEnv.TRADELENS_RAW_BUCKET=${TRADELENS_RAW_BUCKET}"
SPARK_SUBMIT_PARAMS+=" --conf spark.emr-serverless.driverEnv.TRADELENS_CURATED_BUCKET=${TRADELENS_CURATED_BUCKET}"

# Logs to a bucket we can read via CLI, not just EMR's browser-only managed
# dashboard — see scripts/watch_emr_job.sh. This is what actually surfaced
# the VPC/S3 connectivity issue (silent multi-minute hang, zero errors) that
# blind state-polling alone would never have caught.
CONFIG_OVERRIDES="{\"monitoringConfiguration\":{\"s3MonitoringConfiguration\":{\"logUri\":\"s3://${TRADELENS_CURATED_BUCKET}/emr-logs/\"}}}"

RESULT=$(aws emr-serverless start-job-run \
  --application-id "${TRADELENS_EMR_APP_ID}" \
  --execution-role-arn "${TRADELENS_EMR_JOB_ROLE_ARN}" \
  --region "${AWS_REGION}" \
  --name "tradelens-batch-$(date +%Y%m%d-%H%M%S)" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"/home/hadoop/tradelens/jobs/run_pipeline.py\",
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
