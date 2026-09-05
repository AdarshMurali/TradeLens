#!/usr/bin/env bash
# Actively watch an EMR Serverless job: prints job state changes and any new
# driver stdout/stderr lines as they appear in S3, instead of blind-polling
# job state with no visibility into whether it's actually progressing or
# silently stuck. Requires the job to have been submitted with
# monitoringConfiguration.s3MonitoringConfiguration pointed at a bucket this
# account can read (submit_emr_serverless.sh already sets this up).
#
# Usage: bash scripts/watch_emr_job.sh [job-run-id]
#   Defaults to the job-run-id saved by submit_emr_serverless.sh in
#   .last_job_run_id if none is given.
set -uo pipefail
source .env
export AWS_PROFILE

APP_ID="${TRADELENS_EMR_APP_ID}"
JOB_RUN_ID="${1:-$(cat .last_job_run_id)}"
LOG_PREFIX="s3://${TRADELENS_CURATED_BUCKET}/emr-logs/applications/${APP_ID}/jobs/${JOB_RUN_ID}"
WORKDIR=$(mktemp -d)
LAST_STDOUT_LINES=0
LAST_STDERR_LINES=0
LAST_STATE=""
TICKS=0

echo "Watching jobRunId=${JOB_RUN_ID}"
echo "Log prefix: ${LOG_PREFIX}"

while true; do
  STATE=$(aws emr-serverless get-job-run --application-id "$APP_ID" --job-run-id "$JOB_RUN_ID" \
    --region "$AWS_REGION" --query 'jobRun.state' --output text 2>/dev/null)

  if [ "$STATE" != "$LAST_STATE" ]; then
    echo "$(date +%H:%M:%S) STATE CHANGE: ${LAST_STATE:-<none>} -> ${STATE}"
    LAST_STATE="$STATE"
  fi

  for STREAM in SPARK_DRIVER/stdout SPARK_DRIVER/stderr; do
    aws s3 cp "${LOG_PREFIX}/${STREAM}.gz" "${WORKDIR}/$(basename "$STREAM").gz" \
      --region "$AWS_REGION" >/dev/null 2>&1
    if [ -f "${WORKDIR}/$(basename "$STREAM").gz" ]; then
      gunzip -kf "${WORKDIR}/$(basename "$STREAM").gz" 2>/dev/null
      TOTAL_LINES=$(wc -l < "${WORKDIR}/$(basename "$STREAM")" 2>/dev/null || echo 0)
      if [ "$STREAM" = "SPARK_DRIVER/stdout" ]; then
        if [ "$TOTAL_LINES" -gt "$LAST_STDOUT_LINES" ]; then
          echo "--- new stdout (lines $((LAST_STDOUT_LINES+1))-${TOTAL_LINES}) ---"
          tail -n "+$((LAST_STDOUT_LINES+1))" "${WORKDIR}/$(basename "$STREAM")"
          LAST_STDOUT_LINES=$TOTAL_LINES
        fi
      else
        if [ "$TOTAL_LINES" -gt "$LAST_STDERR_LINES" ]; then
          echo "--- new stderr (lines $((LAST_STDERR_LINES+1))-${TOTAL_LINES}), showing errors/warnings only ---"
          tail -n "+$((LAST_STDERR_LINES+1))" "${WORKDIR}/$(basename "$STREAM")" | grep -iE "error|exception|warn" | tail -20
          LAST_STDERR_LINES=$TOTAL_LINES
        fi
      fi
    fi
  done

  TICKS=$((TICKS+1))
  if [ $((TICKS % 4)) -eq 0 ]; then
    echo "$(date +%H:%M:%S) heartbeat: state=${STATE}, stdout_lines=${LAST_STDOUT_LINES}"
  fi

  if [[ "$STATE" =~ ^(SUCCESS|FAILED|CANCELLED)$ ]]; then
    echo "$(date +%H:%M:%S) FINAL STATE: ${STATE}"
    break
  fi
  sleep 30
done
