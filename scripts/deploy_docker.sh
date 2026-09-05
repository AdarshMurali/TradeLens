#!/usr/bin/env bash
# Build the TradeLens runtime image and push it to Docker Hub + ECR, then
# point the EMR Serverless application at it. Local equivalent of what
# .github/workflows/deploy.yml does in CI on every push to main.
#
# NOTE: this base image is large (a full EMR Spark/Hadoop distribution) and
# building it is memory-hungry. On a resource-constrained machine, prefer
# pushing to main and letting CI build it instead — that's what this repo's
# CI/CD is actually for. A local build here once visibly starved an 8GB dev
# laptop down to ~60MB free RAM mid-build.
set -euo pipefail
source .env
export AWS_PROFILE="${AWS_PROFILE:?Set AWS_PROFILE in .env}"

DOCKERHUB_IMAGE="adarshmurali/tradelens"
ECR_REPOSITORY="tradelens"
TAG="$(git rev-parse --short HEAD)"

echo ">> Building ${DOCKERHUB_IMAGE}:${TAG}"
docker build -t "${DOCKERHUB_IMAGE}:latest" -t "${DOCKERHUB_IMAGE}:${TAG}" .

echo ">> Pushing to Docker Hub"
docker push "${DOCKERHUB_IMAGE}:latest"
docker push "${DOCKERHUB_IMAGE}:${TAG}"

ECR_REGISTRY=$(aws sts get-caller-identity --query Account --output text).dkr.ecr.${AWS_REGION}.amazonaws.com
echo ">> Logging into ECR (${ECR_REGISTRY})"
aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

echo ">> Tagging + pushing to ECR"
docker tag "${DOCKERHUB_IMAGE}:${TAG}" "${ECR_REGISTRY}/${ECR_REPOSITORY}:${TAG}"
docker push "${ECR_REGISTRY}/${ECR_REPOSITORY}:${TAG}"

echo ">> Pointing the EMR Serverless application at the new image"
aws emr-serverless update-application \
  --application-id "${TRADELENS_EMR_APP_ID}" \
  --image-configuration "imageUri=${ECR_REGISTRY}/${ECR_REPOSITORY}:${TAG}" \
  --region "${AWS_REGION}"

echo ">> Done. ${ECR_REGISTRY}/${ECR_REPOSITORY}:${TAG} is now the application's runtime image."
