# Infrastructure & Cost Control

> **Do Phase 6 of the plan only after this checklist. Cost discipline is part of
> the project story.**

## 0. Billing alarm FIRST (AWS Budgets — always free)
- Create a monthly cost budget of **$5** with an alert at 80% and 100%.
  `scripts/setup_aws.sh` does this automatically via `aws budgets create-budget`
  as its first step, reading `TRADELENS_BUDGET_LIMIT_USD` and
  `TRADELENS_BILLING_ALERT_EMAIL` from `.env` — or create it manually:
  Console: Billing → Budgets → Create budget → Cost budget.
- Optionally a $10 hard "zero-spend"-style alert as backup.

## 0.5. Multiple AWS accounts on one machine
If this machine has more than one AWS CLI profile configured (`aws configure
list-profiles`), set `AWS_PROFILE` in `.env` to the correct one for this
project **before running any script in `scripts/`** — every script sources
`.env` and exports `AWS_PROFILE` from it, so nothing here ever falls back to
whatever the ambient default profile happens to be. Verify with
`aws sts get-caller-identity --profile <name>` before the first run.

## 1. Resources this project creates
| Resource | Name | Idle cost |
|---|---|---|
| S3 buckets | `tradelens-raw-*`, `tradelens-curated-*`, `tradelens-code-*` | ~cents/mo for a few GB |
| Glue database + tables | `tradelens_db` | free tier |
| EMR Serverless app | `tradelens` | **$0 when idle** (scales to zero) |
| Athena workgroup | `tradelens-athena-wg` | pay-per-scan only |
| Redshift Serverless | `tradelens-redshift-ns/-wg` | **compute $0 idle, but STORAGE bills continuously** |

## 2. The one thing that can quietly cost money
**Redshift Serverless managed storage** bills while data sits in tables, even with
no queries. After a demo, either:
- **Delete** the namespace/workgroup and reload from S3 next time (`COPY`), or
- **Snapshot then delete**, restore before the next demo (delete snapshot when
  your interview cycle is over).

Everything else (EMR Serverless, Athena, S3-at-rest for a few GB) is effectively
free or negligible when idle.

## 3. IAM (least privilege)
- `tradelens-emr-job-role`: read raw+code buckets, read/write curated bucket,
  Glue catalog access.
- A Redshift COPY role with read access to the curated bucket.

## 4. Teardown (run when done demoing)
```
# Redshift (stops the only continuous cost)
aws redshift-serverless delete-workgroup --workgroup-name tradelens-redshift-wg
aws redshift-serverless delete-namespace --namespace-name tradelens-redshift-ns
# EMR Serverless app (optional; it's free idle)
# aws emr-serverless delete-application --application-id <id>
# S3: keep a small sample for demos, or empty buckets to reach true $0.
```
All teardown commands need `--profile <name>` (or an exported `AWS_PROFILE`)
too, per SS0.5.

Full foundation teardown (buckets, IAM role, EMR app, Glue DB), once you're
done with the project entirely:
```
aws emr-serverless delete-application --application-id "$TRADELENS_EMR_APP_ID"
aws iam delete-role-policy --role-name tradelens-emr-job-role --policy-name tradelens-emr-job-role-policy
aws iam delete-role --role-name tradelens-emr-job-role
aws glue delete-database --name tradelens_db
aws s3 rb "s3://$TRADELENS_RAW_BUCKET" --force
aws s3 rb "s3://$TRADELENS_CURATED_BUCKET" --force
aws s3 rb "s3://$TRADELENS_CODE_BUCKET" --force
```

## 5. IaC (optional upgrade)
Terraform or CloudFormation for all of the above is a strong addition and a good
extra resume signal. Add under `infra/terraform/` if you go that route.

## 6. CI/CD and the Docker image
The `Dockerfile` at the repo root builds TradeLens's EMR Serverless runtime
image (extends AWS's own `public.ecr.aws/emr-serverless/spark/emr-7.2.0` base
— custom images must extend an AWS-provided base, arbitrary bases aren't
supported). It bakes in the Delta JARs, the `delta`/`yaml`/`dotenv` Python
packages, and the application code + config — everything the earlier
`--jars`/`--py-files`/`--files` approach had to fetch from S3 at job-submit
time, which was fragile (see the debugging notes for Phase 6/7 in project
memory). `scripts/submit_emr_serverless.sh` and
`.github/workflows/run-pipeline.yml` now just reference an in-image
entryPoint — no jar/zip staging.

**Building it is CI's job, not your laptop's.** The base image is a full
Spark/Hadoop distribution — a local build once starved an 8GB dev machine
down to ~60MB free RAM mid-build. `scripts/deploy_docker.sh` exists for a
more capable machine, but the default path is: push to `main`,
`.github/workflows/deploy.yml` builds and pushes the image (Docker Hub as
the primary published artifact, mirrored to ECR since EMR Serverless can
only pull custom images from there — never Docker Hub directly), then
updates the EMR Serverless application to use it via
`aws emr-serverless update-application --image-configuration`.

Three workflows:
- `ci.yml` — runs pytest on every PR (and is reused by `deploy.yml`, so
  pushes to `main` don't test twice).
- `deploy.yml` — on push to `main`: test, then build+push the image to
  Docker Hub + ECR, then point the application at the new image. Never
  submits a job — no AWS compute spend happens here.
- `run-pipeline.yml` — **manual only** (`workflow_dispatch`), per the user's
  explicit choice: this is the one workflow that spends real EMR Serverless
  compute, so it never fires on its own.

**One-time setup** (GitHub repo secrets — Settings → Secrets and variables →
Actions, or `gh secret set`): `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
for the `lavanya` account and `DOCKERHUB_TOKEN` (a Docker Hub access token,
not your account password — generate one at hub.docker.com → Account
Settings → Security) need to be set by you directly, since an agent
shouldn't be the one handling raw credential material even to move it into
a secret store. `DOCKERHUB_USERNAME`, `TRADELENS_EMR_APP_ID`,
`TRADELENS_EMR_JOB_ROLE_ARN`, and `TRADELENS_CURATED_BUCKET` aren't
credentials and are already set from `.env`.
