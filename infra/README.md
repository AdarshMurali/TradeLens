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
