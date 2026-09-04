# Infrastructure & Cost Control

> **Do Phase 6 of the plan only after this checklist. Cost discipline is part of
> the project story.**

## 0. Billing alarm FIRST (AWS Budgets — always free)
- Create a monthly cost budget of **$5** with an alert at 80% and 100%.
  Console: Billing → Budgets → Create budget → Cost budget.
- Optionally a $10 hard "zero-spend"-style alert as backup.

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

## 5. IaC (optional upgrade)
Terraform or CloudFormation for all of the above is a strong addition and a good
extra resume signal. Add under `infra/terraform/` if you go that route.
