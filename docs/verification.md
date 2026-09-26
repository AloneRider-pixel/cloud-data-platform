# Verification & Evidence

This repository uses an evidence-first policy for data engineering claims.

## Claim classes

- Capability: implemented behavior backed by source, tests, or reproducible execution.
- Design target: engineering objective, not a measured result.
- Measured benchmark: must include dataset size, environment, run count, commit, and command.
- Synthetic/demo: controlled data used to validate pipeline behavior, not external performance.

## Current evidence

| Area | Evidence | Reproduction |
|---|---|---|
| Ingestion validation | ingestion/tests/ | cd ingestion && pytest |
| dbt transformation | dbt project and models | dbt parse/compile/run/test with dbt/profiles.yml |
| Data-quality policy | dbt/dbt_project.yml | dbt tests are configured to fail on severity=error |
| CI | .github/workflows/ci.yml | GitHub Actions |
| Supply-chain security | .github/workflows/scorecard.yml | OpenSSF Scorecard |

## Publication rule

Do not publish throughput, success-rate, freshness, or quality percentages as measured results unless the dataset, workload, environment, run count, timestamp, and commit are recorded.