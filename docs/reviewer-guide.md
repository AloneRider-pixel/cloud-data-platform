# Reviewer Guide

Recommended review path for a first technical pass:

1. Read `README.md` for the end-to-end data flow.
2. Inspect ingestion extractors, retry logic, and validation boundaries.
3. Inspect Airflow DAGs for orchestration, backfills, and incremental loading.
4. Inspect dbt staging/intermediate/analytics models and data-quality tests.
5. Run the local stack and trace a dataset from raw storage through the analytics layer.

Engineering concerns intentionally documented in the repository include idempotency, incremental processing, schema drift, freshness, data-quality gates, and reproducible local execution.