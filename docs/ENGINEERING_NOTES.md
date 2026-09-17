# Engineering Notes

## Engineering focus
An end-to-end data platform demonstrating ingestion, orchestration, transformation, data quality, warehouse modeling, and analytics delivery.

## Key design decisions
- **Raw → clean → analytics layers:** separates ingestion concerns from business-ready models.
- **Airflow orchestration:** makes batch and incremental workflows explicit and observable.
- **dbt transformations/tests:** keeps SQL transformation logic versioned and testable.
- **Idempotent/incremental loading:** supports safe reruns and avoids unnecessary full refreshes.
- **Data-quality gates:** schema, completeness, uniqueness, referential integrity, freshness, and volume checks are treated as pipeline concerns.

## Verification checklist
- Start the local stack with Docker Compose.
- Run ingestion tests and dbt validation.
- Execute batch and incremental DAGs.
- Verify dashboard output against warehouse tables.

## Portfolio note
Pipeline metrics shown in the README are targets unless backed by reproducible benchmark output in the repository.
