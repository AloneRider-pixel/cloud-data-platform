# Cloud Data Platform Architecture

```mermaid
flowchart LR
    API[API sources] --> S3[(AWS S3 raw zone)]
    BATCH[Batch sources] --> S3
    S3 --> AIR[Apache Airflow]
    AIR --> ING[Python ingestion]
    ING --> VALID[Schema / data-quality validation]
    VALID --> DB[(PostgreSQL)]
    DB --> DBT[dbt models]
    DBT --> WH[(Snowflake)]
    AIR --> OBS[Pipeline metadata / logs]
    WH --> BI[Analytics / downstream consumers]
```

## Design goals

- Separate ingestion, validation, transformation, and analytical serving concerns.
- Make pipeline retries safe through idempotent processing and deterministic transformations.
- Validate schemas and data quality before downstream publication.
- Keep orchestration, transformation code, and warehouse models independently testable.

## Operational checks

CI validates ingestion code, dbt parsing/compilation, and Docker builds before changes are considered ready.
