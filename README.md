# Cloud Data Platform

[![CI](https://github.com/AloneRider-pixel/cloud-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/AloneRider-pixel/cloud-data-platform/actions/workflows/ci.yml)
[![CodeQL](https://github.com/AloneRider-pixel/cloud-data-platform/actions/workflows/codeql.yml/badge.svg)](https://github.com/AloneRider-pixel/cloud-data-platform/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

End-to-end data-engineering reference platform for ingestion, orchestration, transformation, validation, warehouse modeling, and analytics.

## Architecture

```mermaid
graph LR
    SRC[APIs / CSV / Events] --> ING[Python Ingestion]
    ING --> RAW[S3 / MinIO]
    RAW --> AIR[Airflow]
    AIR --> DBT[dbt]
    DBT --> WH[(PostgreSQL / Snowflake)]
    WH --> BI[Dashboard]
    ING --> Q[Quality Gates]
```

## Engineering capabilities

- Paginated API, CSV, and event ingestion with validation and replay-safe loading.
- Incremental and idempotent processing.
- Airflow batch and incremental orchestration.
- dbt transformations and warehouse modeling.
- Bronze → Silver → Gold data layers.
- Schema, freshness, reconciliation, and quality checks.
- Dockerized local infrastructure plus CI, CodeQL, dependency review, and Scorecard.

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.11, Requests, Pandas, PyArrow |
| Orchestration | Apache Airflow |
| Transformation | dbt Core |
| Object storage | AWS S3 / MinIO |
| Warehouse | PostgreSQL / Snowflake |
| Quality | Pytest, dbt tests, SQL assertions |
| Delivery | Docker, GitHub Actions |

## Repository map

```text
ingestion/
  src/extractors/
  src/loaders/
  src/validators/
  tests/
airflow/dags/
dbt/
warehouse/schemas/
dashboard/
scripts/
docs/
.github/workflows/
```

## Quick start

Prerequisites: Docker Compose and Python 3.11+.

```bash
git clone https://github.com/AloneRider-pixel/cloud-data-platform.git
cd cloud-data-platform
cp .env.example .env
docker compose up -d
```

Use MinIO for a local S3-compatible workflow. Provide real AWS credentials only when connecting to AWS.

## Verification

```bash
cd ingestion
ruff check src/
pytest tests/ -v --cov=src

cd ../dbt
dbt parse --profiles-dir .
dbt compile --profiles-dir .
```

## Data-quality contract

A successful pipeline run means the configured validation gates passed. Preserve schema validation, row-count reconciliation, freshness controls, and idempotency when extending ingestion or transformation behavior.

## Security

Keep cloud credentials outside source control. Review IAM scope, object-store permissions, database credentials, and data-ingestion boundaries together. CI uses immutable action references and least-privilege workflow permissions.

## Evidence policy

Runtime, throughput, freshness, quality, and cost claims require a named workload/dataset, environment, measurement method, sample/run count, and producing commit. Synthetic fixtures are not production benchmarks.

See [docs/evidence-policy.md](docs/evidence-policy.md).

## Documentation

- [Architecture](docs/architecture.md)
- [Data quality](docs/data-quality.md)
- [Verification](docs/verification.md)
- [Reviewer guide](docs/reviewer-guide.md)

## Roadmap

Lineage visualization, managed-cloud examples, reproducible Snowflake benchmarks, and broader data-quality integrations.

## License

MIT
