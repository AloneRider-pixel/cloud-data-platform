# 🏗️ Cloud ETL & Analytics Platform

**End-to-End Data Engineering Platform** — Batch & streaming ingestion, orchestrated ETL pipelines, dbt transformations, data quality, and analytics dashboards.

---

## 🏗️ Architecture

```
REST APIs / CSV / Event Streams
         ↓
   Python Ingestion Layer
   (Extractors + Validators)
         ↓
     AWS S3 (Raw Zone)
   (Parquet, partitioned)
         ↓
     Apache Airflow
   (Orchestration + Monitoring)
         ↓
      dbt Core
   (Transform + Test)
         ↓
   PostgreSQL / Snowflake
   (Raw → Clean → Analytics)
         ↓
   Analytics Dashboard
   (Streamlit / Metabase)
```

## ✨ Features

### Ingestion
- **REST API Extraction** — Paginated API ingestion with rate limiting & retries
- **CSV Batch Loading** — Multi-file CSV ingestion with schema validation
- **Event Stream Processing** — Simulated event ingestion from Kinesis/SQS
- **Incremental Loading** — Watermark-based delta detection for efficient loads
- **Idempotent Pipelines** — Safe to re-run without data duplication

### Orchestration (Airflow)
- **Batch ETL DAG** — Full refresh pipeline for small/medium datasets
- **Incremental DAG** — Delta-only processing for large datasets
- **API Ingestion DAG** — Scheduled API pulls with backfill support
- **Data Quality DAG** — Automated data quality checks post-load
- **Retry & Alerting** — Configurable retries with Slack/email notifications
- **Data Lineage** — DAG-level lineage tracking across pipeline stages

### Transformation (dbt)
- **Layered Architecture** — Staging → Intermediate → Analytics models
- **Incremental Models** — Efficient updates using merge strategies
- **Schema Validation** — Auto-generated schema tests on all models
- **Data Quality Tests** — Uniqueness, non-null, referential integrity, accepted values
- **Reusable Macros** — Custom Jinja macros for common patterns
- **Documentation** — Auto-generated model docs with column descriptions

### Data Quality
- **Great Expectations-style tests** — Built into dbt test framework
- **Schema drift detection** — Alerts on unexpected column changes
- **Row count validation** — Source vs target reconciliation
- **Freshness checks** — Detect stale data in production tables
- **Anomaly detection** — Statistical outlier flagging

### Infrastructure
- **Docker Compose** — Full local development environment
- **GitHub Actions CI/CD** — Automated testing, linting, and deployment
- **AWS S3** — Raw data lake with partitioned Parquet storage
- **PostgreSQL/Snowflake** — Analytical warehouse with layered schemas
- **Monitoring** — Pipeline health dashboard with SLA tracking

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python 3.11, Requests, Pandas, PyArrow |
| Orchestration | Apache Airflow 2.8 |
| Transformation | dbt Core 1.7 |
| Warehouse | PostgreSQL 16, Snowflake |
| Storage | AWS S3, Local MinIO |
| Quality | dbt tests, custom SQL assertions |
| Dashboard | Streamlit |
| Infra | Docker, GitHub Actions, AWS |

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- AWS credentials (optional, uses MinIO locally)

### 1. Clone & Configure

```bash
git clone https://github.com/AloneRider-pixel/cloud-data-platform.git
cd cloud-data-platform
cp .env.example .env
```

### 2. Start All Services

```bash
docker-compose up -d
```

This starts:
- **PostgreSQL** → localhost:5432
- **MinIO (S3)** → localhost:9000 (Console: localhost:9001)
- **Airflow** → localhost:8080
- **Streamlit Dashboard** → localhost:8501

### 3. Run Initial Data Load

```bash
docker-compose exec ingestion python -m src.loaders.seed_data
```

### 4. Trigger Airflow DAGs

Open http://localhost:8080 and unpause the DAGs, or:

```bash
docker-compose exec airflow-webserver airflow dags trigger etl_batch_pipeline
docker-compose exec airflow-webserver airflow dags trigger api_ingestion_pipeline
```

### 5. Run dbt Transformations

```bash
docker-compose exec dbt dbt run --profiles-dir /root/.dbt
docker-compose exec dbt dbt test --profiles-dir /root/.dbt
```

### 6. View Dashboard

Open http://localhost:8501

---

## 📁 Project Structure

```
cloud-data-platform/
├── ingestion/                  # Python data extraction layer
│   ├── src/
│   │   ├── extractors/         # API, CSV, Event extractors
│   │   ├── loaders/            # S3 & warehouse loaders
│   │   ├── validators/         # Schema & data validation
│   │   ├── transforms/         # Light pre-load transformations
│   │   └── config.py           # Centralized configuration
│   ├── tests/                  # Unit tests for ingestion
│   ├── Dockerfile
│   └── requirements.txt
├── airflow/                    # Orchestration layer
│   ├── dags/                   # Airflow DAG definitions
│   │   ├── etl_batch_dag.py
│   │   ├── etl_incremental_dag.py
│   │   ├── api_ingestion_dag.py
│   │   ├── data_quality_dag.py
│   │   └── common/             # Shared operators & utilities
│   ├── plugins/                # Custom Airflow plugins
│   ├── config/                 # Airflow configuration
│   └── Dockerfile
├── dbt/                        # Transformation layer
│   ├── models/
│   │   ├── staging/            # Source-aligned models
│   │   ├── intermediate/       # Business logic layer
│   │   └── analytics/          # Final analytics models
│   ├── tests/                  # Singular data tests
│   ├── macros/                 # Reusable Jinja macros
│   ├── seeds/                  # Seed/reference data
│   ├── snapshots/              # SCD Type 2 snapshots
│   ├── dbt_project.yml
│   └── profiles.yml
├── warehouse/                  # SQL definitions
│   ├── migrations/             # Schema migrations
│   ├── schemas/                # DDL for all schemas
│   └── seeds/                  # Reference data SQL
├── dashboard/                  # Analytics layer
│   ├── src/
│   │   ├── pages/              # Dashboard pages
│   │   ├── components/         # Reusable UI components
│   │   └── queries/            # SQL query library
│   ├── Dockerfile
│   └── requirements.txt
├── monitoring/                 # Pipeline observability
│   ├── checks/                 # Data quality checks
│   ├── alerts/                 # Alerting configuration
│   └── metrics/                # Custom metrics
├── scripts/                    # Utility scripts
│   ├── generate_mock_data.py
│   ├── setup_warehouse.py
│   └── run_pipeline.py
├── tests/                      # Integration tests
├── .github/workflows/ci.yml   # CI/CD pipeline
├── docker-compose.yml
└── .env.example
```

---

## 📊 Data Layers

### Raw Layer (Bronze)
- Direct copy of source data
- Parquet format, partitioned by date
- No transformations applied
- Full audit trail preserved

### Clean Layer (Silver)
- Deduplicated and validated
- Type casting and standardization
- Referential integrity enforced
- Slowly Changing Dimensions (SCD Type 2)

### Analytics Layer (Gold)
- Business-ready aggregated views
- Optimized for dashboard queries
- Pre-computed KPIs and metrics
- Star schema for BI tools

---

## 📈 Pipeline Metrics

| Metric | Target |
|--------|--------|
| Batch ETL Runtime | < 15 min |
| Incremental Load | < 2 min |
| Data Quality Pass Rate | > 99% |
| Pipeline Success Rate | > 99.5% |
| Data Freshness SLA | < 1 hour |

---

## 🔒 Data Quality Framework

| Check Type | Implementation |
|-----------|---------------|
| Schema validation | dbt `dbt expect_column_to_exist` |
| Uniqueness | dbt `unique` test on primary keys |
| Completeness | dbt `not_null` test on required fields |
| Referential | dbt `relationships` test for FK integrity |
| Freshness | dbt `source_freshness` for SLA monitoring |
| Volume | Row count reconciliation source → target |
| Distribution | Statistical bounds on key metrics |

---

## 📝 License

MIT
