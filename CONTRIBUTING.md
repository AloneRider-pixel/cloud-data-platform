# Contributing to Cloud Data Platform

This repository is maintained as a production-oriented data engineering portfolio project.

## Development workflow

1. Create a focused branch from `main`.
2. Keep changes scoped and document important data-model or pipeline decisions.
3. Add or update tests and data-quality checks for behavioral changes.
4. Run linting, tests, and dbt validation locally before opening a pull request.
5. Never commit secrets, credentials, API keys, warehouse data, or generated artifacts.

## Quality expectations

- Prefer idempotent and observable pipelines.
- Validate schemas and data quality at ingestion and transformation boundaries.
- Keep SQL transformations readable, testable, and documented.
- Update pipeline documentation when interfaces, schedules, or data contracts change.

## Pull requests

Include a concise summary, validation performed, schema/data-contract impact, and any operational considerations.
