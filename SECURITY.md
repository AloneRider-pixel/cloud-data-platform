# Security Policy

## Reporting

Report suspected vulnerabilities privately through GitHub's security reporting mechanism when available. Include affected components, reproducible steps, and sanitized evidence. Never publish secrets or credentials in an issue.

## Practices

- Credentials are provided through environment variables or managed secret stores.
- Data-quality validation is part of the ingestion pipeline.
- CI runs linting, tests, dbt validation, and dependency checks.
- CodeQL is enabled for static security analysis.
