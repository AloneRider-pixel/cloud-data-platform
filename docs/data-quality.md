# Data Quality Verification

The platform treats data quality as a pipeline gate rather than a dashboard-only concern.

## Validation layers

### Ingestion
Validate source schemas, required fields, types, and malformed records before publication.

### Storage
Track source-to-target row counts and preserve enough metadata to identify the input and load boundary.

### Transformation
Use dbt tests for nullability, uniqueness, relationships, accepted values, and model-level integrity.

### Freshness
Monitor source and downstream timestamps so stale data can be detected before it reaches analytical consumers.

### Incremental correctness
Use watermarks and idempotent load patterns so retries do not silently duplicate records or skip an already-processed boundary.

## Review checklist

A pipeline change should answer:

- What is the source contract?
- What happens when the schema changes?
- Is a retry safe?
- How are duplicate records handled?
- How is freshness measured?
- Which dbt tests fail when the contract is violated?
- Can a failed load be replayed deterministically?

## Benchmarking

Performance figures should only be published with the dataset size, environment, run count, workload shape, and commit/configuration used for the measurement.

The current README values marked as targets are design targets, not measured production results.
