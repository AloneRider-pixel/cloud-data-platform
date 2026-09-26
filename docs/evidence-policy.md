# Evidence and reproducibility policy

This repository distinguishes data-pipeline capability from measured pipeline performance or data-quality results.

## Measured results

A published runtime, throughput, freshness, quality, or reliability number should identify the source dataset, row count or workload size, environment, tool versions, run count, failure definition, command/workflow, timestamp, and commit SHA.

## Data quality

Schema, null, uniqueness, relationship, accepted-value, freshness, and reconciliation claims should be backed by executable tests. A successful CI validation proves the configured checks passed; it does not prove the quality of an arbitrary external dataset.

## Synthetic/local data

MinIO, local PostgreSQL, generated fixtures, and sample datasets are useful for reproducible development. Results from those inputs must be labeled as sample or synthetic and must not be represented as production-scale evidence.

## Benchmark rule

Design targets in documentation are targets. Measured results require reproducible artifacts and methodology.
