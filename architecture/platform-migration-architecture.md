# Platform Migration Architecture

## Overview

This document describes the target architecture for migrating from the legacy
on-prem analytics stack to a modern, cloud-native data platform.

## Goals

- Decouple ingestion, transformation, and serving layers.
- Make all data flows declarative, idempotent, and observable.
- Enforce contracts at the boundary between producers and consumers.
- Reduce time-to-insight for analytics and ML use cases.

## Logical components

1. **Sources** — legacy OLTP systems (Postgres, MySQL, SQL Server), SaaS APIs,
   and flat-file drops on SFTP.
2. **Ingestion layer** — Python-based extractors orchestrated by Airflow.
   See `scripts/sample_ingestion.py` for the reference pattern.
3. **Raw / landing zone** — append-only schema in the warehouse
   (`raw.legacy_*`) with audit columns (`_ingested_at`, `_source_system`).
4. **Transformation layer** — dbt models organized as
   staging → intermediate → marts.
5. **Quality layer** — declarative validations
   (`data_quality/validation_rules.yml`), freshness checks
   (`data_quality/freshness_checks.sql`), and dbt tests
   (`dbt/tests/schema.yml`).
6. **Serving layer** — curated marts exposed to BI tools, reverse-ETL, and
   feature stores.
7. **Metadata & observability** — ingestion watermarks
   (`meta.ingestion_watermarks`), dbt artifacts, and lineage.

## Data flow

```
Legacy Sources
     │
     ▼
[Airflow DAG: legacy_to_modern_pipeline]
     │  (sample_ingestion.py per source)
     ▼
raw.legacy_<entity>            ← append-only, audit columns
     │
     ▼
dbt staging (stg_<entity>)     ← typing, renaming, light cleanup
     │
     ▼
dbt intermediate (int_<...>)   ← joins, business logic
     │
     ▼
dbt marts (fct_*, dim_*)       ← consumer-facing models
     │
     ▼
BI / ML / reverse-ETL
```

## Key design decisions

- **Incremental by default**: every ingestion is watermark-driven; full
  refresh is opt-in.
- **Idempotency**: re-running the same window must not produce duplicates;
  enforced by primary keys and merge semantics in the staging layer.
- **Schema evolution**: additive changes are auto-propagated; breaking
  changes require a contract bump (see `docs/data_contracts.md`).
- **Separation of duties**: ingestion code never embeds business logic;
  business logic lives only in dbt.

## Out of scope (for v1)

- Real-time / streaming ingestion (Kafka, Kinesis).
- ML feature pipelines.
- Cross-region replication.

These will be addressed in a follow-up phase.
