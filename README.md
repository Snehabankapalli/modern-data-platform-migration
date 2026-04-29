# Modern Data Platform Migration

A reference implementation for migrating a legacy data stack to a modern, cloud-native data platform built around Airflow, dbt, and a cloud data warehouse.

## What this repo contains

- **architecture/** — high-level platform migration architecture diagrams and decisions
- **airflow/** — orchestration DAGs, including the `legacy_to_modern_pipeline`
- **dbt/** — staging, intermediate, and marts models plus tests
- **data_quality/** — declarative validation rules and freshness checks
- **docs/** — migration strategy, data contracts, and operational runbook
- **scripts/** — standalone Python utilities (e.g., `sample_ingestion.py`)

## Quickstart

1. Clone the repo and create a Python virtualenv.
2. Install dependencies: `pip install -r requirements.txt` (TBD).
3. Configure connections for your legacy source and modern warehouse via environment variables.
4. Run the sample ingestion:
   ```bash
   python scripts/sample_ingestion.py \
     --source-name customers \
     --source-uri postgresql://user:pass@legacy-host/db \
     --source-table public.customers \
     --target-uri postgresql://user:pass@warehouse-host/analytics
   ```
5. Trigger the Airflow DAG `legacy_to_modern_pipeline` to run the full end-to-end flow.

## Architecture at a glance

Legacy OLTP sources → Airflow-orchestrated ingestion → raw schema in warehouse → dbt staging → intermediate → marts → BI / ML consumers. Data quality is enforced both in-flight (during ingestion) and post-load (via dbt tests and freshness checks).

See `architecture/platform-migration-architecture.md` and `docs/migration_strategy.md` for details.

## Status

Reference / scaffolding repository. Not production-ready out of the box.
# modern-data-platform-migration
