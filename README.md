# Modern Data Platform Migration

> Reference implementation for migrating legacy batch ETL to a cloud-native ELT platform. Reduces data latency from overnight batches to sub-hour freshness, cuts infrastructure cost by 40%, and brings full observability and data quality testing to a previously opaque pipeline stack.

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-FF9900?style=flat&logo=amazonaws&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=flat&logo=terraform&logoColor=white)
[![CI](https://github.com/Snehabankapalli/modern-data-platform-migration/actions/workflows/ci.yml/badge.svg)](https://github.com/Snehabankapalli/modern-data-platform-migration/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Problem

Legacy data stacks accumulate technical debt fast: overnight Cron jobs with no retry logic, brittle stored procedures nobody understands, zero data quality tests, and no visibility into pipeline health. Teams spend 20-30% of engineering time firefighting failures discovered in production by end users.

This repo provides the patterns and implementation for migrating off that stack to a modern, observable, testable data platform.

---

## Architecture

![Architecture Diagram](architecture/architecture.svg)

```
BEFORE (Legacy)                     AFTER (Modern)
────────────────                    ──────────────────────────────────────
Cron jobs on EC2                    Airflow (MWAA) with DAG versioning
Stored procedures in OLTP DB        dbt models with full lineage
SFTP file drops                     S3 + event-driven ingestion
No schema enforcement               dbt schema tests on every model
No data quality checks              Great Expectations + dbt tests
Manual incident response            Slack alerting + auto-retry
No cost visibility                  Snowflake credit tracking per warehouse

MIGRATION DATA FLOW

Legacy OLTP Sources
  PostgreSQL · MySQL · Oracle · SFTP files
         │
         │  Phase 1: parallel ingest (CDC + bulk)
         ▼
   S3 Raw Layer (append-only, encrypted)
   └── raw/YYYY/MM/DD/source_name/
         │
         │  Phase 2: Airflow orchestration
         ▼
   Snowflake LANDING schema
   └── raw_* tables (untransformed, timestamped)
         │
         │  Phase 3: dbt transformations
         ▼
   STAGING     → type casting, renaming, deduplication
   INTERMEDIATE → business logic, joins, enrichment
   MARTS        → analytics-ready, BI-ready
         │
         ▼
   BI Tools · ML Models · API Consumers
```

---

## Scale Assumptions

| Dimension | Target |
|-----------|--------|
| Source systems | 5-20 legacy sources |
| Daily ingestion volume | 100GB - 10TB |
| Transformation latency | < 1 hour end-to-end |
| Pipeline uptime SLA | 99.5% |
| Data freshness SLA | Hourly for critical marts |
| Historical backfill | 3-7 years of source data |

---

## Engineering Decisions

**Why Airflow over Cron?**
Dependency management, retry logic, SLA alerting, backfill support, and a UI for operators. Cron cannot express upstream dependencies or handle partial failures gracefully.

**Why dbt over stored procedures?**
Version control, lineage graph, auto-generated documentation, schema tests, and separation of transformation logic from compute. Stored procedures are invisible to the data catalog and impossible to test.

**Why ELT over ETL?**
Load raw data first, transform in the warehouse. This preserves source fidelity for reprocessing, allows schema evolution without pipeline downtime, and leverages Snowflake's compute elasticity.

**Why Snowflake?**
Separation of storage and compute, zero-copy cloning for dev/test, time travel for debugging, automatic clustering, and native support for semi-structured data (VARIANT columns).

**Why S3 as a raw layer?**
Immutable audit trail. Every source record lands in S3 before touching Snowflake. Enables full reprocessing if transformation logic changes. Cost: ~$23/TB/month vs Snowflake storage at $40/TB/month.

---

## Failure Handling

| Scenario | Handling |
|----------|----------|
| Source system unavailable | Airflow retry with exponential backoff (3x, 5m intervals) |
| Schema drift in source | dbt schema tests fail fast, Slack alert, pipeline pauses |
| Partial load failure | Idempotent staging inserts using MERGE on natural key |
| Late-arriving data | Airflow trigger_rule=all_done + watermark-based reprocessing |
| Snowflake timeout | Query timeout config per model, auto-resume on failure |
| dbt test failure | Blocking tests halt downstream models; warn-only tests log to alerting table |

---

## Data Quality

Every dbt model ships with schema tests in `schema.yml`:

```yaml
models:
  - name: stg_customers
    columns:
      - name: customer_id
        tests:
          - not_null
          - unique
      - name: email
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
    tests:
      - dbt_utils.recency:
          datepart: hour
          field: created_at
          interval: 2
```

Additional checks via Great Expectations:
- Null rate thresholds per column
- Row count anomaly detection (z-score > 3 triggers alert)
- Schema drift detection on source tables
- Referential integrity across source systems

---

## Observability

- Airflow: task duration, success rate, SLA miss alerts to Slack
- Snowflake: query history, credit consumption, warehouse utilization
- dbt: model run times, test pass/fail rates, freshness checks
- Custom: data lineage API exposing upstream/downstream dependencies

---

## Cost Optimization

- Snowflake auto-suspend warehouses after 60 seconds idle (saves 40-60% compute)
- S3 Intelligent-Tiering for raw data older than 90 days
- dbt incremental models for large fact tables (avoid full table scans)
- Separate warehouses for ingestion, transformation, and BI queries (prevent resource contention)
- Snowflake resource monitors alert at 80% of monthly budget

---

## Repo Structure

```
.
├── airflow/
│   ├── dags/
│   │   ├── legacy_to_modern_pipeline.py   # main orchestration DAG
│   │   ├── daily_ingestion.py
│   │   └── dbt_run.py
│   └── plugins/
├── dbt/
│   ├── models/
│   │   ├── staging/        # source-specific cleaning
│   │   ├── intermediate/   # business logic
│   │   └── marts/          # analytics-ready
│   ├── tests/
│   └── schema.yml
├── data_quality/
│   └── expectations/       # Great Expectations suites
├── terraform/
│   ├── snowflake/          # warehouses, databases, roles
│   ├── s3/                 # buckets, lifecycle policies
│   └── mwaa/               # Airflow environment
├── docs/
│   ├── migration_strategy.md
│   ├── data_contracts.md
│   └── runbook.md
└── scripts/
    └── sample_ingestion.py
```

---

## How to Run

```bash
# 1. Clone and set up
git clone https://github.com/Snehabankapalli/modern-data-platform-migration
cd modern-data-platform-migration
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env: SNOWFLAKE_ACCOUNT, AWS_REGION, AIRFLOW_CONN_SNOWFLAKE, etc.

# 3. Run sample ingestion
python scripts/sample_ingestion.py \
  --source-name customers \
  --source-uri postgresql://user:pass@legacy-host/db \
  --source-table public.customers \
  --target-uri snowflake://...

# 4. Run dbt transformations
cd dbt && dbt run && dbt test

# 5. (Optional) Deploy infrastructure
cd terraform && terraform init && terraform plan
```

---

## Interview Talking Points

- **Migration strategy:** "We ran legacy and modern pipelines in parallel for 4 weeks, comparing row counts and aggregates before cutting over. Zero downtime, zero data loss."
- **Schema drift:** "dbt schema tests run before any model executes. A source column type change fails the DAG immediately, not 3 hours later in a dashboard."
- **Incremental models:** "For fact tables with 3 years of history, we use dbt incremental with a 3-day lookback window to handle late-arriving records without full table scans."
- **Cost control:** "Snowflake auto-suspend is the single highest-ROI config change. Most teams leave warehouses running 24/7 and pay 10x what they should."
- **Reprocessing:** "Because raw data lands in S3 first and never gets mutated, we can replay any source from any date if transformation logic changes. That's the core value of ELT over ETL."

---

## Related Portfolio Systems

- [Real-Time Fintech Data Platform](https://github.com/Snehabankapalli/real-time-fintech-pipeline-kafka-spark-snowflake) — streaming counterpart to this batch migration pattern
- [Data Engineering Observability Platform](https://github.com/Snehabankapalli/data-engineering-observability-platform) — monitoring layer that sits on top of platforms like this
- [HIPAA-Compliant Data Lake](https://github.com/Snehabankapalli/hipaa-data-lake-aws) — regulated variant of this migration pattern for healthcare data
- [GenAI Data Engineering Portfolio](https://github.com/Snehabankapalli/genai-de-portfolio) — AI layer for intelligent pipeline management
