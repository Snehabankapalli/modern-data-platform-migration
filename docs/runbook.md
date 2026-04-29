# Operational Runbook

This runbook covers the most common operational scenarios for the
`legacy_to_modern_pipeline` Airflow DAG and the dbt project.

## On-call quick links

- Airflow UI: https://airflow.example.com/dags/legacy_to_modern_pipeline
- dbt docs:    https://dbt-docs.example.com
- Warehouse query console: https://warehouse.example.com
- Alerting channel: #data-platform-oncall

---

## Scenario 1: Ingestion task failed

**Symptoms**: `ingest_<source>` task is red in Airflow.

**Diagnosis**:
1. Open the failed task's logs.
2. Look for the JSON `IngestionResult` printed at the end. The `error`
   field will indicate the failure mode.

**Common causes & fixes**:
- *Connection refused / timeout to legacy DB*: confirm legacy DB is up;
  check VPC peering / firewall rules.
- *Permission denied on `raw` schema*: verify the warehouse role has
  `USAGE` and `CREATE` on `raw` and on `meta`.
- *Schema mismatch (column missing)*: a legacy schema change has occurred.
  Coordinate with the source-owning team and follow the data contract change
  process.

**Recovery**: re-run the failed task. The pipeline is idempotent; re-running
will resume from the last committed watermark.

---

## Scenario 2: Freshness check failed

**Symptoms**: `freshness_checks` task fails with
`Freshness check failed for N source table(s)`.

**Diagnosis**:
1. Run `data_quality/freshness_checks.sql` interactively and inspect the
   `graded` CTE to see which tables exceeded their threshold.
2. Cross-reference with the `ingest_*` tasks for the same DAG run.

**Recovery**:
- If the upstream system was down: rerun the affected ingestion tasks once
  the system recovers.
- If thresholds are too tight for normal operation: revisit
  `dbt/tests/schema.yml` source freshness config and
  `data_quality/freshness_checks.sql`.

---

## Scenario 3: dbt test failed

**Symptoms**: `dbt_test` task fails.

**Diagnosis**:
1. Open the task logs and find the failing test names.
2. Run the failing test locally:
   ```bash
   cd dbt
   dbt test --select <model_name>
   ```
3. For data tests, query the compiled SQL to see the offending rows.

**Recovery**:
- *not_null / unique* failures usually indicate an ingestion or upstream
  source issue; treat as a contract violation and escalate.
- *relationships* failures usually indicate referential integrity drift;
  may need a dimension reload.

---

## Scenario 4: Backfill a source

```bash
# Reset the watermark for a single source
psql "\$WAREHOUSE_URI" -c "
  delete from meta.ingestion_watermarks where source_name = 'orders';
"

# Optionally truncate the raw table for a clean reload
psql "\$WAREHOUSE_URI" -c "truncate raw.legacy_orders;"

# Re-run the DAG (or trigger the single ingest task in Airflow)
```

The next pipeline run will perform a full reload and re-establish the
watermark.

---

## Scenario 5: Out-of-band manual ingestion

To run an ad-hoc ingestion (e.g., for debugging) without scheduling a DAG run:

```bash
python scripts/sample_ingestion.py \\
  --source-name customers \\
  --source-uri "\$LEGACY_DB_URI" \\
  --source-table public.customers \\
  --target-uri "\$WAREHOUSE_URI"
```

The script writes its own watermark to `meta.ingestion_watermarks`, so the
next scheduled run will pick up where the manual run left off.

---

## Escalation

| Severity | Definition                                                | Action                                |
|----------|-----------------------------------------------------------|---------------------------------------|
| SEV-1    | Marts unavailable to BI users for > 30 min                | Page on-call. Notify stakeholders.    |
| SEV-2    | Freshness SLA breached for any mart                       | Open incident. Investigate within 2h. |
| SEV-3    | Ingestion task failure self-healed by retry               | Log only.                             |
