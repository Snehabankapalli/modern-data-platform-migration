"""
legacy_to_modern_pipeline.py

Airflow DAG that orchestrates the end-to-end legacy -> modern data platform
migration flow:

    1. Extract+load each legacy source via scripts/sample_ingestion.py
    2. Run dbt staging models
    3. Run dbt intermediate + marts models
    4. Run dbt tests
    5. Run freshness / data-quality checks

Connections and warehouse credentials are expected to be configured as
Airflow connections (or pulled from a secrets backend). This DAG is a
reference scaffold and intentionally avoids hard-coding environment-specific
values.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Local import; in a real deployment, scripts/ would be on PYTHONPATH or
# packaged as part of the DAG bundle.
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(REPO_ROOT / "scripts"))

from sample_ingestion import (  # noqa: E402
    SourceConfig,
    TargetConfig,
    run_ingestion,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SOURCES = [
    {
        "name": "customers",
        "table": "public.customers",
        "watermark_column": "updated_at",
        "primary_key": "id",
    },
    {
        "name": "orders",
        "table": "public.orders",
        "watermark_column": "updated_at",
        "primary_key": "order_id",
    },
    {
        "name": "order_items",
        "table": "public.order_items",
        "watermark_column": "updated_at",
        "primary_key": "order_item_id",
    },
]


def _ingest_source(source_cfg: dict, **context) -> dict:
    """PythonOperator callable that ingests one source."""
    from airflow.models import Variable

    source = SourceConfig(
        name=source_cfg["name"],
        connection_uri=Variable.get("LEGACY_DB_URI"),
        table=source_cfg["table"],
        watermark_column=source_cfg["watermark_column"],
        primary_key=source_cfg["primary_key"],
    )
    target = TargetConfig(
        connection_uri=Variable.get("WAREHOUSE_URI"),
        schema="raw",
        table_prefix="legacy_",
        write_mode="append",
    )
    result = run_ingestion(
        source,
        target,
        validation_rules_path=REPO_ROOT / "data_quality" / "validation_rules.yml",
    )
    if result.status != "success":
        raise RuntimeError(f"Ingestion failed for {source_cfg['name']}: {result.error}")
    return {
        "source": result.source,
        "rows_loaded": result.rows_loaded,
        "rows_rejected": result.rows_rejected,
        "new_watermark": result.new_watermark,
    }


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="legacy_to_modern_pipeline",
    description="End-to-end legacy -> modern data platform migration pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["migration", "dbt", "ingestion"],
) as dag:

    start = EmptyOperator(task_id="start")

    ingestion_tasks = []
    for src in SOURCES:
        task = PythonOperator(
            task_id=f"ingest_{src['name']}",
            python_callable=_ingest_source,
            op_kwargs={"source_cfg": src},
        )
        ingestion_tasks.append(task)

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd {{ params.dbt_dir }} && dbt run --select staging",
        params={"dbt_dir": str(REPO_ROOT / "dbt")},
    )

    dbt_marts = BashOperator(
        task_id="dbt_run_intermediate_and_marts",
        bash_command="cd {{ params.dbt_dir }} && dbt run --select intermediate marts",
        params={"dbt_dir": str(REPO_ROOT / "dbt")},
    )

    dbt_tests = BashOperator(
        task_id="dbt_test",
        bash_command="cd {{ params.dbt_dir }} && dbt test",
        params={"dbt_dir": str(REPO_ROOT / "dbt")},
    )

    freshness_checks = BashOperator(
        task_id="freshness_checks",
        bash_command=(
            "psql \"$WAREHOUSE_URI\" -v ON_ERROR_STOP=1 "
            "-f {{ params.sql_file }}"
        ),
        params={
            "sql_file": str(REPO_ROOT / "data_quality" / "freshness_checks.sql"),
        },
    )

    end = EmptyOperator(task_id="end")

    start >> ingestion_tasks >> dbt_staging >> dbt_marts >> dbt_tests >> freshness_checks >> end
