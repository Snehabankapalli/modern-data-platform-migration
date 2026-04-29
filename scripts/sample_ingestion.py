"""
sample_ingestion.py

Sample ingestion script for the modern data platform migration.

This script demonstrates the pattern used to extract data from legacy
sources (e.g., on-prem Postgres, MySQL, flat files) and land it into the
modern warehouse's raw/staging layer. It is intended to be invoked either
directly or orchestrated by the Airflow DAG at
`airflow/dags/legacy_to_modern_pipeline.py`.

Design goals:
    * Idempotent, incremental loads driven by a watermark column
    * Schema-on-read with explicit data contracts (see docs/data_contracts.md)
    * Emits structured metadata for downstream observability
    * Fails loudly and early; integrates with data_quality/validation_rules.yml
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

import pandas as pd
import sqlalchemy as sa
import yaml
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
log = logging.getLogger("sample_ingestion")


# ---------------------------------------------------------------------------
# Configuration models
# ---------------------------------------------------------------------------
@dataclass
class SourceConfig:
    """Connection + extraction settings for a legacy source."""
    name: str
    connection_uri: str
    table: str
    watermark_column: str = "updated_at"
    primary_key: str = "id"
    batch_size: int = 50_000


@dataclass
class TargetConfig:
    """Connection + load settings for the modern warehouse."""
    connection_uri: str
    schema: str = "raw"
    table_prefix: str = "legacy_"
    write_mode: str = "append"  # append | overwrite | merge


@dataclass
class IngestionResult:
    source: str
    target_table: str
    rows_extracted: int = 0
    rows_loaded: int = 0
    rows_rejected: int = 0
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    finished_at: Optional[str] = None
    new_watermark: Optional[str] = None
    status: str = "running"
    error: Optional[str] = None

    def finalize(self, status: str, error: Optional[str] = None) -> "IngestionResult":
        self.status = status
        self.error = error
        self.finished_at = datetime.now(timezone.utc).isoformat()
        return self


# ---------------------------------------------------------------------------
# Engines
# ---------------------------------------------------------------------------
@contextmanager
def engine_scope(uri: str) -> Iterator[Engine]:
    engine = sa.create_engine(uri, pool_pre_ping=True, future=True)
    try:
        yield engine
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Watermark management
# ---------------------------------------------------------------------------
WATERMARK_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS meta.ingestion_watermarks (
    source_name      TEXT PRIMARY KEY,
    target_table     TEXT NOT NULL,
    last_watermark   TEXT,
    updated_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""


def get_last_watermark(target_engine: Engine, source_name: str) -> Optional[str]:
    with target_engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS meta;")
        conn.exec_driver_sql(WATERMARK_TABLE_DDL)
        row = conn.execute(
            sa.text(
                "SELECT last_watermark FROM meta.ingestion_watermarks "
                "WHERE source_name = :s"
            ),
            {"s": source_name},
        ).fetchone()
    return row[0] if row else None


def set_last_watermark(
    target_engine: Engine, source_name: str, target_table: str, watermark: str
) -> None:
    with target_engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT INTO meta.ingestion_watermarks
                    (source_name, target_table, last_watermark, updated_at)
                VALUES (:s, :t, :w, NOW())
                ON CONFLICT (source_name) DO UPDATE
                SET last_watermark = EXCLUDED.last_watermark,
                    target_table   = EXCLUDED.target_table,
                    updated_at     = NOW();
                """
            ),
            {"s": source_name, "t": target_table, "w": watermark},
        )


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------
def extract_incremental(
    source_engine: Engine,
    source: SourceConfig,
    last_watermark: Optional[str],
) -> Iterable[pd.DataFrame]:
    """Yield batches of rows newer than `last_watermark`."""
    where_clause = ""
    params: Dict[str, Any] = {}
    if last_watermark is not None:
        where_clause = f"WHERE {source.watermark_column} > :wm"
        params["wm"] = last_watermark

    query = sa.text(
        f"SELECT * FROM {source.table} {where_clause} "
        f"ORDER BY {source.watermark_column} ASC"
    )

    log.info("Extracting from %s (watermark=%s)", source.table, last_watermark)
    with source_engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql(query, conn, params=params, chunksize=source.batch_size):
            yield chunk


# ---------------------------------------------------------------------------
# Lightweight in-flight validation
# ---------------------------------------------------------------------------
def load_validation_rules(path: Path) -> Dict[str, Any]:
    if not path.exists():
        log.warning("No validation rules file found at %s", path)
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def apply_validations(df: pd.DataFrame, rules: Dict[str, Any]) -> (pd.DataFrame, int):
    """Drop rows failing not-null/unique-ish checks; return (clean_df, rejected)."""
    if df.empty or not rules:
        return df, 0

    initial = len(df)
    not_null_cols = rules.get("not_null", [])
    for col in not_null_cols:
        if col in df.columns:
            df = df[df[col].notna()]

    rejected = initial - len(df)
    if rejected:
        log.warning("Validation dropped %d rows", rejected)
    return df, rejected


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def load_batch(
    target_engine: Engine,
    df: pd.DataFrame,
    target_schema: str,
    target_table: str,
    write_mode: str,
) -> int:
    if df.empty:
        return 0

    # Add audit columns
    df = df.copy()
    df["_ingested_at"] = datetime.now(timezone.utc)
    df["_source_system"] = target_table

    if_exists = "append" if write_mode in ("append", "merge") else "replace"

    with target_engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")
        df.to_sql(
            name=target_table,
            con=conn,
            schema=target_schema,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=10_000,
        )
    return len(df)


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------
def run_ingestion(
    source: SourceConfig,
    target: TargetConfig,
    validation_rules_path: Optional[Path] = None,
) -> IngestionResult:
    target_table = f"{target.table_prefix}{source.name}"
    result = IngestionResult(source=source.name, target_table=target_table)

    rules = (
        load_validation_rules(validation_rules_path).get(source.name, {})
        if validation_rules_path
        else {}
    )

    try:
        with engine_scope(source.connection_uri) as src_eng, \
             engine_scope(target.connection_uri) as tgt_eng:

            last_wm = get_last_watermark(tgt_eng, source.name)
            max_wm_seen: Optional[str] = last_wm

            for batch in extract_incremental(src_eng, source, last_wm):
                result.rows_extracted += len(batch)

                clean, rejected = apply_validations(batch, rules)
                result.rows_rejected += rejected

                loaded = load_batch(
                    tgt_eng, clean, target.schema, target_table, target.write_mode
                )
                result.rows_loaded += loaded

                if not clean.empty and source.watermark_column in clean.columns:
                    batch_max = str(clean[source.watermark_column].max())
                    if max_wm_seen is None or batch_max > max_wm_seen:
                        max_wm_seen = batch_max

            if max_wm_seen and max_wm_seen != last_wm:
                set_last_watermark(tgt_eng, source.name, target_table, max_wm_seen)
                result.new_watermark = max_wm_seen

        return result.finalize(status="success")

    except Exception as exc:  # noqa: BLE001
        log.exception("Ingestion failed for %s", source.name)
        return result.finalize(status="failed", error=str(exc))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Legacy -> Modern ingestion sample")
    parser.add_argument("--source-name", required=True, help="Logical source name")
    parser.add_argument("--source-uri", required=True, help="SQLAlchemy URI for source")
    parser.add_argument("--source-table", required=True, help="Fully-qualified table")
    parser.add_argument("--watermark-column", default="updated_at")
    parser.add_argument("--primary-key", default="id")
    parser.add_argument("--batch-size", type=int, default=50_000)

    parser.add_argument("--target-uri", required=True, help="SQLAlchemy URI for warehouse")
    parser.add_argument("--target-schema", default="raw")
    parser.add_argument("--target-prefix", default="legacy_")
    parser.add_argument(
        "--write-mode", choices=["append", "overwrite", "merge"], default="append"
    )

    parser.add_argument(
        "--validation-rules",
        type=Path,
        default=Path("data_quality/validation_rules.yml"),
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    source = SourceConfig(
        name=args.source_name,
        connection_uri=args.source_uri,
        table=args.source_table,
        watermark_column=args.watermark_column,
        primary_key=args.primary_key,
        batch_size=args.batch_size,
    )
    target = TargetConfig(
        connection_uri=args.target_uri,
        schema=args.target_schema,
        table_prefix=args.target_prefix,
        write_mode=args.write_mode,
    )

    result = run_ingestion(source, target, args.validation_rules)
    print(json.dumps(asdict(result), indent=2, default=str))
    return 0 if result.status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
