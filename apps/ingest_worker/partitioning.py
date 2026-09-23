"""Applies a fetched schema to raw rows and writes the result into a Hive-style partitioned
Parquet "data lake" - the actual "T" and "L" of this ETL pipeline (the fileevent_handler and
schema_registry modules handle the "E").

Partitioning by `source_system/year=YYYY/month=MM/` (rather than one giant file, or one file per
source batch) is what makes a data lake actually queryable at scale later: a query engine that
understands Hive partitioning (Athena, Spark, DuckDB, pandas itself via
`pd.read_parquet(dataset, filters=...)`) can skip reading entire directories that don't match a
query's date range, instead of scanning every row ever ingested.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


class SchemaValidationError(Exception):
    """Raised when a raw row is missing a column the schema says is required."""


def apply_schema(rows: List[Dict[str, Any]], schema: Dict[str, Any]) -> pd.DataFrame:
    """Rename/cast raw columns according to `schema['columns']`.

    Each column entry looks like `{"name": "order_total", "source_column": "ORDER_TOTAL", "dtype": "float64"}` -
    `source_column` is how the column is named in the raw file, `name` is what it should be
    called in the lake (the same rename-to-a-stable-name idea as datalake-service's
    `serviceFields.mappedTo`, without needing the fixed-width/grouped-attribute machinery that
    powers).
    """
    if not rows:
        columns = [c['name'] for c in schema['columns']]
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows)
    renamed = {}
    for column in schema['columns']:
        source_column = column['source_column']
        if source_column not in df.columns:
            raise SchemaValidationError(f"Raw data is missing expected column {source_column!r}")
        renamed[source_column] = column['name']

    df = df.rename(columns=renamed)[[c['name'] for c in schema['columns']]]

    for column in schema['columns']:
        df[column['name']] = df[column['name']].astype(column['dtype'])

    return df


def write_partitioned(df: pd.DataFrame, lake_root: Path, source_system: str, generated_time: str) -> Path:
    """Write `df` as one Parquet file under `lake_root/source_system=.../year=YYYY/month=MM/`."""
    parsed_time = datetime.fromisoformat(generated_time)
    partition_dir = (
        Path(lake_root)
        / f'source_system={source_system}'
        / f'year={parsed_time.year:04d}'
        / f'month={parsed_time.month:02d}'
    )
    partition_dir.mkdir(parents=True, exist_ok=True)

    output_path = partition_dir / f'part-{uuid.uuid4().hex}.parquet'
    df.to_parquet(output_path, index=False)
    return output_path
