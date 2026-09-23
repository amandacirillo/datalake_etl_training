"""The SQS-triggered worker that turns one enqueued ingest job into partitioned Parquet in the
lake - a training-sized reimplementation of datalake-service's Dask-based ingest step, without
the distributed-cluster machinery (see the README's exercises for how you'd add that back for a
real large-file/many-files-at-once workload; a single-threaded pandas pipeline is the right size
for teaching the schema-application + partitioning + status-tracking ideas on their own).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, List

from apps.common.schema_registry import FetchSchema, SchemaCache, get_schema
from apps.common.status_ledger import IngestStatus, StatusLedger
from apps.ingest_worker.partitioning import apply_schema, write_partitioned

ReadRawRows = Callable[[str, str], List[Dict[str, Any]]]  # (bucket, key) -> raw rows


def process_ingest_job(
    job: Dict[str, Any],
    status_ledger: StatusLedger,
    schema_cache: SchemaCache,
    fetch_schema: FetchSchema,
    read_raw_rows: ReadRawRows,
    lake_root: Path,
) -> Dict[str, Any]:
    source_system = job['source_system']
    batch_id = job['batch_id']
    generated_time = job['generated_time']

    try:
        status_ledger.update_status(source_system, batch_id, generated_time, IngestStatus.INGEST_READING_SOURCE)
        schema = get_schema(job['schema_id'], schema_cache, fetch_schema)
        raw_rows = read_raw_rows(job['bucket'], job['key'])
        df = apply_schema(raw_rows, schema)

        status_ledger.update_status(source_system, batch_id, generated_time, IngestStatus.INGEST_WRITING_PARTITIONS)
        output_path = write_partitioned(df, lake_root, source_system, generated_time)

        status_ledger.update_status(source_system, batch_id, generated_time, IngestStatus.INGEST_COMPLETED)
        return {'status': 'INGEST_COMPLETED', 'rows_written': len(df), 'output_path': str(output_path)}

    except Exception as exc:
        status_ledger.update_status(
            source_system, batch_id, generated_time, IngestStatus.INGEST_FAILED, status_details=str(exc)
        )
        raise


def handler(
    event: Dict[str, Any],
    _context: Any = None,
    status_ledger: StatusLedger = None,
    schema_cache: SchemaCache = None,
    fetch_schema: FetchSchema = None,
    read_raw_rows: ReadRawRows = None,
    lake_root: Path = None,
) -> Dict[str, Any]:
    """SQS event handler - `event['Records']` is a batch of `{"body": "...json job..."}` messages."""
    results = []
    for record in event['Records']:
        job = json.loads(record['body'])
        results.append(
            process_ingest_job(job, status_ledger, schema_cache, fetch_schema, read_raw_rows, lake_root)
        )
    return {'processed': results}
