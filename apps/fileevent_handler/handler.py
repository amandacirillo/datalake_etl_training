"""The S3-event-triggered entry point of the ingest pipeline - a training-sized reimplementation
of datalake-service's `fileevents_lambda.handler`.

This one Lambda handles THREE different phases of a file's life, distinguishing them purely by
"what does this file's own S3 metadata/location say right now" - and, critically, each phase that
isn't the final one modifies the file (moves it, or updates its metadata) and returns WITHOUT
calling the next stage directly. That modification itself fires a brand new S3 event, which
re-invokes this same handler, which re-inspects the file and picks up in the next phase. This
"idempotent single-step handler, chained by its own side effects re-triggering itself" pattern is
the one genuinely reusable idea worth taking from the real service's much more complex handler:

  1. **Not at its canonical path yet** -> move it there (preserving metadata), return.
     (fan-in: however many raw prefixes/upload paths exist, everything ends up organized the
     same way before any parsing happens)
  2. **At its canonical path, not yet preprocessed** -> preprocess it, mark `preprocessed=true`
     in its S3 metadata, return.
  3. **At its canonical path AND preprocessed** -> check the status ledger for an already-in-
     -flight ingest of this exact batch (idempotency - S3 delivers events at-least-once, so the
     same object can legitimately trigger this handler more than once); if none, enqueue an
     ingest job and mark the batch INGEST_STARTED.

Every AWS-shaped dependency is injected, so tests exercise all three phases without touching S3,
DynamoDB, or SQS.
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from apps.common.status_ledger import IngestStatus, StatusLedger

REQUIRED_METADATA_KEYS = ('source_system', 'batch_id', 'generated_time', 'schema_id')

# Prefixes containing schema definitions themselves, not data to ingest - mirrors the real
# handler's `if source_file.startswith('layouts/')` early-exit.
IGNORED_KEY_PREFIXES = ('schemas/',)


class InvalidObjectMetadataError(Exception):
    """Raised when an S3 object is missing one of the metadata keys the pipeline requires."""


HeadObject = Callable[[str, str], Dict[str, Any]]  # (bucket, key) -> {"Metadata": {...}}
CopyObject = Callable[[str, str, str, Dict[str, str]], None]  # (bucket, source_key, dest_key, metadata)
UpdateMetadata = Callable[[str, str, Dict[str, str]], None]  # (bucket, key, metadata_updates)
PreprocessFile = Callable[[str, str], None]  # (bucket, key) - normalizes the file content in place
EnqueueIngest = Callable[[Dict[str, Any]], None]


def _require_metadata(metadata: Dict[str, str]) -> None:
    missing = [key for key in REQUIRED_METADATA_KEYS if key not in metadata]
    if missing:
        raise InvalidObjectMetadataError(f"Object metadata is missing required key(s): {missing}")


def _canonical_key(source_system: str, generated_time: str, original_key: str) -> str:
    year = datetime.fromisoformat(generated_time).year
    filename = os.path.basename(original_key)
    return f'{source_system}/{year}/{filename}'


def handle_object_created(
    bucket: str,
    key: str,
    status_ledger: StatusLedger,
    head_object: HeadObject,
    copy_object: CopyObject,
    update_metadata: UpdateMetadata,
    preprocess_file: PreprocessFile,
    enqueue_ingest: EnqueueIngest,
) -> Dict[str, Any]:
    if key.startswith(IGNORED_KEY_PREFIXES):
        return {'action': 'ignored', 'reason': 'schema definition object, not ingestible data'}

    file_head = head_object(bucket, key)
    metadata = file_head['Metadata']
    _require_metadata(metadata)

    source_system = metadata['source_system']
    batch_id = metadata['batch_id']
    generated_time = metadata['generated_time']
    schema_id = metadata['schema_id']

    canonical_key = _canonical_key(source_system, generated_time, key)

    if key != canonical_key:
        copy_object(bucket, key, canonical_key, metadata)
        status_ledger.update_status(source_system, batch_id, generated_time, IngestStatus.RECEIVED)
        return {'action': 'moved_to_canonical_path', 'from': key, 'to': canonical_key}

    if metadata.get('preprocessed') != 'true':
        status_ledger.update_status(source_system, batch_id, generated_time, IngestStatus.PREPROCESSING_STARTED)
        preprocess_file(bucket, key)
        update_metadata(bucket, key, {'preprocessed': 'true'})
        status_ledger.update_status(source_system, batch_id, generated_time, IngestStatus.PREPROCESSED)
        return {'action': 'preprocessed', 'key': key}

    if status_ledger.is_ingest_in_progress(source_system, batch_id, generated_time):
        return {'action': 'skipped_already_in_progress', 'source_system': source_system, 'batch_id': batch_id}

    enqueue_ingest({
        'source_system': source_system,
        'batch_id': batch_id,
        'generated_time': generated_time,
        'schema_id': schema_id,
        'bucket': bucket,
        'key': key,
    })
    status_ledger.update_status(source_system, batch_id, generated_time, IngestStatus.INGEST_STARTED)
    return {'action': 'ingest_triggered', 'source_system': source_system, 'batch_id': batch_id}


def handler(
    event: Dict[str, Any],
    _context: Any = None,
    status_ledger: Optional[StatusLedger] = None,
    head_object: Optional[HeadObject] = None,
    copy_object: Optional[CopyObject] = None,
    update_metadata: Optional[UpdateMetadata] = None,
    preprocess_file: Optional[PreprocessFile] = None,
    enqueue_ingest: Optional[EnqueueIngest] = None,
) -> Dict[str, Any]:
    """S3 event handler - one `Records[0].s3` block per invocation, matching how S3 -> Lambda
    event notifications are actually delivered (never batched, unlike the SQS handler downstream).
    """
    record = event['Records'][0]['s3']
    bucket = record['bucket']['name']
    key = record['object']['key']

    return handle_object_created(
        bucket,
        key,
        status_ledger=status_ledger,
        head_object=head_object,
        copy_object=copy_object,
        update_metadata=update_metadata,
        preprocess_file=preprocess_file,
        enqueue_ingest=enqueue_ingest,
    )
