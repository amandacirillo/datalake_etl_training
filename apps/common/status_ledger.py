"""A training-sized reimplementation of datalake-service's fileevents status tracking: a
DynamoDB-backed ledger, keyed by `(source_system, batch_id)`, that records where a batch is in
its ingest lifecycle and - critically - lets an event-driven, multi-step pipeline know whether a
batch is ALREADY being processed, so a duplicate S3 event (S3 delivers "at least once", never
exactly once) doesn't kick off a second, wasteful, possibly-conflicting ingest run.

`StatusLedger` is written against the same handful of methods boto3's DynamoDB `Table` resource
exposes (`get_item`/`put_item`/`update_item`) - not against a `boto3.resource('dynamodb')` import -
so tests can inject an in-memory fake (see tests/test_status_ledger.py's `FakeTable`) with zero
AWS credentials, and the real Lambda can inject `boto3.resource('dynamodb').Table(...)` unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class IngestStatus:
    RECEIVED = 'RECEIVED'
    PREPROCESSING_STARTED = 'PREPROCESSING_STARTED'
    PREPROCESSED = 'PREPROCESSED'
    INGEST_STARTED = 'INGEST_STARTED'
    INGEST_READING_SOURCE = 'INGEST_READING_SOURCE'
    INGEST_WRITING_PARTITIONS = 'INGEST_WRITING_PARTITIONS'
    INGEST_COMPLETED = 'INGEST_COMPLETED'
    INGEST_COMPLETED_WITH_ERRORS = 'INGEST_COMPLETED_WITH_ERRORS'
    INGEST_FAILED = 'INGEST_FAILED'


# Explicit, rather than "everything not in ENDED_STATUSES" derived by introspection - a new status
# added to IngestStatus above without also being added to one of these lists fails loudly (a
# KeyError from is_ingest_in_progress) instead of silently being treated as "still in progress."
SUCCEEDED_STATUSES = {IngestStatus.INGEST_COMPLETED}
FAILED_STATUSES = {IngestStatus.INGEST_COMPLETED_WITH_ERRORS, IngestStatus.INGEST_FAILED}
IN_PROGRESS_STATUSES = {
    IngestStatus.RECEIVED,
    IngestStatus.PREPROCESSING_STARTED,
    IngestStatus.PREPROCESSED,
    IngestStatus.INGEST_STARTED,
    IngestStatus.INGEST_READING_SOURCE,
    IngestStatus.INGEST_WRITING_PARTITIONS,
}
ENDED_STATUSES = SUCCEEDED_STATUSES | FAILED_STATUSES

ALL_STATUSES = IN_PROGRESS_STATUSES | ENDED_STATUSES


class UnknownStatusError(Exception):
    """Raised when a status isn't in any of SUCCEEDED/FAILED/IN_PROGRESS - see the comment above."""


@dataclass(frozen=True)
class IngestRecord:
    source_system: str
    batch_id: str
    generated_time: str
    status: str
    status_details: Optional[str] = None


def _key(source_system: str, batch_id: str) -> Dict[str, str]:
    return {'source_system': source_system, 'batch_id': batch_id}


class StatusLedger:
    """Tracks one ingest record per `(source_system, batch_id)`, coded against the boto3 DynamoDB
    `Table` resource's method surface (`get_item`/`put_item`), not a live AWS connection.
    """

    def __init__(self, table: Any) -> None:
        self.table = table

    def get_record(self, source_system: str, batch_id: str) -> Optional[IngestRecord]:
        response = self.table.get_item(Key=_key(source_system, batch_id))
        item = response.get('Item')
        if item is None:
            return None
        return IngestRecord(
            source_system=item['source_system'],
            batch_id=item['batch_id'],
            generated_time=item['generated_time'],
            status=item['status'],
            status_details=item.get('status_details'),
        )

    def is_ingest_in_progress(self, source_system: str, batch_id: str, expected_generated_time: str) -> bool:
        """True if a record for this exact batch exists, matches the generated_time we expect,
        and hasn't reached a terminal (succeeded/failed) status yet.

        A DIFFERENT `generated_time` for the same `(source_system, batch_id)` means this is a
        newer re-delivery of the same batch (e.g. a corrected re-extract) - not a duplicate of an
        already-tracked one - so it's treated as not in progress, exactly like the real service.
        """
        record = self.get_record(source_system, batch_id)
        if record is None:
            return False
        if record.generated_time != expected_generated_time:
            return False
        if record.status not in ALL_STATUSES:
            raise UnknownStatusError(f"Unrecognized ingest status {record.status!r} for {source_system}/{batch_id}")
        return record.status in IN_PROGRESS_STATUSES

    def update_status(
        self,
        source_system: str,
        batch_id: str,
        generated_time: str,
        status: str,
        status_details: Optional[str] = None,
    ) -> IngestRecord:
        self.table.put_item(
            Item={
                'source_system': source_system,
                'batch_id': batch_id,
                'generated_time': generated_time,
                'status': status,
                'status_details': status_details,
                'status_updated_at': datetime.now(timezone.utc).isoformat(),
            }
        )
        return IngestRecord(source_system, batch_id, generated_time, status, status_details)
