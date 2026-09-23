import json

import pandas as pd

from apps.common.schema_registry import InMemorySchemaCache
from apps.common.status_ledger import IngestStatus, StatusLedger
from apps.ingest_worker.handler import handler, process_ingest_job

SCHEMA = {
    'columns': [
        {'name': 'order_id', 'source_column': 'ORDER_ID', 'dtype': 'string'},
        {'name': 'order_total', 'source_column': 'ORDER_TOTAL', 'dtype': 'float64'},
    ]
}


class FakeTable:
    def __init__(self):
        self.items = {}

    def get_item(self, Key):
        item = self.items.get((Key['source_system'], Key['batch_id']))
        return {'Item': item} if item is not None else {}

    def put_item(self, Item):
        self.items[(Item['source_system'], Item['batch_id'])] = Item


def _job():
    return {
        'source_system': 'orders',
        'batch_id': 'batch-1',
        'generated_time': '2024-03-10T00:00:00',
        'schema_id': 'orders-v1',
        'bucket': 'raw-bucket',
        'key': 'orders/2024/f.csv',
    }


def test_process_ingest_job_writes_partitioned_output_and_marks_completed(tmp_path):
    ledger = StatusLedger(FakeTable())
    cache = InMemorySchemaCache()

    def fetch_schema(schema_id):
        return SCHEMA

    def read_raw_rows(bucket, key):
        return [{'ORDER_ID': 'A1', 'ORDER_TOTAL': '19.99'}]

    result = process_ingest_job(_job(), ledger, cache, fetch_schema, read_raw_rows, tmp_path)

    assert result['status'] == 'INGEST_COMPLETED'
    assert result['rows_written'] == 1
    record = ledger.get_record('orders', 'batch-1')
    assert record.status == IngestStatus.INGEST_COMPLETED

    written = pd.read_parquet(result['output_path'])
    assert written['order_id'].tolist() == ['A1']


def test_process_ingest_job_marks_failed_and_reraises_on_error(tmp_path):
    ledger = StatusLedger(FakeTable())
    cache = InMemorySchemaCache()

    def fetch_schema(schema_id):
        return SCHEMA

    def read_raw_rows(bucket, key):
        return [{'ORDER_ID': 'A1'}]  # missing ORDER_TOTAL -> SchemaValidationError

    try:
        process_ingest_job(_job(), ledger, cache, fetch_schema, read_raw_rows, tmp_path)
        assert False, 'expected an exception to propagate'
    except Exception:
        pass

    record = ledger.get_record('orders', 'batch-1')
    assert record.status == IngestStatus.INGEST_FAILED
    assert 'ORDER_TOTAL' in record.status_details


def test_handler_processes_a_batch_of_sqs_records(tmp_path):
    ledger = StatusLedger(FakeTable())
    cache = InMemorySchemaCache()

    event = {'Records': [{'body': json.dumps(_job())}]}

    result = handler(
        event,
        status_ledger=ledger,
        schema_cache=cache,
        fetch_schema=lambda schema_id: SCHEMA,
        read_raw_rows=lambda bucket, key: [{'ORDER_ID': 'A1', 'ORDER_TOTAL': '5.00'}],
        lake_root=tmp_path,
    )

    assert len(result['processed']) == 1
    assert result['processed'][0]['status'] == 'INGEST_COMPLETED'
