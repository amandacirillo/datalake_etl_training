import pytest

from apps.common.status_ledger import IngestStatus, StatusLedger
from apps.fileevent_handler.handler import InvalidObjectMetadataError, handle_object_created


class FakeTable:
    def __init__(self):
        self.items = {}

    def get_item(self, Key):
        item = self.items.get((Key['source_system'], Key['batch_id']))
        return {'Item': item} if item is not None else {}

    def put_item(self, Item):
        self.items[(Item['source_system'], Item['batch_id'])] = Item


class Collaborators:
    """Records every AWS-shaped call the handler makes, so tests can assert on behavior without
    a real S3/SQS in the loop.
    """

    def __init__(self, objects):
        self.objects = objects  # key -> {"Metadata": {...}}
        self.copies = []
        self.metadata_updates = []
        self.preprocessed = []
        self.enqueued = []

    def head_object(self, bucket, key):
        return self.objects[key]

    def copy_object(self, bucket, source_key, dest_key, metadata):
        self.copies.append((source_key, dest_key))
        self.objects[dest_key] = {'Metadata': dict(metadata)}

    def update_metadata(self, bucket, key, updates):
        self.metadata_updates.append((key, updates))
        self.objects[key]['Metadata'].update(updates)

    def preprocess_file(self, bucket, key):
        self.preprocessed.append(key)

    def enqueue_ingest(self, message):
        self.enqueued.append(message)


BASE_METADATA = {
    'source_system': 'orders',
    'batch_id': 'batch-1',
    'generated_time': '2024-03-10T00:00:00',
    'schema_id': 'orders-v1',
}


def test_ignores_schema_definition_objects():
    collaborators = Collaborators({})
    ledger = StatusLedger(FakeTable())

    result = handle_object_created(
        'raw-bucket', 'schemas/orders-v1.json', ledger,
        collaborators.head_object, collaborators.copy_object, collaborators.update_metadata,
        collaborators.preprocess_file, collaborators.enqueue_ingest,
    )

    assert result['action'] == 'ignored'


def test_raises_on_missing_required_metadata():
    collaborators = Collaborators({'uploads/f.csv': {'Metadata': {'source_system': 'orders'}}})
    ledger = StatusLedger(FakeTable())

    with pytest.raises(InvalidObjectMetadataError):
        handle_object_created(
            'raw-bucket', 'uploads/f.csv', ledger,
            collaborators.head_object, collaborators.copy_object, collaborators.update_metadata,
            collaborators.preprocess_file, collaborators.enqueue_ingest,
        )


def test_phase_one_moves_file_to_canonical_path_and_does_not_enqueue():
    collaborators = Collaborators({'uploads/f.csv': {'Metadata': dict(BASE_METADATA)}})
    ledger = StatusLedger(FakeTable())

    result = handle_object_created(
        'raw-bucket', 'uploads/f.csv', ledger,
        collaborators.head_object, collaborators.copy_object, collaborators.update_metadata,
        collaborators.preprocess_file, collaborators.enqueue_ingest,
    )

    assert result['action'] == 'moved_to_canonical_path'
    assert result['to'] == 'orders/2024/f.csv'
    assert collaborators.copies == [('uploads/f.csv', 'orders/2024/f.csv')]
    assert collaborators.enqueued == []


def test_phase_two_preprocesses_file_already_at_canonical_path_and_does_not_enqueue():
    collaborators = Collaborators({'orders/2024/f.csv': {'Metadata': dict(BASE_METADATA)}})
    ledger = StatusLedger(FakeTable())

    result = handle_object_created(
        'raw-bucket', 'orders/2024/f.csv', ledger,
        collaborators.head_object, collaborators.copy_object, collaborators.update_metadata,
        collaborators.preprocess_file, collaborators.enqueue_ingest,
    )

    assert result['action'] == 'preprocessed'
    assert collaborators.preprocessed == ['orders/2024/f.csv']
    assert collaborators.metadata_updates == [('orders/2024/f.csv', {'preprocessed': 'true'})]
    assert collaborators.enqueued == []


def test_phase_three_enqueues_ingest_once_preprocessed_and_at_canonical_path():
    metadata = dict(BASE_METADATA, preprocessed='true')
    collaborators = Collaborators({'orders/2024/f.csv': {'Metadata': metadata}})
    ledger = StatusLedger(FakeTable())

    result = handle_object_created(
        'raw-bucket', 'orders/2024/f.csv', ledger,
        collaborators.head_object, collaborators.copy_object, collaborators.update_metadata,
        collaborators.preprocess_file, collaborators.enqueue_ingest,
    )

    assert result['action'] == 'ingest_triggered'
    assert len(collaborators.enqueued) == 1
    assert collaborators.enqueued[0]['source_system'] == 'orders'


def test_phase_three_is_idempotent_for_a_duplicate_s3_event():
    metadata = dict(BASE_METADATA, preprocessed='true')
    collaborators = Collaborators({'orders/2024/f.csv': {'Metadata': metadata}})
    ledger = StatusLedger(FakeTable())
    ledger.update_status('orders', 'batch-1', BASE_METADATA['generated_time'], IngestStatus.INGEST_STARTED)

    result = handle_object_created(
        'raw-bucket', 'orders/2024/f.csv', ledger,
        collaborators.head_object, collaborators.copy_object, collaborators.update_metadata,
        collaborators.preprocess_file, collaborators.enqueue_ingest,
    )

    assert result['action'] == 'skipped_already_in_progress'
    assert collaborators.enqueued == []
