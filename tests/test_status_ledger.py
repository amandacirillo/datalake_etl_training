import pytest

from apps.common.status_ledger import IngestStatus, StatusLedger, UnknownStatusError


class FakeTable:
    """Mimics enough of boto3's DynamoDB `Table` resource (`get_item`/`put_item`) for
    StatusLedger to work against, with zero AWS involved.
    """

    def __init__(self):
        self.items = {}

    def get_item(self, Key):
        key = (Key['source_system'], Key['batch_id'])
        item = self.items.get(key)
        return {'Item': item} if item is not None else {}

    def put_item(self, Item):
        self.items[(Item['source_system'], Item['batch_id'])] = Item


def test_update_status_then_get_record_round_trips():
    ledger = StatusLedger(FakeTable())

    ledger.update_status('orders', 'batch-1', '2024-01-15T00:00:00', IngestStatus.RECEIVED)
    record = ledger.get_record('orders', 'batch-1')

    assert record.source_system == 'orders'
    assert record.batch_id == 'batch-1'
    assert record.status == IngestStatus.RECEIVED


def test_get_record_returns_none_when_nothing_tracked_yet():
    ledger = StatusLedger(FakeTable())
    assert ledger.get_record('orders', 'unknown-batch') is None


def test_is_ingest_in_progress_true_for_an_in_progress_status():
    ledger = StatusLedger(FakeTable())
    ledger.update_status('orders', 'batch-1', '2024-01-15T00:00:00', IngestStatus.INGEST_STARTED)

    assert ledger.is_ingest_in_progress('orders', 'batch-1', '2024-01-15T00:00:00') is True


def test_is_ingest_in_progress_false_for_a_terminal_status():
    ledger = StatusLedger(FakeTable())
    ledger.update_status('orders', 'batch-1', '2024-01-15T00:00:00', IngestStatus.INGEST_COMPLETED)

    assert ledger.is_ingest_in_progress('orders', 'batch-1', '2024-01-15T00:00:00') is False


def test_is_ingest_in_progress_false_when_no_record_exists():
    ledger = StatusLedger(FakeTable())
    assert ledger.is_ingest_in_progress('orders', 'never-seen', '2024-01-15T00:00:00') is False


def test_is_ingest_in_progress_false_when_generated_time_does_not_match():
    """A different generated_time means a newer re-delivery of the batch, not a duplicate of the
    one already being tracked."""
    ledger = StatusLedger(FakeTable())
    ledger.update_status('orders', 'batch-1', '2024-01-15T00:00:00', IngestStatus.INGEST_STARTED)

    assert ledger.is_ingest_in_progress('orders', 'batch-1', '2024-02-01T00:00:00') is False


def test_unknown_status_raises_instead_of_silently_treating_as_in_progress():
    ledger = StatusLedger(FakeTable())
    ledger.update_status('orders', 'batch-1', '2024-01-15T00:00:00', 'SOME_NEW_STATUS_NOBODY_REGISTERED')

    with pytest.raises(UnknownStatusError):
        ledger.is_ingest_in_progress('orders', 'batch-1', '2024-01-15T00:00:00')
