import pandas as pd
import pytest

from apps.ingest_worker.partitioning import SchemaValidationError, apply_schema, write_partitioned

SCHEMA = {
    'columns': [
        {'name': 'order_id', 'source_column': 'ORDER_ID', 'dtype': 'string'},
        {'name': 'order_total', 'source_column': 'ORDER_TOTAL', 'dtype': 'float64'},
    ]
}


def test_apply_schema_renames_and_casts_columns():
    rows = [{'ORDER_ID': 'A1', 'ORDER_TOTAL': '19.99'}, {'ORDER_ID': 'A2', 'ORDER_TOTAL': '4.50'}]

    df = apply_schema(rows, SCHEMA)

    assert list(df.columns) == ['order_id', 'order_total']
    assert df['order_total'].dtype == 'float64'
    assert df['order_total'].tolist() == [19.99, 4.50]


def test_apply_schema_raises_on_missing_source_column():
    rows = [{'ORDER_ID': 'A1'}]

    with pytest.raises(SchemaValidationError):
        apply_schema(rows, SCHEMA)


def test_apply_schema_returns_empty_dataframe_with_correct_columns_when_no_rows():
    df = apply_schema([], SCHEMA)
    assert list(df.columns) == ['order_id', 'order_total']
    assert len(df) == 0


def test_write_partitioned_lays_out_hive_style_partitions(tmp_path):
    df = pd.DataFrame({'order_id': ['A1'], 'order_total': [19.99]})

    output_path = write_partitioned(df, tmp_path, 'orders', '2024-03-10T00:00:00')

    assert output_path.exists()
    assert output_path.parent == tmp_path / 'source_system=orders' / 'year=2024' / 'month=03'

    round_tripped = pd.read_parquet(output_path)
    assert round_tripped['order_id'].tolist() == ['A1']
