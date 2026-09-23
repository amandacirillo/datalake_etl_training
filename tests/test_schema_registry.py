import pytest

from apps.common.schema_registry import InMemorySchemaCache, SchemaNotFoundError, get_schema


def test_cache_hit_never_calls_fetch_schema():
    cache = InMemorySchemaCache()
    cache.put('orders-v1', {'columns': [{'name': 'order_id', 'source_column': 'ORDER_ID', 'dtype': 'string'}]})

    def fetch_schema(schema_id):
        raise AssertionError('fetch_schema should not be called on a cache hit')

    schema = get_schema('orders-v1', cache, fetch_schema)

    assert schema['columns'][0]['name'] == 'order_id'


def test_cache_miss_falls_back_to_fetch_and_populates_the_cache():
    cache = InMemorySchemaCache()
    fetched_ids = []

    def fetch_schema(schema_id):
        fetched_ids.append(schema_id)
        return {'columns': [{'name': 'order_id', 'source_column': 'ORDER_ID', 'dtype': 'string'}]}

    first = get_schema('orders-v1', cache, fetch_schema)
    second = get_schema('orders-v1', cache, fetch_schema)

    assert fetched_ids == ['orders-v1']  # only fetched once - the second call was a cache hit
    assert first == second


def test_schema_not_found_anywhere_propagates():
    cache = InMemorySchemaCache()

    def fetch_schema(schema_id):
        raise SchemaNotFoundError(f'no such schema {schema_id}')

    with pytest.raises(SchemaNotFoundError):
        get_schema('does-not-exist', cache, fetch_schema)
