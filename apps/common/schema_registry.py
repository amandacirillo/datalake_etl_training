"""A training-sized reimplementation of datalake-service's `layout.py get_layout()`: a schema is
identified by an id, looked up in a cache first, and only fetched from the (slow, external, rate-
limited) schema registry service on a cache miss - after which the fetched schema is written back
to the cache so the NEXT ingest for the same schema id is fast.

Every dependency here is injected (`cache`, `fetch_schema`) rather than imported directly (no
`boto3`, no `requests`) - tests exercise the real cache-aside control flow (hit vs. miss vs.
"not found anywhere") with plain Python dicts and functions, no network, no AWS.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Protocol


class SchemaNotFoundError(Exception):
    """Raised when a schema id isn't in the cache AND the registry doesn't have it either."""


class SchemaCache(Protocol):
    def get(self, schema_id: str) -> Dict[str, Any]: ...  # raises KeyError on a miss
    def put(self, schema_id: str, schema: Dict[str, Any]) -> None: ...


FetchSchema = Callable[[str], Dict[str, Any]]  # raises SchemaNotFoundError if the registry doesn't have it


class InMemorySchemaCache:
    """A plain-dict cache - stands in for what would be an S3-object-as-cache in production
    (see datalake-service's `get_file_contents_from_s3_or_local`/`put_file_contents_to_s3_or_local`).
    """

    def __init__(self) -> None:
        self._schemas: Dict[str, Dict[str, Any]] = {}

    def get(self, schema_id: str) -> Dict[str, Any]:
        return self._schemas[schema_id]

    def put(self, schema_id: str, schema: Dict[str, Any]) -> None:
        self._schemas[schema_id] = schema


def get_schema(schema_id: str, cache: SchemaCache, fetch_schema: FetchSchema) -> Dict[str, Any]:
    """Cache-aside: try the cache first, fall back to `fetch_schema` on a miss, cache the result.

    A schema looks like `{"columns": [{"name": ..., "dtype": ..., "source_column": ...}, ...]}` -
    see apps/ingest_worker/partitioning.py for how it's applied to a raw dataframe.
    """
    try:
        return cache.get(schema_id)
    except KeyError:
        pass

    schema = fetch_schema(schema_id)  # may raise SchemaNotFoundError - deliberately not caught here
    cache.put(schema_id, schema)
    return schema
