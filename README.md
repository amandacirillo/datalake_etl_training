# ETL Into a Data Lake Training

A small, runnable model of the **event-driven, idempotent ETL pipeline** pattern used by
`datalake-service`. It reimplements the shape of that pipeline against a generic **"raw extract
files from various source systems"** domain instead of the real proprietary fixed-width-file
parsing/scoring logic - the goal is to teach the *pattern*, safely.

## What This Teaches

1. **Idempotent, single-step handlers that re-trigger themselves via their own side effects.**
   `apps/fileevent_handler/handler.py` handles the same S3-event Lambda invocation completely
   differently depending on what the object's OWN metadata/location currently says: (a) not at
   its canonical `source_system/year/filename` path yet -> move it there and stop; (b) at its
   canonical path but not preprocessed -> preprocess it, flag it, and stop; (c) preprocessed and
   in place -> enqueue it for ingest. Each of the first two phases relies on its own S3 write
   (a copy, a metadata update) firing a brand-new S3 event that re-invokes this same handler and
   picks up the next phase - no phase calls the next one directly. This is the single most
   reusable idea in the whole pipeline: it turns "one big handler with a lot of branches" into
   "a state machine expressed entirely through where a file is and what it says about itself."
2. **A DynamoDB status ledger that makes at-least-once delivery safe.** S3 (and SQS) deliver
   events *at least once*, never exactly once - the same object can legitimately trigger the
   handler twice. `apps/common/status_ledger.py`'s `is_ingest_in_progress()` is what makes a
   duplicate delivery a no-op instead of a duplicate ingest run, by checking both the tracked
   status AND the `generated_time` (a different `generated_time` for the same batch means a
   genuinely newer re-extract, not a duplicate, and should proceed).
3. **Explicit status sets instead of introspection.** The real service derives "is this status
   still in progress" by inspecting `dir(IngestStatus)` and filtering out known-terminal names by
   convention - add a new status without also updating that filter and it silently misbehaves.
   This training's `SUCCEEDED_STATUSES`/`FAILED_STATUSES`/`IN_PROGRESS_STATUSES` are hand-
   maintained sets, and an unrecognized status raises `UnknownStatusError` instead of guessing.
4. **Cache-aside schema lookups.** `apps/common/schema_registry.get_schema()` mirrors the real
   `get_layout()`'s pattern exactly: try the cache, fall back to fetching from a (slow, external)
   registry only on a miss, and write the fetched result back so the next lookup for the same
   schema id is fast - all fully testable with a plain dict cache and a fake fetch function.
5. **Hive-style partitioned Parquet output**, so a query engine that understands partitioning
   (Athena, Spark, DuckDB, `pd.read_parquet` with filters) can skip entire directories that don't
   match a query instead of scanning every row ever ingested - see
   `apps/ingest_worker/partitioning.write_partitioned()`.
6. **Everything AWS-shaped is injected, nothing is imported.** No module in `apps/` imports
   `boto3` - `StatusLedger` is written against the same `get_item`/`put_item` method surface
   boto3's DynamoDB `Table` resource exposes, and every S3/SQS-like operation the handlers need
   (`head_object`, `copy_object`, `enqueue_ingest`, `read_raw_rows`, ...) is a plain injected
   function. Tests exercise real control flow with in-memory fakes, no AWS credentials, no
   network.

## Project Layout

```
apps/common/
  status_ledger.py      IngestStatus, StatusLedger, IngestRecord - the DynamoDB-backed status ledger
  schema_registry.py     cache-aside get_schema(), InMemorySchemaCache

apps/fileevent_handler/
  handler.py             the 3-phase, self-retriggering S3 event handler

apps/ingest_worker/
  partitioning.py         apply_schema() + write_partitioned() (pandas + pyarrow)
  handler.py              the SQS-triggered worker tying schema lookup + partitioning + status together

tests/
  test_status_ledger.py       idempotency, terminal-status detection, unknown-status handling
  test_schema_registry.py     cache hit / cache miss / not-found-anywhere
  test_fileevent_handler.py   all three phases, plus the duplicate-event idempotency case
  test_partitioning.py        schema application + Hive-style partition layout
  test_ingest_worker.py       end-to-end job processing, failure path marks INGEST_FAILED

cdk/
  lib/datalake_stack.ts       raw bucket, lake bucket, status table, ingest queue + DLQ, both Lambdas
  test/datalake_stack.test.ts Jest + aws-cdk-lib/assertions structural tests
```

## Try It

### Run the Python tests
```powershell
cd C:\PythonProjects\datalake_etl_training
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt -r requirements-dev.txt
.\.venv\Scripts\python -m pytest -v
```
23 tests, all offline - a fake DynamoDB table, an in-memory schema cache, and `tmp_path` for the
lake bucket. No AWS credentials, no network.

### Walk the fileevent handler through all three phases yourself
```powershell
.\.venv\Scripts\python -c "
from apps.common.status_ledger import StatusLedger
from apps.fileevent_handler.handler import handle_object_created

class FakeTable:
    def __init__(self): self.items = {}
    def get_item(self, Key):
        item = self.items.get((Key['source_system'], Key['batch_id']))
        return {'Item': item} if item else {}
    def put_item(self, Item):
        self.items[(Item['source_system'], Item['batch_id'])] = Item

objects = {'uploads/f.csv': {'Metadata': {
    'source_system': 'orders', 'batch_id': 'b1',
    'generated_time': '2024-03-10T00:00:00', 'schema_id': 'orders-v1'}}}

result = handle_object_created(
    'raw-bucket', 'uploads/f.csv', StatusLedger(FakeTable()),
    head_object=lambda b, k: objects[k],
    copy_object=lambda b, s, d, m: objects.update({d: {'Metadata': dict(m)}}),
    update_metadata=lambda b, k, u: objects[k]['Metadata'].update(u),
    preprocess_file=lambda b, k: None,
    enqueue_ingest=lambda msg: print('ENQUEUED:', msg),
)
print(result)
"
```
Run the same object key again after each phase and watch the `action` in the result change:
`moved_to_canonical_path` -> `preprocessed` -> `ingest_triggered` -> `skipped_already_in_progress`.

### Type-check + test the CDK stack (no AWS account needed)
```powershell
cd cdk
npm install
npx tsc --noEmit
npx jest
```

### Inspect the synthesized stack
```powershell
npx cdk synth --quiet
```

## Exercises (for training)

1. **Add a fourth phase: schema validation before enqueue.** Right now a bad `schema_id` isn't
   discovered until the ingest worker calls `get_schema()` and it 404s. Add a phase to the
   fileevent handler that validates the schema exists (via an injected `schema_exists(schema_id)`)
   before enqueuing, and update the status ledger with a new terminal status
   (`INVALID_SCHEMA`) if it doesn't - remember to add it to `FAILED_STATUSES`, not just
   `IngestStatus`.
2. **Reproduce the real service's `is_ingest_in_progress` edge case, then fix it a different way.**
   The current check treats a different `generated_time` as "not in progress" unconditionally.
   What happens if two different generated_times for the same batch arrive nearly
   simultaneously? Sketch (or implement) a version that instead tracks in-progress runs per
   `(source_system, batch_id, generated_time)` triple, and discuss the DynamoDB key-schema change
   that requires.
3. **Add partial-failure handling to the ingest worker.** Currently one bad row anywhere in
   `apply_schema()` fails the whole batch. Change `apply_schema` to return `(good_df, bad_rows)`
   instead of raising, write `bad_rows` to a `.../rejected/` partition, and update the status to
   `INGEST_COMPLETED_WITH_ERRORS` when there were any.
4. **Reintroduce distributed processing for real.** This training deliberately uses a single-
   threaded pandas pipeline. Sketch what changes if `read_raw_rows` instead has to handle a
   50GB file - would you shard by row range, by partition key, something else? (See
   `stepfunctions_training` for the fan-out/fan-in orchestration pattern you'd likely reach for.)
5. **Wire the real Lambda handlers into the CDK stack.** `cdk/lib/datalake_stack.ts` currently
   uses `lambda.Code.fromInline` placeholders for both functions. Package `apps/` (plus
   `pandas`/`pyarrow` as a Lambda layer) and point `lambda.Code.fromAsset(...)` at it instead -
   and add a CDK-level test asserting the real handler path (`fileevent_handler.handler.handler`)
   is configured, not just that *a* handler exists.
