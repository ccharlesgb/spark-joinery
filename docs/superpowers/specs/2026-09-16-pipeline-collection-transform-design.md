# Pipeline → Collection → Transform Design

## Scope

Introduce a `Collection` layer between `Pipeline` and individual transforms, analogous to FastAPI's `app -> routers -> routes`. `TransformSpec` state moves off the transform function (no more `setattr(fn, "__transform_spec__", spec)`) and into the owning `Collection`. Every `Pipeline` has a built-in default `Collection` for simple cases; additional `Collection` instances can be supplied for larger/multi-collection pipelines.

Out of scope: cross-collection auto-wiring, collection name prefixes/namespacing, `>>` operator sugar (still deferred per the original pipeline-wiring design).

## Public API

```python
from pyspark_schemas.collection import Collection
from pyspark_schemas.pipeline import Pipeline

# Simple case: built-in collection
pipeline = Pipeline()

@pipeline.transform
def read_orders(spark: SparkSession) -> Annotated[DataFrame, Order]:
    ...

orders = pipeline.add_step(read_orders, "orders")

# Complex case: explicit collection(s), fixed at construction
orders_collection = Collection()

@orders_collection.transform
def read_orders(spark: SparkSession) -> Annotated[DataFrame, Order]:
    ...

@orders_collection.transform(validate_output=False)
def filter_orders(orders: Annotated[DataFrame, Order]) -> Annotated[DataFrame, Order]:
    ...

pipeline = Pipeline(collections=[orders_collection])
orders = pipeline.add_step(read_orders, "orders")
filtered = pipeline.add_step(filter_orders, "filtered_orders")
pipeline.connect(orders, filtered)
```

`connect`, `connect_many`, `validate`, `ExecutablePipeline.run` are unchanged from the existing pipeline-wiring design — they only operate on `Step` objects.

## `Collection` (new file: `src/pyspark_schemas/collection.py`)

```python
class Collection:
    def __init__(self):
        self._specs: dict[Callable, TransformSpec] = {}

    def transform(
        self,
        f=None,
        *,
        validate_input: bool = True,
        validate_output: bool = True,
        ignore_nullable: bool = True,
    ):
        ...
```

Behavior is identical to today's `pyspark_schemas.transform.transform` decorator (same runtime input/output schema validation via `assertSchemaEqual`), except the computed `TransformSpec` is stored in `self._specs[wrapper]` instead of being set as an attribute on the wrapped function. No public getter is needed beyond what `Pipeline` uses internally (`Pipeline` reaches into `collection._specs`, or `Collection` exposes a narrow internal method `_get_spec(fn) -> TransformSpec | None`).

## `Pipeline` changes (`src/pyspark_schemas/pipeline.py`)

```python
class Pipeline:
    def __init__(self, collections: Sequence[Collection] = ()):
        self._collection = Collection()                     # built-in default
        self._collections = (self._collection, *collections)  # immutable, fixed at construction

    def transform(
        self,
        f=None,
        *,
        validate_input: bool = True,
        validate_output: bool = True,
        ignore_nullable: bool = True,
    ):
        return self._collection.transform(
            f,
            validate_input=validate_input,
            validate_output=validate_output,
            ignore_nullable=ignore_nullable,
        )

    def add_step(self, transform, name) -> Step:
        spec = self._resolve_spec(transform)   # searches self._collections in order
        ...
```

`Pipeline.transform` is a regular method with the full keyword signature mirrored from `Collection.transform` (not a `@property`), so `@pipeline.transform` and `@pipeline.transform(validate_input=False)` both work exactly like `@collection.transform(...)`.

`_resolve_spec(transform)` raises `TypeError` (same message shape as today's `get_transform_spec` error) if the function isn't registered in any of the pipeline's collections — this replaces the current "not decorated with @transform" check.

`collections` passed to the constructor are stored as an immutable tuple; there is no `include_collection`/`add_collection` method to mutate the set after construction.

## `transform.py` changes

Remove the module-level `transform` decorator and `get_transform_spec`. Keep `TransformSpec`, `_inspect_transform`, `_get_spark_parameter`, `_get_annotated_dataframe_schema_model` as internals imported by `collection.py`.

## Migration of existing code

- `examples/order_product/transformations.py`: replace `from pyspark_schemas.transform import transform` + `@transform` with a module-level `Collection()` instance and `@collection.transform`.
- `examples/order_product/pipeline.py`: construct `Pipeline(collections=[collection])` and use `pipeline.add_step(fn, name)` as today.
- `tests/test_transform.py`: replace direct `get_transform_spec(fn)` assertions with a `Collection`-based equivalent (register via `collection.transform`, assert against `collection._specs[wrapped_fn]` or a small internal accessor).
- `tests/test_pipeline.py`: existing tests using bare `@transform` switch to a `Collection` fixture.

## Testing

- `Collection.transform` computes the same `TransformSpec` as before (input/output schemas, spark parameter) — port existing `test_transform.py` cases.
- `Collection` does not set any attribute on the decorated function (regression test: `not hasattr(fn, "__transform_spec__")`).
- `Pipeline()` with no `collections` arg still supports `pipeline.transform` + `add_step` (built-in collection case).
- `Pipeline(collections=[c1, c2])` resolves specs from any supplied collection; `add_step` on a function not registered in any of the pipeline's collections raises `TypeError`.
- `pipeline.transform(validate_input=False)` / `validate_output=False` / `ignore_nullable=False` behave the same as the equivalent `Collection.transform(...)` kwargs.
