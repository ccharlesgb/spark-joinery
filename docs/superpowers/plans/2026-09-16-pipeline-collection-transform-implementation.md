# Pipeline Collection Transform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `Collection` layer between `Pipeline` and transforms so `TransformSpec` lives on the collection instead of monkey-patched onto the transform function.

**Architecture:** New `Collection` class owns a `dict[Callable, TransformSpec]` and exposes a `.transform` decorator with the same runtime-validation behavior as today's `@transform`. `Pipeline` gets an immutable built-in `Collection` plus an optional `collections` constructor arg, a `transform` method that delegates to its built-in collection, and resolves specs by searching its collections instead of reading `__transform_spec__`.

**Tech Stack:** Python, pytest, pyspark.

**Spec:** [docs/superpowers/specs/2026-09-16-pipeline-collection-transform-design.md](../specs/2026-09-16-pipeline-collection-transform-design.md)

## Global Constraints

- `Collection.transform` must not set any attribute on the wrapped function (no `__transform_spec__`).
- `Pipeline(collections=...)` is fixed at construction; no method to add collections afterward.
- `Pipeline.transform` is a plain method with the full `(f=None, *, validate_input=True, validate_output=True, ignore_nullable=True)` signature — not a `@property`.
- `connect`/`connect_many`/`validate`/`ExecutablePipeline.run` behavior is unchanged.

---

### Task 1: `Collection` class

**Files:**
- Create: `src/pyspark_schemas/collection.py`
- Test: `tests/test_collection.py`

**Interfaces:**
- Consumes: `pyspark_schemas.transform.TransformSpec`, `pyspark_schemas.transform._inspect_transform` (existing internals, unchanged).
- Produces: `Collection` class with `.transform(f=None, *, validate_input=True, validate_output=True, ignore_nullable=True)` and an internal `_specs: dict[Callable, TransformSpec]` used by `Pipeline` in Task 3.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_collection.py
from dataclasses import dataclass
from typing import Annotated, Generator

import pytest
from pyspark.sql import DataFrame, SparkSession, types

from pyspark_schemas import fixtures, schemas
from pyspark_schemas.collection import Collection


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    spark = (
        SparkSession.builder.appName("pyspark-schemas-collection-tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_transform_accepts_matching_input_and_output_schemas(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int
        field2: str

    @dataclass
    class OutputRow:
        field1: int

    input_schema = schemas.get_spark_schema_from_dataclass(InputRow)
    output_schema = schemas.get_spark_schema_from_dataclass(OutputRow)
    input_df = spark.createDataFrame([(1, "a")], input_schema)

    collection = Collection()

    @collection.transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return input1.select("field1")

    result = my_function(input_df)
    assert result.schema == output_schema


def test_transform_raises_for_input_schema_mismatch(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int
        field2: str

    bad_input_df = spark.createDataFrame(
        [(1,)],
        types.StructType([types.StructField("field1", types.IntegerType(), False)]),
    )

    collection = Collection()

    @collection.transform
    def my_function(input1: Annotated[DataFrame, InputRow]):
        return input1

    with pytest.raises(ValueError, match="Schema mismatch for parameter 'input1'"):
        my_function(bad_input_df)


def test_transform_raises_for_output_schema_mismatch(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int
        field2: str

    @dataclass
    class OutputRow:
        field1: int

    input_df = fixtures.get_dataframe(spark, InputRow, [InputRow(1, "a")])

    collection = Collection()

    @collection.transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return input1

    with pytest.raises(ValueError, match="Return schema mismatch for 'my_function'"):
        my_function(input_df)


def test_transform_raises_for_non_dataframe_return(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int

    @dataclass
    class OutputRow:
        field1: int

    input_df = fixtures.get_dataframe(spark, InputRow, [InputRow(1)])

    collection = Collection()

    @collection.transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return "not a dataframe"  # type: ignore

    with pytest.raises(
        TypeError,
        match="Return value from 'my_function' must be a pyspark.sql.DataFrame",
    ):
        my_function(input_df)


def test_transform_parameterized_no_args_still_validates(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int
        field2: str

    bad_input_df = spark.createDataFrame(
        [(1,)],
        types.StructType([types.StructField("field1", types.IntegerType(), False)]),
    )

    collection = Collection()

    @collection.transform()
    def my_function(input1: Annotated[DataFrame, InputRow]):
        return input1

    with pytest.raises(ValueError, match="Schema mismatch for parameter 'input1'"):
        my_function(bad_input_df)


def test_transform_can_disable_output_validation(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int

    @dataclass
    class OutputRow:
        field1: int

    input_df = fixtures.get_dataframe(spark, InputRow, [InputRow(1)])

    collection = Collection()

    @collection.transform(validate_output=False)
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return "not a dataframe"  # type: ignore

    assert my_function(input_df) == "not a dataframe"


def test_transform_accepts_pydantic_input_and_output_schemas(spark: SparkSession):
    from pydantic import BaseModel

    class InputRow(BaseModel):
        field1: int
        field2: str

    class OutputRow(BaseModel):
        field1: int

    input_schema = schemas.get_spark_schema_from_model(InputRow)
    output_schema = schemas.get_spark_schema_from_model(OutputRow)
    input_df = spark.createDataFrame([(1, "a")], input_schema)

    collection = Collection()

    @collection.transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return input1.select("field1")

    result = my_function(input_df)
    assert result.schema == output_schema


def test_collection_stores_spec_without_patching_function():
    @dataclass
    class InputRow:
        field1: int

    @dataclass
    class OutputRow:
        field2: str

    collection = Collection()

    @collection.transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return input1.selectExpr("cast(field1 as string) as field2")

    assert not hasattr(my_function, "__transform_spec__")

    spec = collection._specs[my_function]
    assert spec.input_schemas == {
        "input1": schemas.get_spark_schema_from_dataclass(InputRow)
    }
    assert spec.output_schema == schemas.get_spark_schema_from_dataclass(OutputRow)


def test_collection_metadata_returns_none_for_unannotated_output():
    collection = Collection()

    @collection.transform
    def my_function():
        return "not a dataframe"

    assert collection._specs[my_function].output_schema is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_collection.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pyspark_schemas.collection'`

- [ ] **Step 3: Implement `Collection`**

```python
# src/pyspark_schemas/collection.py
from functools import wraps
from typing import Any, Callable

from pyspark.errors import PySparkAssertionError
from pyspark.sql import DataFrame
from pyspark.testing import assertSchemaEqual

from .transform import TransformSpec, _inspect_transform


class Collection:
    def __init__(self) -> None:
        self._specs: dict[Callable[..., Any], TransformSpec] = {}

    def transform(
        self,
        f=None,
        *,
        validate_input: bool = True,
        validate_output: bool = True,
        ignore_nullable: bool = True,
    ):
        def decorator(fn):
            import inspect

            signature = inspect.signature(fn)
            spec = _inspect_transform(fn)

            @wraps(fn)
            def wrapper(*args, **kwds):
                bound_arguments = signature.bind(*args, **kwds)
                bound_arguments.apply_defaults()

                if validate_input:
                    for parameter_name, expected_schema in spec.input_schemas.items():
                        value = bound_arguments.arguments.get(parameter_name)
                        if not isinstance(value, DataFrame):
                            raise TypeError(
                                f"Parameter '{parameter_name}' must be a pyspark.sql.DataFrame"
                            )

                        try:
                            assertSchemaEqual(
                                value.schema,
                                expected_schema,
                                ignoreNullable=ignore_nullable,
                            )
                        except PySparkAssertionError as e:
                            raise ValueError(
                                f"Schema mismatch for parameter '{parameter_name}'"
                            ) from e

                result = fn(*args, **kwds)

                if validate_output and spec.output_schema is not None:
                    if not isinstance(result, DataFrame):
                        raise TypeError(
                            f"Return value from '{fn.__name__}' must be a pyspark.sql.DataFrame"
                        )

                    try:
                        assertSchemaEqual(
                            result.schema,
                            spec.output_schema,
                            ignoreNullable=ignore_nullable,
                        )
                    except PySparkAssertionError as e:
                        raise ValueError(
                            f"Return schema mismatch for '{fn.__name__}'"
                        ) from e

                return result

            self._specs[wrapper] = spec
            return wrapper

        if f is None:
            return decorator

        return decorator(f)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_collection.py -v`
Expected: PASS (all tests)

- [ ] **Step 5: Commit**

```bash
git add src/pyspark_schemas/collection.py tests/test_collection.py
git commit -m "feat: add Collection with transform decorator storing spec off-function"
```

---

### Task 2: Trim `transform.py` to internals only

**Files:**
- Modify: `src/pyspark_schemas/transform.py`
- Delete: `tests/test_transform.py` (superseded by `tests/test_collection.py` from Task 1)

**Interfaces:**
- Consumes: nothing new.
- Produces: `transform.py` keeps `TransformSpec`, `_get_annotated_dataframe_schema_model`, `_get_spark_parameter`, `_inspect_transform` importable exactly as before. `transform()` decorator and `get_transform_spec()` are removed.

- [ ] **Step 1: Delete the superseded test file**

Run: `git rm tests/test_transform.py`

- [ ] **Step 2: Remove `transform()` and `get_transform_spec()` from transform.py**

Remove this block (and the now-unused `wraps`, `PySparkAssertionError`, `assertSchemaEqual` imports if nothing else in the file uses them):

```python
def get_transform_spec(f: Any) -> TransformSpec:
    spec = getattr(f, "__transform_spec__", None)
    if spec is None:
        name = getattr(f, "__name__", repr(f))
        raise TypeError(f"'{name}' is not decorated with @transform")
    return spec


def transform(
    f=None,
    *,
    validate_input: bool = True,
    validate_output: bool = True,
    ignore_nullable: bool = True,
):
    def decorator(fn):
        signature = inspect.signature(fn)
        spec = _inspect_transform(fn)

        @wraps(fn)
        def wrapper(*args, **kwds):
            ...

        setattr(wrapper, "__transform_spec__", spec)
        return wrapper

    if f is None:
        return decorator

    return decorator(f)
```

The file should end with `_inspect_transform` and its `return TransformSpec(...)`. Keep `from functools import wraps` removed only if no longer referenced; `inspect`, `DataFrame`, `types`, `get_type_hints` etc. stay since `_inspect_transform`/`_get_spark_parameter` still use them. `PySparkAssertionError`, `assertSchemaEqual`, `SparkSession` (used by `_get_spark_parameter`) — check each import is still used before removing; `SparkSession` is still used, `PySparkAssertionError`/`assertSchemaEqual` are not, remove those two imports.

- [ ] **Step 3: Run full test suite to confirm nothing else references removed symbols**

Run: `pytest -q`
Expected: FAIL only in `tests/test_pipeline.py` (still uses `from pyspark_schemas.transform import transform`) and example tests — these are fixed in later tasks. `tests/test_collection.py` and `tests/test_schemas.py`/`tests/test_fixtures.py`/`tests/test_type_inspection.py` still PASS.

- [ ] **Step 4: Commit**

```bash
git add src/pyspark_schemas/transform.py tests/test_transform.py
git commit -m "refactor: remove transform() decorator and get_transform_spec from transform.py"
```

---

### Task 3: `Pipeline` gains collections

**Files:**
- Modify: `src/pyspark_schemas/pipeline.py`

**Interfaces:**
- Consumes: `Collection` from Task 1 (`from .collection import Collection`).
- Produces: `Pipeline(collections: Sequence[Collection] = ())`, `Pipeline.transform(f=None, *, validate_input=True, validate_output=True, ignore_nullable=True)`, `Pipeline.add_step` unchanged signature but resolves specs via collections instead of `get_transform_spec`.

- [ ] **Step 1: Update the import and constructor**

```python
from .collection import Collection
```
(replaces `from .transform import get_transform_spec`)

```python
class Pipeline:
    def __init__(self, collections: Sequence[Collection] = ()):
        self._collection = Collection()
        self._collections: tuple[Collection, ...] = (self._collection, *collections)
        self._steps: dict[str, Step] = {}
        self._validated = False
        self._executable: ExecutablePipeline | None = None

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

    def _resolve_spec(self, transform: Transform):
        for collection in self._collections:
            spec = collection._specs.get(transform)
            if spec is not None:
                return spec
        name = getattr(transform, "__name__", repr(transform))
        raise TypeError(
            f"'{name}' is not registered in this pipeline's collections"
        )
```

- [ ] **Step 2: Replace the `get_transform_spec` call in `add_step`**

In `add_step`, replace:

```python
        spec = get_transform_spec(transform)
```

with:

```python
        spec = self._resolve_spec(transform)
```

- [ ] **Step 3: Run pipeline tests (still using old `@transform`) to confirm the expected failure mode**

Run: `pytest tests/test_pipeline.py -v`
Expected: FAIL — `ImportError` on `from pyspark_schemas.transform import transform` (fixed in Task 4), confirming `pipeline.py` no longer depends on the removed symbol.

- [ ] **Step 4: Commit**

```bash
git add src/pyspark_schemas/pipeline.py
git commit -m "feat: resolve transform specs from Pipeline's collections"
```

---

### Task 4: Migrate `tests/test_pipeline.py` to collections

**Files:**
- Modify: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `Collection` from Task 1, updated `Pipeline(collections=...)` from Task 3.
- Produces: no new interfaces; all existing pipeline behavior tests continue to pass.

Apply this mechanical transform to every test function in the file:

1. Replace the import `from pyspark_schemas.transform import transform` with `from pyspark_schemas.collection import Collection`.
2. In each test function, add `collection = Collection()` as the first line of the function body.
3. Replace every `@transform` (bare or parameterized) inside that function with `@collection.transform`.
4. Replace every `Pipeline()` construction in that function with `Pipeline(collections=[collection])`.
5. For `test_connect_rejects_steps_from_different_pipelines`, which builds two `Pipeline()` instances sharing the same `read_users` function, use one shared `collection` for both: `first = Pipeline(collections=[collection])`, `second = Pipeline(collections=[collection])`.

Example before/after for `test_validate_returns_cached_executable_and_freezes_pipeline`:

```python
# before
def test_validate_returns_cached_executable_and_freezes_pipeline():
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    pipeline = Pipeline()
    pipeline.add_step(read_users, "users")

    executable = pipeline.validate()
    ...

# after
def test_validate_returns_cached_executable_and_freezes_pipeline():
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    pipeline = Pipeline(collections=[collection])
    pipeline.add_step(read_users, "users")

    executable = pipeline.validate()
    ...
```

Example for the two-pipeline test:

```python
# before
def test_connect_rejects_steps_from_different_pipelines():
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    first = Pipeline()
    second = Pipeline()
    users = first.add_step(read_users, "users")
    filtered = second.add_step(read_users, "filtered")

    with pytest.raises(ValueError, match="same pipeline"):
        first.connect(users, filtered)

# after
def test_connect_rejects_steps_from_different_pipelines():
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    first = Pipeline(collections=[collection])
    second = Pipeline(collections=[collection])
    users = first.add_step(read_users, "users")
    filtered = second.add_step(read_users, "filtered")

    with pytest.raises(ValueError, match="same pipeline"):
        first.connect(users, filtered)
```

Every other test in the file (`test_duplicate_edges_are_idempotent`, `test_connect_many_rejects_empty_sources`, `test_pipeline_rejects_unsupported_non_dataframe_parameter`, `test_pipeline_requires_spark_session_for_source`, `test_pipeline_rejects_unannotated_output`, `test_pipeline_rejects_missing_dataframe_match`, `test_pipeline_rejects_extra_upstream_output`, `test_pipeline_rejects_ambiguous_duplicate_schema_outputs`, `test_pipeline_rejects_cycles`, `test_pipeline_supports_multiple_dataframe_inputs`, `test_executable_pipeline_runs_sources_and_downstream_steps`, `test_executable_pipeline_runs_fan_in_and_independent_components`, `test_executable_pipeline_rejects_invalid_spark_session`, `test_executable_pipeline_wraps_transform_failure`) follows steps 2-4 only (single collection, single pipeline per test).

- [ ] **Step 1: Apply the mechanical transform described above to the whole file**

- [ ] **Step 2: Run the test file**

Run: `pytest tests/test_pipeline.py -v`
Expected: PASS (all tests)

- [ ] **Step 3: Commit**

```bash
git add tests/test_pipeline.py
git commit -m "test: migrate pipeline tests to Collection-based transform registration"
```

---

### Task 5: Update package exports and examples

**Files:**
- Modify: `src/pyspark_schemas/__init__.py`
- Modify: `examples/order_product/transformations.py`
- Modify: `examples/order_product/pipeline.py`

**Interfaces:**
- Consumes: `Collection` from Task 1.
- Produces: `pyspark_schemas.Collection` export; example pipeline continues to build and run identically.

- [ ] **Step 1: Export `Collection` from the package**

```python
# src/pyspark_schemas/__init__.py
from .collection import Collection
from .pipeline import (
    ExecutablePipeline,
    Pipeline,
    PipelineExecutionError,
    Step,
)

__all__ = [
    "Collection",
    "ExecutablePipeline",
    "Pipeline",
    "PipelineExecutionError",
    "Step",
]
```

- [ ] **Step 2: Update `examples/order_product/transformations.py` to use a `Collection`**

Replace:

```python
from pyspark_schemas.transform import transform
from dataclasses import dataclass, fields
```

with:

```python
from pyspark_schemas.collection import Collection
from dataclasses import dataclass, fields

order_product_collection = Collection()
```

Then replace every `@transform` in that file with `@order_product_collection.transform` (four occurrences: `read_orders`, `filter_orders`, `read_customers`, `join_orders_with_customers`).

- [ ] **Step 3: Update `examples/order_product/pipeline.py` to construct the pipeline with the collection**

Replace:

```python
from pyspark_schemas.pipeline import Pipeline, ExecutablePipeline

from .transformations import (
    Customer,
    Order,
    filter_orders,
    join_orders_with_customers,
    read_customers,
    read_orders,
)
```

with:

```python
from pyspark_schemas.pipeline import Pipeline, ExecutablePipeline

from .transformations import (
    Customer,
    Order,
    filter_orders,
    join_orders_with_customers,
    order_product_collection,
    read_customers,
    read_orders,
)
```

Replace:

```python
    pipeline = Pipeline()
```

with:

```python
    pipeline = Pipeline(collections=[order_product_collection])
```

- [ ] **Step 4: Run the example and its tests**

Run: `pytest examples/order_product/test_transformations.py -v`
Expected: PASS

Run: `python -m examples.order_product.pipeline`
Expected: exits 0 and prints the joined DataFrame rows

- [ ] **Step 5: Commit**

```bash
git add src/pyspark_schemas/__init__.py examples/order_product/transformations.py examples/order_product/pipeline.py
git commit -m "feat: export Collection and migrate order_product example to it"
```

---

### Task 6: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `pytest -q`
Expected: all tests PASS, no references to `pyspark_schemas.transform.transform` or `get_transform_spec` remain

- [ ] **Step 2: Grep for leftover references**

Run: `grep -rn "get_transform_spec\|__transform_spec__" src tests examples`
Expected: no matches

- [ ] **Step 3: Commit if any stray fixes were needed**

```bash
git add -A
git commit -m "chore: final cleanup for pipeline/collection/transform hierarchy"
```
