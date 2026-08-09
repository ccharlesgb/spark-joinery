# Dependency Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let transforms declare non-`DataFrame` parameters (`Annotated[T, Context()]`) that are resolved by type from a `PipelineContext` at `run()` time, replacing the broken closure/`partial` workaround.

**Architecture:** A new `dependencies.py` module defines `Context` (a parameter marker) and `PipelineContext` (a `dict[type, Any]`-backed registry). `transform.py`'s inspection gains a `context_parameters` field on `TransformSpec`. `pipeline.py`'s `Step`/`Pipeline.add_step`/`ExecutablePipeline.run` are extended to allow and resolve these parameters. The `examples/order_product` pipeline is migrated off closures onto this mechanism.

**Tech Stack:** Python 3.12+, PySpark, pytest, dataclasses, `typing.Annotated`.

**Spec:** [docs/superpowers/specs/2026-09-16-dependency-injection-design.md](../specs/2026-09-16-dependency-injection-design.md)

## Global Constraints

- `Context()` resolves by the annotated parameter type; `Context(SomeType)` may override the lookup type explicitly.
- No name-based disambiguation. Two registered values of the same type must raise `ValueError` at `PipelineContext` construction — no escape hatch.
- `PipelineContext` does **not** carry `spark`. `ExecutablePipeline.run(spark, context=None)` keeps `spark` as a separate, required argument.
- A step with unresolved `Context` parameters and no `context` passed to `run()`, or a dependency missing from the registered `context`, must raise `PipelineExecutionError` (not a raw `KeyError`/`TypeError`) — consistent with existing error handling in `ExecutablePipeline.run`.
- Existing behavior (DataFrame schema validation, spark parameter, topological validation) must be unchanged for transforms that don't use `Context`.

---

### Task 1: `Context` and `PipelineContext`

**Files:**
- Create: `src/pyspark_schemas/dependencies.py`
- Test: `tests/test_dependencies.py`

**Interfaces:**
- Produces:
  - `class Context: def __init__(self, type_: type | None = None) -> None`, attribute `.type`
  - `class PipelineContext: def __init__(self, *, values: Sequence[Any] = ()) -> None`
  - `PipelineContext.resolve(self, param_type: type, marker: Context) -> Any` — raises `KeyError` if unresolved
  - `PipelineContext` raises `ValueError` at construction if two values share the same type

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_dependencies.py
from dataclasses import dataclass

import pytest

from pyspark_schemas.dependencies import Context, PipelineContext


@dataclass(frozen=True)
class PathConfig:
    value: str


@dataclass(frozen=True)
class OtherConfig:
    value: str


def test_pipeline_context_resolves_value_by_annotated_type():
    context = PipelineContext(values=[PathConfig("gs://bucket/path")])

    resolved = context.resolve(PathConfig, Context())

    assert resolved == PathConfig("gs://bucket/path")


def test_pipeline_context_resolves_by_explicit_context_type_override():
    context = PipelineContext(values=[PathConfig("gs://bucket/path")])

    # marker.type overrides whatever the parameter is annotated with
    resolved = context.resolve(OtherConfig, Context(PathConfig))

    assert resolved == PathConfig("gs://bucket/path")


def test_pipeline_context_raises_key_error_for_unregistered_type():
    context = PipelineContext(values=[PathConfig("gs://bucket/path")])

    with pytest.raises(KeyError, match="OtherConfig"):
        context.resolve(OtherConfig, Context())


def test_pipeline_context_rejects_duplicate_types_at_construction():
    with pytest.raises(ValueError, match="duplicate dependency type"):
        PipelineContext(values=[PathConfig("a"), PathConfig("b")])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_dependencies.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pyspark_schemas.dependencies'`

- [ ] **Step 3: Write the implementation**

```python
# src/pyspark_schemas/dependencies.py
from typing import Any, Sequence


class Context:
    """Marks a transform parameter as resolved from a PipelineContext."""

    def __init__(self, type_: type | None = None) -> None:
        self.type = type_


class PipelineContext:
    """Registry of dependency values, looked up by type."""

    def __init__(self, *, values: Sequence[Any] = ()) -> None:
        self._by_type: dict[type, Any] = {}
        for value in values:
            value_type = type(value)
            if value_type in self._by_type:
                raise ValueError(
                    f"duplicate dependency type {value_type!r} in PipelineContext"
                )
            self._by_type[value_type] = value

    def resolve(self, param_type: type, marker: "Context") -> Any:
        lookup_type = marker.type or param_type
        if lookup_type not in self._by_type:
            raise KeyError(f"no dependency registered for type {lookup_type!r}")
        return self._by_type[lookup_type]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_dependencies.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/pyspark_schemas/dependencies.py tests/test_dependencies.py
git commit -m "feat: add Context marker and PipelineContext dependency registry"
```

---

### Task 2: Detect `Context`-annotated parameters in transform inspection

**Files:**
- Modify: `src/pyspark_schemas/transform.py`
- Test: `tests/test_transform.py` (create if it doesn't exist — check first; if `_inspect_transform`/`TransformSpec` tests already live in `tests/test_pipeline.py` or elsewhere, add there instead of duplicating)

**Interfaces:**
- Consumes: `Context` from Task 1 (`from .dependencies import Context`)
- Produces:
  - `TransformSpec.context_parameters: dict[str, tuple[type, Context]]` (new field, parameter name → `(declared_type, marker)`)
  - `_inspect_transform` populates it for any parameter annotated `Annotated[T, Context(...)]`

- [ ] **Step 1: Check for an existing transform-inspection test file**

Run: `ls tests/ | grep -i transform`

If `tests/test_transform.py` exists, add the new tests there. Otherwise create it.

- [ ] **Step 2: Write the failing tests**

```python
# tests/test_transform.py
from dataclasses import dataclass
from typing import Annotated

from pyspark.sql import DataFrame, SparkSession

from pyspark_schemas.dependencies import Context
from pyspark_schemas.transform import _inspect_transform


@dataclass
class Order:
    order_id: int


@dataclass(frozen=True)
class PathConfig:
    value: str


def test_inspect_transform_collects_context_parameters():
    def read_orders(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, Order]:
        raise AssertionError

    spec = _inspect_transform(read_orders)

    assert set(spec.context_parameters) == {"path"}
    declared_type, marker = spec.context_parameters["path"]
    assert declared_type is PathConfig
    assert isinstance(marker, Context)


def test_inspect_transform_context_parameter_not_treated_as_dataframe_input():
    def filter_orders(
        orders: Annotated[DataFrame, Order],
        path: Annotated[PathConfig, Context()],
    ) -> Annotated[DataFrame, Order]:
        raise AssertionError

    spec = _inspect_transform(filter_orders)

    assert set(spec.input_schemas) == {"orders"}
    assert set(spec.context_parameters) == {"path"}


def test_inspect_transform_with_no_context_parameters_has_empty_dict():
    def read_orders(spark: SparkSession) -> Annotated[DataFrame, Order]:
        raise AssertionError

    spec = _inspect_transform(read_orders)

    assert spec.context_parameters == {}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/test_transform.py -v`
Expected: FAIL — `TransformSpec.__init__() got an unexpected keyword argument` is not the failure; instead it should fail on `AttributeError: 'TransformSpec' object has no attribute 'context_parameters'`

- [ ] **Step 4: Implement — add the detector and wire it into `_inspect_transform`**

In `src/pyspark_schemas/transform.py`, add the import and detector function near `_get_annotated_dataframe_schema_model`:

```python
from .dependencies import Context
```

```python
def _get_context_marker(annotation: Any) -> tuple[type, Context] | None:
    if get_origin(annotation) is not Annotated:
        return None

    annotated_args = get_args(annotation)
    if len(annotated_args) < 2:
        return None

    base_type = annotated_args[0]
    metadata = annotated_args[1:]
    for metadata_value in metadata:
        if isinstance(metadata_value, Context):
            return base_type, metadata_value

    return None
```

Update `TransformSpec`:

```python
@dataclass(frozen=True)
class TransformSpec:
    input_schemas: dict[str, types.StructType]
    output_schema: types.StructType | None
    spark_parameter: str | None
    context_parameters: dict[str, tuple[type, Context]]
```

Update `_inspect_transform`'s parameter loop:

```python
def _inspect_transform(f: Any) -> TransformSpec:
    signature = inspect.signature(f)
    type_hints = get_type_hints(f, include_extras=True)
    input_schemas: dict[str, types.StructType] = {}
    context_parameters: dict[str, tuple[type, Context]] = {}

    for parameter_name in signature.parameters:
        parameter_type = type_hints.get(parameter_name)
        if parameter_type is None:
            continue

        dataframe_schema_model = _get_annotated_dataframe_schema_model(parameter_type)
        if dataframe_schema_model is not None:
            input_schemas[parameter_name] = schemas.get_spark_schema_from_model(
                dataframe_schema_model
            )
            continue

        context_marker = _get_context_marker(parameter_type)
        if context_marker is not None:
            context_parameters[parameter_name] = context_marker

    output_schema = None
    return_type = type_hints.get("return")
    if return_type is not None:
        dataframe_schema_model = _get_annotated_dataframe_schema_model(return_type)
        if dataframe_schema_model is not None:
            output_schema = schemas.get_spark_schema_from_model(dataframe_schema_model)

    return TransformSpec(
        input_schemas=input_schemas,
        output_schema=output_schema,
        spark_parameter=_get_spark_parameter(f),
        context_parameters=context_parameters,
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_transform.py -v`
Expected: PASS (3 tests)

- [ ] **Step 6: Run the full test suite to check nothing else broke**

Run: `pytest -x`
Expected: PASS (existing `TransformSpec` construction sites, if any test them positionally, may need the new field — check failures and fix call sites, not test expectations)

- [ ] **Step 7: Commit**

```bash
git add src/pyspark_schemas/transform.py tests/test_transform.py
git commit -m "feat: detect Context-annotated parameters in transform inspection"
```

---

### Task 3: Allow and resolve `Context` parameters in `Pipeline`/`Step`/`ExecutablePipeline`

**Files:**
- Modify: `src/pyspark_schemas/pipeline.py`
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `TransformSpec.context_parameters` from Task 2, `Context`/`PipelineContext` from Task 1
- Produces:
  - `Step._context_parameters: dict[str, tuple[type, Context]]`
  - `Pipeline.add_step` accepts transforms with `Context`-annotated parameters
  - `ExecutablePipeline.run(self, spark: SparkSession, context: PipelineContext | None = None) -> dict[str, DataFrame]`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
from pyspark_schemas.dependencies import Context, PipelineContext


@dataclass(frozen=True)
class PathConfig:
    value: str


def test_pipeline_accepts_context_annotated_parameter():
    collection = Collection()

    @collection.transform
    def read_users(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    pipeline = Pipeline(collections=[collection])
    pipeline.add_step(read_users, "users")  # must not raise


def test_run_resolves_context_parameter_from_pipeline_context(spark: SparkSession):
    collection = Collection()

    @collection.transform
    def read_users(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, User]:
        assert path == PathConfig("gs://bucket/users")
        return spark.createDataFrame([(1,)], "user_id INT")

    pipeline = Pipeline(collections=[collection])
    pipeline.add_step(read_users, "users")
    executable = pipeline.validate()

    context = PipelineContext(values=[PathConfig("gs://bucket/users")])
    outputs = executable.run(spark, context)

    assert outputs["users"].count() == 1


def test_run_raises_pipeline_execution_error_when_context_missing(spark: SparkSession):
    collection = Collection()

    @collection.transform
    def read_users(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, User]:
        raise AssertionError

    pipeline = Pipeline(collections=[collection])
    pipeline.add_step(read_users, "users")
    executable = pipeline.validate()

    with pytest.raises(PipelineExecutionError, match="requires a PipelineContext"):
        executable.run(spark)


def test_run_raises_pipeline_execution_error_when_dependency_unregistered(
    spark: SparkSession,
):
    collection = Collection()

    @collection.transform
    def read_users(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, User]:
        raise AssertionError

    pipeline = Pipeline(collections=[collection])
    pipeline.add_step(read_users, "users")
    executable = pipeline.validate()

    with pytest.raises(PipelineExecutionError, match="failed to resolve dependency"):
        executable.run(spark, PipelineContext(values=[]))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k "context or Context"`
Expected: FAIL — `test_pipeline_accepts_context_annotated_parameter` fails with `TypeError: pipeline steps only support SparkSession and annotated DataFrame parameters`; the `run` tests fail with `TypeError: run() takes 2 positional arguments but 3 were given` or similar.

- [ ] **Step 3: Implement — update `Step`, `add_step`, and `ExecutablePipeline.run`**

Add the import at the top of `src/pyspark_schemas/pipeline.py`:

```python
from .dependencies import Context, PipelineContext
```

Update the `Step` dataclass:

```python
@dataclass(eq=False)
class Step:
    name: str
    transform: Transform
    _pipeline: Pipeline
    _input_schemas: dict[str, types.StructType]
    _output_schema: types.StructType
    _spark_parameter: str | None
    _context_parameters: dict[str, tuple[type, Context]]
    _upstream_steps: list[Step]
```

Update `ExecutablePipeline.run`:

```python
def run(
    self, spark: SparkSession, context: PipelineContext | None = None
) -> dict[str, DataFrame]:
    if not isinstance(spark, SparkSession):
        raise TypeError("run() requires a SparkSession")

    outputs: dict[str, DataFrame] = {}
    for execution_step in self._execution_steps:
        step = execution_step.step
        arguments: dict[str, Any] = {
            parameter_name: outputs[upstream_step.name]
            for parameter_name, upstream_step in execution_step.dataframe_bindings
        }
        if step._spark_parameter is not None:
            arguments[step._spark_parameter] = spark

        for parameter_name, (param_type, marker) in step._context_parameters.items():
            if context is None:
                raise PipelineExecutionError(
                    f"step '{step.name}' requires a PipelineContext but none was provided"
                )
            try:
                arguments[parameter_name] = context.resolve(param_type, marker)
            except KeyError as error:
                raise PipelineExecutionError(
                    f"step '{step.name}' failed to resolve dependency"
                ) from error

        try:
            result = step.transform(**arguments)
            if not isinstance(result, DataFrame):
                raise TypeError(
                    f"step '{step.name}' returned a non-DataFrame value"
                )
        except Exception as error:
            if isinstance(error, PipelineExecutionError):
                raise
            raise PipelineExecutionError(
                f"Pipeline step '{step.name}' failed"
            ) from error

        outputs[step.name] = result

    return outputs
```

Update `Pipeline.add_step` — extend the supported-parameters check and construct `Step` with `_context_parameters`:

```python
def add_step(self, transform: Transform, name: str) -> Step:
    self._ensure_mutable()
    if name in self._steps:
        raise ValueError(f"step name '{name}' is already registered")

    spec = self._resolve_spec(transform)
    input_schemas = spec.input_schemas
    output_schema = spec.output_schema
    if output_schema is None:
        raise TypeError("pipeline steps require an annotated DataFrame output")

    spark_parameter = spec.spark_parameter
    parameter_names = set(inspect.signature(transform).parameters)
    supported_parameters = (
        set(input_schemas)
        | ({spark_parameter} if spark_parameter is not None else set())
        | set(spec.context_parameters)
    )
    unsupported_parameters = parameter_names - supported_parameters
    if unsupported_parameters:
        unsupported = sorted(unsupported_parameters)[0]
        raise TypeError(
            "pipeline steps only support SparkSession, annotated DataFrame, "
            f"and Context-annotated parameters; unsupported parameter '{unsupported}'"
        )

    if not input_schemas and spark_parameter is None:
        raise TypeError("source step must declare a SparkSession")

    step = Step(
        name=name,
        transform=transform,
        _pipeline=self,
        _input_schemas=input_schemas,
        _output_schema=output_schema,
        _spark_parameter=spark_parameter,
        _context_parameters=spec.context_parameters,
        _upstream_steps=[],
    )
    self._steps[name] = step
    return step
```

Note: the error message text in `add_step` changed (`"only support SparkSession, annotated DataFrame, and Context-annotated parameters"`). Update the existing test `test_pipeline_rejects_unsupported_non_dataframe_parameter` in `tests/test_pipeline.py` to match the new message:

```python
with pytest.raises(
    TypeError, match="only support SparkSession, annotated DataFrame"
):
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_pipeline.py -v`
Expected: PASS (all tests, including the 4 new ones and the updated message-match test)

- [ ] **Step 5: Run the full test suite**

Run: `pytest -x`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pyspark_schemas/pipeline.py tests/test_pipeline.py
git commit -m "feat: resolve Context-annotated parameters when running a pipeline"
```

---

### Task 4: Export `Context`/`PipelineContext` from the package root

**Files:**
- Modify: `src/pyspark_schemas/__init__.py`
- Test: `tests/test_pipeline.py` (extend the existing export-check test, or add alongside it)

**Interfaces:**
- Consumes: `Context`, `PipelineContext` from Task 1
- Produces: `pyspark_schemas.Context`, `pyspark_schemas.PipelineContext`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_pipeline.py`:

```python
from pyspark_schemas import Context as ExportedContext, PipelineContext as ExportedPipelineContext
from pyspark_schemas.dependencies import Context, PipelineContext


def test_dependency_types_are_exported_from_package():
    assert ExportedContext is Context
    assert ExportedPipelineContext is PipelineContext
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_pipeline.py -k test_dependency_types_are_exported_from_package -v`
Expected: FAIL with `ImportError: cannot import name 'Context' from 'pyspark_schemas'`

- [ ] **Step 3: Implement**

```python
# src/pyspark_schemas/__init__.py
from .collection import Collection
from .dependencies import Context, PipelineContext
from .pipeline import (
    ExecutablePipeline,
    Pipeline,
    PipelineExecutionError,
    Step,
)

__all__ = [
    "Collection",
    "Context",
    "ExecutablePipeline",
    "Pipeline",
    "PipelineContext",
    "PipelineExecutionError",
    "Step",
]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_pipeline.py -k test_dependency_types_are_exported_from_package -v`
Expected: PASS

- [ ] **Step 5: Run the full test suite**

Run: `pytest -x`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pyspark_schemas/__init__.py tests/test_pipeline.py
git commit -m "feat: export Context and PipelineContext from package root"
```

---

### Task 5: Migrate `examples/order_product` off closures onto `Context`

**Files:**
- Modify: `examples/order_product/transformations.py`
- Modify: `examples/order_product/pipeline.py`
- Modify: `examples/order_product/test_transformations.py`

**Interfaces:**
- Consumes: `Context`, `PipelineContext` from `pyspark_schemas`
- Produces:
  - `OrdersPath`, `CustomersPath`, `RunDate` dataclasses in `transformations.py`
  - `read_orders(spark, path: Annotated[OrdersPath, Context()])`
  - `read_customers(spark, path: Annotated[CustomersPath, Context()])`
  - `filter_orders(orders, run_date: Annotated[RunDate, Context()])`
  - `build_pipeline(orders_path: str, customers_path: str, run_date: date) -> ExecutablePipeline` (signature unchanged) internally builds a `PipelineContext`
  - `main` block calls `pipeline.run(spark, context)`

- [ ] **Step 1: Update `transformations.py` — add wrapper types and change signatures**

Read the current file first (`examples/order_product/transformations.py`) to confirm exact current content, then apply:

```python
from datetime import date, datetime
from typing import Annotated

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pyspark_schemas.collection import Collection
from pyspark_schemas.dependencies import Context
from dataclasses import dataclass, fields

order_product_collection = Collection()


@dataclass(frozen=True)
class OrdersPath:
    value: str


@dataclass(frozen=True)
class CustomersPath:
    value: str


@dataclass(frozen=True)
class RunDate:
    value: date


@dataclass
class Order:
    order_id: int
    customer_id: str
    order_timestamp: datetime
    product_id: int
    quantity: int


@dataclass
class Customer:
    customer_id: str
    name: str


@dataclass
class OrderWithCustomerDimension(Order, Customer):
    pass


@order_product_collection.transform
def read_orders(
    spark: SparkSession, path: Annotated[OrdersPath, Context()]
) -> Annotated[DataFrame, Order]:
    return spark.read.parquet(path.value)


@order_product_collection.transform
def filter_orders(
    orders: Annotated[DataFrame, Order], run_date: Annotated[RunDate, Context()]
) -> Annotated[DataFrame, Order]:
    return orders.filter(
        F.to_date(F.col("order_timestamp")) == F.lit(run_date.value)
    )


@order_product_collection.transform
def read_customers(
    spark: SparkSession, path: Annotated[CustomersPath, Context()]
) -> Annotated[DataFrame, Customer]:
    return spark.read.parquet(path.value)


@order_product_collection.transform
def join_orders_with_customers(
    orders: Annotated[DataFrame, Order], customers: Annotated[DataFrame, Customer]
) -> Annotated[DataFrame, OrderWithCustomerDimension]:
    joined = orders.join(customers, on="customer_id", how="inner")
    output_columns = [field.name for field in fields(OrderWithCustomerDimension)]
    return joined.select(*output_columns)
```

- [ ] **Step 2: Update `test_transformations.py` to use `RunDate`**

Read the current file first (`examples/order_product/test_transformations.py`), then change the two call sites:

```python
from .transformations import Order, RunDate, filter_orders
```

```python
        partial(filter_orders, run_date=RunDate(date(2026, 1, 1)))(df_wrong_schema)
```

```python
    orders_for_date = partial(filter_orders, run_date=RunDate(date(2026, 1, 1)))
```

- [ ] **Step 3: Run the example's tests to verify they fail, then confirm the fix**

Run: `pytest examples/order_product/test_transformations.py -v`
Expected first (before Step 2 edits land): FAIL on schema/type mismatch since `filter_orders` now expects `RunDate`, not `date`, and does `run_date.value` internally.
After Step 2 edits: PASS.

- [ ] **Step 4: Update `pipeline.py` — remove closures, use `PipelineContext`**

Read the current file first (`examples/order_product/pipeline.py`), then replace its contents:

```python
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from pyspark_schemas.dependencies import PipelineContext
from pyspark_schemas.pipeline import Pipeline, ExecutablePipeline

from .transformations import (
    CustomersPath,
    OrdersPath,
    RunDate,
    filter_orders,
    join_orders_with_customers,
    order_product_collection,
    read_customers,
    read_orders,
)

DATA_DIR = Path(__file__).parent / "data"


def build_pipeline() -> ExecutablePipeline:
    pipeline = Pipeline(collections=[order_product_collection])
    orders = pipeline.add_step(read_orders, "orders")
    filtered_orders = pipeline.add_step(filter_orders, "filtered_orders")
    customers = pipeline.add_step(read_customers, "customers")
    joined = pipeline.add_step(join_orders_with_customers, "joined")

    pipeline.connect(orders, filtered_orders)
    pipeline.connect(filtered_orders, joined)
    pipeline.connect(customers, joined)

    return pipeline.validate()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("order-product-pipeline")
        .master("local[*]")
        .getOrCreate()
    )

    pipeline = build_pipeline()
    context = PipelineContext(
        values=[
            OrdersPath(str(DATA_DIR / "orders.parquet")),
            CustomersPath(str(DATA_DIR / "customers.parquet")),
            RunDate(date(2026, 1, 1)),
        ]
    )
    outputs = pipeline.run(spark, context)
    outputs["joined"].show()

    spark.stop()
```

Note: `build_pipeline()` drops its `orders_path`/`customers_path`/`run_date` parameters since those values now flow through `PipelineContext` at `run()` time rather than at graph-construction time — this is the point of the fix (the graph no longer needs per-run values baked in via closures).

- [ ] **Step 5: Run the example end-to-end**

Run: `python -m examples.order_product.pipeline`
Expected: runs to completion, printing the joined DataFrame, exit code 0.

- [ ] **Step 6: Run the full test suite**

Run: `pytest -x` (or `just test`)
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add examples/order_product/transformations.py examples/order_product/pipeline.py examples/order_product/test_transformations.py
git commit -m "refactor: migrate order_product example to Context-based dependency injection"
```

---

### Task 6: Update `docs/specs/dependencies.md` to match the implemented API

**Files:**
- Modify: `docs/specs/dependencies.md`

**Interfaces:**
- Consumes: nothing (documentation only)
- Produces: nothing (documentation only)

- [ ] **Step 1: Rewrite the doc to match what was actually built**

Replace the "future work" note and the invalid-syntax examples with the real, implemented API: `Context()` / `Context(SomeType)`, `PipelineContext(values=[...])`, `ExecutablePipeline.run(spark, context)`, and a corrected version of the `user_pipeline` example (fix the stray parentheses, undefined `PathConfig`/`RunDateConfig`, and `run_date` reference bugs already present in the draft). Base the corrected example directly on the patterns proven in `examples/order_product`.

- [ ] **Step 2: Verify doc code compiles conceptually**

Cross-check every symbol used in the doc's code samples (`Context`, `PipelineContext`, `Pipeline`, `Collection`) against the actual public API from Task 4's `__init__.py` exports. Fix any mismatch.

- [ ] **Step 3: Commit**

```bash
git add docs/specs/dependencies.md
git commit -m "docs: update dependency injection spec to match implementation"
```

---

## Self-Review Notes

- **Spec coverage:** `Context`/`PipelineContext` (Task 1), inspection (Task 2), `Pipeline`/`Step`/`run` (Task 3), exports (Task 4), example migration (Task 5), doc sync (Task 6) — all design sections covered. Non-goals (named deps, `Depends(callable)`, caching) intentionally have no tasks.
- **Type consistency:** `context_parameters: dict[str, tuple[type, Context]]` is used identically in `TransformSpec` (Task 2), `Step` (Task 3), and `ExecutablePipeline.run` (Task 3). `PipelineContext.resolve(param_type, marker)` signature matches its one call site in `run()`.
- **Existing test breakage called out explicitly:** Task 3 Step 3 flags the exact existing test (`test_pipeline_rejects_unsupported_non_dataframe_parameter`) whose expected error message text changes, with the fix inline — not left as a surprise.
