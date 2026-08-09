# Dependency Injection for Transforms — Design

## Problem

Transforms sometimes need non-`DataFrame` inputs (paths, run dates, config
objects). Today the only way to parameterize a transform is to wrap it in a
closure/`functools.partial` (see `examples/order_product/pipeline.py`,
`make_read_orders` etc.). This breaks `Collection`/`Pipeline` because the
wrapped callable is no longer the registered transform, so
`Pipeline._resolve_spec` can't find its `TransformSpec`.

This design adds a small, FastAPI-inspired dependency injection mechanism so
transforms can declare extra parameters that are resolved from a
`PipelineContext` at run time, instead of via closures.

## Syntax

```python
@col.transform
def read_users(
    spark: SparkSession,
    path_config: Annotated[PathConfig, Context()],
) -> Annotated[DataFrame, User]:
    return spark.read.table(path_config.users_path)
```

`Context()` marks a parameter as injectable. It is resolved by the
parameter's annotated type (`PathConfig` above) against values registered on
a `PipelineContext`. `Context(SomeOtherType)` may be passed explicitly to
override the lookup type if ever needed, but the common case is `Context()`.

Out of scope (deferred): named/string-keyed dependencies, dependency
functions (`Depends(callable)`), caching/scopes. If two dependency values of
the same type are registered, construction fails immediately — no
disambiguation mechanism yet.

## Components

### `Context` marker

New small class (proposed home: `src/pyspark_schemas/dependencies.py`):

```python
class Context:
    def __init__(self, type_: type | None = None) -> None:
        self.type = type_
```

### `PipelineContext`

Also in `dependencies.py`:

```python
class PipelineContext:
    def __init__(self, *, spark: SparkSession, values: Sequence[Any] = ()) -> None:
        self.spark = spark
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

`values` are plain user-defined objects (typically small dataclasses like
`PathConfig`, or wrapper types around primitives such as `RunDate` to avoid
ambiguous built-in types like `str`/`date`).

### Inspection (`type_inspection.py`, `transform.py`)

- Add a detector mirroring `_get_annotated_dataframe_schema_model`, e.g.
  `_get_context_marker(annotation) -> Context | None`, matching
  `Annotated[T, Context(...)]` and returning the `Context()` metadata
  instance (the base type `T` is read separately from `get_args`).
- `TransformSpec` gains a new field:
  ```python
  context_parameters: dict[str, tuple[type, Context]]
  ```
  (parameter name → (declared type `T`, its `Context` marker)).
- `_inspect_transform` populates `context_parameters` for any parameter whose
  annotation matches. A parameter cannot simultaneously be a DataFrame-schema
  parameter, the Spark parameter, and a context parameter — these detectors
  are mutually exclusive by construction (different base types), so no extra
  validation is needed there beyond what already exists.

### `Collection.transform`

No changes required. The wrapper only validates parameters present in
`spec.input_schemas`; everything else (including context-injected values)
passes through to the wrapped function unchanged. This means transforms with
`Context`-annotated parameters can still be unit-tested by calling them
directly and passing plain values for those parameters.

### `Pipeline` / `Step` / `ExecutablePipeline`

- `Step` gains `_context_parameters: dict[str, tuple[type, Context]]`,
  populated from the resolved `TransformSpec` in `add_step`.
- `add_step`'s supported-parameter check extends to:
  ```python
  supported_parameters = (
      set(input_schemas)
      | ({spark_parameter} if spark_parameter is not None else set())
      | set(spec.context_parameters)
  )
  ```
- `ExecutablePipeline.run` signature becomes:
  ```python
  def run(self, spark: SparkSession, context: PipelineContext | None = None) -> dict[str, DataFrame]:
  ```
  For each step, in addition to existing dataframe/spark bindings, resolve
  context parameters:
  ```python
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
  ```
  (Existing per-step exception handling already wraps unexpected errors into
  `PipelineExecutionError`; the explicit checks above just give clearer
  messages for the missing-context / missing-dependency cases.)

- `PipelineContext.spark` is stored but not required to equal the `spark`
  argument passed to `run` — `run(spark, context)` keeps `spark` as the single
  source of truth for the Spark parameter (per earlier decision to keep them
  separate); `PipelineContext.spark` exists for future flexibility but is
  unused by `run` today. (Reconsidered below — see Open Question.)

### Open question resolved during brainstorming

We decided `run(spark, context)` keeps `spark` separate rather than folding
it into `PipelineContext`. Given that, `PipelineContext` storing its own
`spark` field is redundant. **Decision: drop `spark` from `PipelineContext`
entirely** — it only holds injected `values`. `run(spark, context)` remains
the call shape.

```python
class PipelineContext:
    def __init__(self, *, values: Sequence[Any] = ()) -> None:
        ...
```

## Example migration

`examples/order_product/pipeline.py`: replace `make_read_orders`,
`make_read_customers`, `make_filter_orders` closures with plain transforms
taking `Context`-annotated parameters, e.g.:

```python
@dataclass(frozen=True)
class OrdersPath:
    value: str

@dataclass(frozen=True)
class CustomersPath:
    value: str

@dataclass(frozen=True)
class RunDate:
    value: date

def read_orders(spark: SparkSession, path: Annotated[OrdersPath, Context()]) -> Annotated[DataFrame, Order]:
    ...

def filter_orders(orders: Annotated[DataFrame, Order], run_date: Annotated[RunDate, Context()]) -> Annotated[DataFrame, Order]:
    ...
```

`build_pipeline` builds a `PipelineContext(values=[OrdersPath(...), CustomersPath(...), RunDate(...)])`
and passes it to `pipeline.run(spark, context)`.

## Testing plan

- New unit tests (`tests/test_dependencies.py` or similar) for `Context` /
  `PipelineContext`: resolve by type, explicit `Context(Type)` override,
  duplicate-type construction error, missing-dependency `KeyError` from
  `resolve`.
- `tests/test_pipeline.py`: `add_step` accepts a transform with a context
  parameter; `run()` injects the resolved value; `run()` raises
  `PipelineExecutionError` when context is missing or a dependency is
  unregistered.
- `tests/test_type_inspection.py` / transform inspection tests: verify
  `TransformSpec.context_parameters` is populated correctly and mutually
  exclusive with `input_schemas`/`spark_parameter`.
- Update `examples/order_product` to use the new pattern; confirm
  `python -m examples.order_product.pipeline` runs end to end.

## Non-goals

- Named/keyword dependency disambiguation.
- Dependency functions/callables (`Depends(callable)` style resolution).
- Caching or scoping of resolved dependencies within a single `run()`.
