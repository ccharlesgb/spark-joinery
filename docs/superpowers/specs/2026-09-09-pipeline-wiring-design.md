# Pipeline Wiring Design

## Scope

This design covers pipeline graph construction and execution for Spark DataFrame transformations. Dependency injection for configurable values is explicitly out of scope for v1.

Pipeline steps may receive only:

- `pyspark.sql.SparkSession`
- Annotated `pyspark.sql.DataFrame` inputs

Every source step must declare a `SparkSession`. Every non-source step must consume at least one annotated DataFrame. Other non-DataFrame parameters are not supported by pipeline execution yet, although transformed functions may continue to be called directly.

## Public API

```python
from typing import Annotated
from pyspark.sql import DataFrame, SparkSession

@transform
def read_users(
    spark: SparkSession,
) -> Annotated[DataFrame, User]:
    return spark.read.parquet("/data/users")

@transform
def filter_users(
    users: Annotated[DataFrame, User],
) -> Annotated[DataFrame, User]:
    return users.filter("active = true")

pipeline = Pipeline()
users = pipeline.add_step(read_users, "users")
filtered_users = pipeline.add_step(filter_users, "filtered_users")

pipeline.connect(users, filtered_users)
executable = pipeline.validate()
outputs = executable.run(spark)
result = outputs["filtered_users"]
```

Fan-in uses an explicit method rather than operator syntax:

```python
pipeline.connect_many(
    [filtered_users, departments],
    users_with_departments,
)
```

The `>>` syntax may be added later as syntactic sugar after the underlying graph API is stable.

## Graph Model

A `Step` represents one named transform invocation in the graph:

```python
@dataclass
class Step:
    name: str
    transform: Transform
    upstream_steps: list[Step]
```

A `Pipeline` owns the registered steps and graph edges:

```python
class Pipeline:
    def add_step(self, transform: Transform, name: str) -> Step:
        ...

    def connect(self, upstream: Step, downstream: Step) -> None:
        ...

    def connect_many(
        self,
        upstream_steps: Sequence[Step],
        downstream: Step,
    ) -> None:
        ...

    def validate(self) -> ExecutablePipeline:
        ...


class ExecutablePipeline:
    def run(self, spark: SparkSession) -> dict[str, DataFrame]:
        ...
```

Step names must be unique within a pipeline. Edges must connect steps belonging to the same pipeline. Duplicate edges are idempotent no-ops.

`Pipeline` is a mutable builder. `validate()` finalizes the graph and returns an immutable `ExecutablePipeline`. The builder must reject further calls to `add_step`, `connect`, or `connect_many` after validation. Only `ExecutablePipeline` exposes `run`, which prevents an unvalidated graph from being executed accidentally.

The executable stores the precomputed topological order and DataFrame parameter bindings. Repeated successful calls to `validate()` return the same cached executable while the builder remains unchanged. A failed validation leaves the builder mutable so the caller can fix the graph and retry.

Independent graph components are valid. Every registered step executes once, and the result mapping contains an output for every step name.

## Transform Contract

Pipeline registration validates the transformed function signature.

Valid source:

```python
@transform
def read_users(
    spark: SparkSession,
) -> Annotated[DataFrame, User]:
    ...
```

Valid downstream step:

```python
@transform
def filter_users(
    users: Annotated[DataFrame, User],
) -> Annotated[DataFrame, User]:
    ...
```

Rejected by pipeline registration:

```python
@transform
def read_users(
    spark: SparkSession,
    path: str,
) -> Annotated[DataFrame, User]:
    ...
```

The existing `@transform` decorator remains responsible for runtime DataFrame input and output schema validation. Pipeline metadata should be attached to or derived from the transformed callable so the graph does not duplicate schema-inspection logic unnecessarily.

Every pipeline step must return an annotated DataFrame. An unannotated output cannot participate in schema-based wiring and is rejected.

A step may declare at most one `SparkSession` parameter. The runtime supplies the same SparkSession to every step that declares it.

## DataFrame Wiring

Graph edges establish which steps must precede a downstream step. Schema annotations determine which upstream output maps to which DataFrame parameter.

For each downstream step:

1. Collect its annotated DataFrame parameters.
2. Collect the outputs of its upstream steps.
3. Match outputs to parameters by schema model.
4. Require exactly one match for every parameter.
5. Reject any unmatched upstream output.
6. Reject ambiguity when multiple upstream outputs have the same schema model.

Example:

```python
@transform
def join_users_to_departments(
    users: Annotated[DataFrame, User],
    departments: Annotated[DataFrame, Department],
) -> Annotated[DataFrame, UserWithDepartment]:
    ...
```

This step can be wired from one `User` producer and one `Department` producer. If two upstream steps both produce `User`, the graph is invalid even if their step names differ. Explicit parameter-to-step wiring is deferred until a later version.

A source step has no DataFrame parameters and must declare `SparkSession`. A non-source step must have one or more DataFrame parameters and does not need to declare `SparkSession`.

## Execution

`Pipeline.validate()` performs these operations before returning an executable pipeline:

1. Validate registered steps and edges.
2. Detect cycles.
3. Topologically order the graph.
4. Validate DataFrame wiring for every step.
5. Precompute the SparkSession and DataFrame parameter bindings.
6. Return an immutable `ExecutablePipeline` containing the execution plan.

Validation errors should occur before any Spark transformations execute.

`ExecutablePipeline.run(spark)` executes the precomputed plan. For each step, it builds keyword arguments from:

- `spark` when the signature declares `SparkSession`
- upstream DataFrames matched by annotated schema

No other arguments are resolved or injected.

The executable performs defensive runtime checks, such as verifying that `spark` is a `SparkSession` and that each transform returns a DataFrame. It does not repeat graph validation, cycle detection, topological sorting, or schema matching. Transform failures are wrapped in `PipelineExecutionError` with the step name in the message and the original exception as its cause.

Parameters are recognized as Spark dependencies only when their resolved annotation is exactly `SparkSession`; the parameter name does not need to be `spark`. For example, `session: SparkSession` is supported, while an unannotated `spark` parameter is not.

## Testing

Tests should cover:

- Registering named steps.
- Rejecting mutation after validation.
- Returning an executable pipeline from `validate`.
- Connecting one upstream step with `connect`.
- Connecting multiple upstream steps with `connect_many`.
- Rejecting duplicate step names.
- Treating duplicate edges as idempotent.
- Rejecting steps from different pipelines.
- Rejecting unsupported non-DataFrame parameters.
- Requiring `SparkSession` for source steps.
- Requiring at least one DataFrame input for non-source steps.
- Rejecting unannotated DataFrame outputs.
- Executing steps in topological order.
- Passing one upstream DataFrame to a matching parameter.
- Matching multiple upstream DataFrames by schema model.
- Rejecting duplicate schema matches as ambiguous.
- Rejecting missing DataFrame matches.
- Rejecting extra upstream outputs with no parameter.
- Rejecting cycles.
- Returning all step outputs by name.
- Defensively rejecting an invalid runtime SparkSession.
- Wrapping transform failures with the step name while preserving the original cause.
- Returning the same cached executable from repeated successful validation.
- Preserving existing `@transform` schema-validation behavior.

## Deferred Work

The following are intentionally excluded from v1:

- `Depends` or `Context` annotations.
- Configurable paths, dates, or arbitrary runtime values.
- Multiple values with the same schema model.
- Explicit parameter-to-step connection, such as `connect(users=users_step)`.
- Operator syntax such as `step_a >> step_b`.
- External DataFrame inputs supplied directly to a pipeline.
- Automatic persistence, caching, retries, or parallel execution.
