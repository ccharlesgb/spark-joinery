---
name: pyspark-joinery
description: PySpark Joinery best practices and conventions. Use when working with PySpark Joinery. Keeps PySpark Joinery code clean and up to date with the latest features and patterns, updated with new versions. Write new code or refactor and update old code.
---

# PySpark Joinery

Spark Joinery is a schema-first framework for constructing testable PySpark transformations and validated pipeline DAGs. Use this skill when adding or changing schemas, decorated transformations, or `Pipeline` definitions.

Import the package as `spark_joinery`, not `pyspark_joinery`.

```python
from spark_joinery import (
	Context,
	Pipeline,
	PipelineContext,
	Project,
	ProjectCast,
	ProjectTopLevel,
	Strict,
	StrictNull,
	transform,
)
```

## Define Schemas First

Define a Python dataclass or Pydantic model for every meaningful DataFrame boundary. Each model field represents a Spark column. Dataclasses are the usual choice; Pydantic is useful when its validation rules also help construct valid test fixtures, but Spark Joinery only enforces the resulting Spark data types.

```python
from dataclasses import dataclass
from datetime import date


@dataclass
class Customer:
	snapshot_date: date
	customer_id: str
	name: str
	is_active: bool
```

- Use nested dataclasses and typed lists for nested Spark structs and arrays.
- Python types map to Spark types by default. Use `typing.Annotated` on a schema field only when an explicit Spark type is required, such as `DecimalType`.
- Fields are nullable by default. Do not treat non-nullable schema metadata as a data-quality constraint.
- Keep schemas focused on the DataFrame contract for a boundary, rather than reusing a broad source-table model throughout a pipeline.
- Use `Schema(Model).create_dataframe(spark, instances)` to create typed DataFrame fixtures in tests.

## Build Transformations

Decorate each transformation with `@transform`. Annotate every DataFrame input and output as `Annotated[DataFrame, Mode(Schema)]`; this is both documentation and a runtime contract. Transformations may also return non-DataFrame values: annotate those with their ordinary Python type, which the pipeline checks for type compatibility without schema coercion.

```python
from typing import Annotated

from pyspark.sql import DataFrame
from spark_joinery import Project, Strict, transform


@transform
def filter_active_customers(
	customers: Annotated[DataFrame, Project(Customer)],
) -> Annotated[DataFrame, Strict(Customer)]:
	return customers.filter(customers.is_active)
```

Choose the coercion mode deliberately:

- `Strict(Model)`: requires exactly the model's fields and types; ignores nullability. Prefer this at stable internal boundaries and for transformation outputs.
- `StrictNull(Model)`: as strict, including nullability. Use only when nullability metadata is itself part of the contract.
- `Project(Model)`: recursively selects the model's fields, removes extras, and does not cast or invent missing fields. Prefer this for inputs from wide or nested sources.
- `ProjectTopLevel(Model)`: projects only top-level fields and requires nested struct fields to match.
- `ProjectCast(Model)`: recursively projects and uses Spark casts. Reserve it for external or loosely typed inputs where conversion is intentional; casts can still fail when evaluated.

The usual robust pattern is a permissive input and a strict output. Keep transformations small and DataFrame-focused; Spark Joinery validates the contracts before Spark's lazy computation, unless the function itself performs an action such as `collect()`.

Source transforms may accept `SparkSession` and return a contracted DataFrame. Sink transforms may accept contracted DataFrames and return `None`.

```python
from pyspark.sql import DataFrame, SparkSession
from typing import Annotated


@transform
def read_customers(
	spark: SparkSession,
) -> Annotated[DataFrame, Project(Customer)]:
	return spark.read.parquet("/data/customers")


@transform
def write_customers(
	customers: Annotated[DataFrame, Strict(Customer)],
) -> None:
	customers.write.mode("overwrite").parquet("/data/active-customers")
```

Outside a pipeline, decorated transformations remain callable directly and can be used with `DataFrame.transform`.

## Wire A Pipeline

Create a `Pipeline`, add each decorated transform as a `Step`, then connect the steps. A `Step` is an occurrence of a transform, so the same transform may be added more than once with distinct names.

```python
pipeline = Pipeline()
read_step = pipeline.add_step(read_customers)
filter_step = pipeline.add_step(filter_active_customers)
write_step = pipeline.add_step(write_customers)

pipeline.connect(read_step, filter_step)
pipeline.connect(filter_step, write_step)

pipeline.run(spark=spark)
```

- Pipeline steps support `SparkSession`, contract-annotated `DataFrame` parameters, typed non-DataFrame parameters, and `Context()`-annotated parameters. Do not add ordinary configuration parameters.
- A source step has no DataFrame inputs and must declare `SparkSession`.
- `connect()` validates the upstream output against a compatible downstream input immediately and rejects cycles.
- When an upstream output could satisfy more than one input parameter, disambiguate with `pipeline.connect(upstream, downstream, param="parameter_name")`.
- Use `connect_many([...], downstream)` for a transform with multiple independently produced DataFrame inputs.
- Give repeated transforms explicit, unique names with `add_step(transform, name="...")`.
- `run()` returns a dictionary of step outputs keyed by step name; sink steps returning `None` are omitted.

## Inject Context Deliberately

Use `Context()` for run-specific configuration or dependencies that should be supplied by the pipeline runner rather than carried as DataFrame data: paths, a run date, feature configuration, a client wrapper, or credentials wrappers. Do not use it for values that should be DataFrame columns or for ordinary transform calls outside a pipeline.

Define a dedicated type for each context value. Context resolution is by type and cannot disambiguate two values of the same type; never inject bare `str`, `int`, or `date` values.

```python
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

from pyspark.sql import DataFrame, SparkSession
from spark_joinery import Context, PipelineContext, Project, transform


@dataclass
class CustomerPaths:
	input_path: Path
	output_path: Path


@transform
def read_customers(
	spark: SparkSession,
	paths: Annotated[CustomerPaths, Context()],
) -> Annotated[DataFrame, Project(Customer)]:
	return spark.read.parquet(str(paths.input_path))


context = PipelineContext(values=[CustomerPaths(Path("/in"), Path("/out"))])
pipeline.run(spark=spark, context=context)
```

For a semantic value based on a built-in, create a distinct subclass, such as `class RunDate(date): pass`, and annotate the parameter with that type. The pipeline raises an execution error when a required context value is absent.

## Checklist

1. Define or refine the schemas at each DataFrame boundary.
2. Add `@transform` and explicit input/output contracts before writing transformation logic.
3. Select coercion modes based on ownership and data quality at the boundary.
4. Unit-test transforms with typed fixtures created from schema models.
5. Add transforms to a `Pipeline`, connect the DAG, and run it with a `SparkSession`.
6. Introduce typed `Context()` only for dependencies or configuration supplied at pipeline execution time.