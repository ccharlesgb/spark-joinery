# Dependency Injection

pyspark-schemas supports dependency injection in a similar way to FastAPI. This
helps with configuring parameterisable transformations such as reading/filtering steps.

To specify a dependency you can use the `Context` annotation. It resolves the
parameter by its annotated type against values registered on a
`PipelineContext`:

```python
from dataclasses import dataclass
from datetime import date
from typing import Annotated

from pyspark.sql import DataFrame, SparkSession
from pyspark_schemas import Collection, Context, Pipeline, PipelineContext

from .schemas import User, Department, UserWithDepartment

user_pipeline = Collection()


@dataclass(frozen=True)
class UsersPath:
    value: str


@dataclass(frozen=True)
class DepartmentsPath:
    value: str


@dataclass(frozen=True)
class RunDate:
    value: date


@user_pipeline.transform
def read_users(
    spark: SparkSession, path: Annotated[UsersPath, Context()]
) -> Annotated[DataFrame, User]:
    return spark.read.table(path.value)


@user_pipeline.transform
def filter_users(
    users: Annotated[DataFrame, User], run_date: Annotated[RunDate, Context()]
) -> Annotated[DataFrame, User]:
    return users.filter(col("date") == run_date.value)


@user_pipeline.transform
def read_departments(
    spark: SparkSession, path: Annotated[DepartmentsPath, Context()]
) -> Annotated[DataFrame, Department]:
    return spark.read.table(path.value)


@user_pipeline.transform
def join_users_to_departments(
    users: Annotated[DataFrame, User], departments: Annotated[DataFrame, Department]
) -> Annotated[DataFrame, UserWithDepartment]:
    return users.join(departments, on="department_id")
```

`Context()` resolves by the parameter's annotated type (`UsersPath`,
`RunDate`, ...). Each type registered on a `PipelineContext` must be
distinct — registering two values of the same type raises a `ValueError`.
Small wrapper dataclasses (like `UsersPath`/`DepartmentsPath` above, both
wrapping a `str`) are the recommended way to keep dependencies unambiguous.

If you intend to use the transformation in a pipeline you can then do:

```python
def get_users_pipeline() -> Pipeline:
    pipeline = Pipeline(collections=[user_pipeline])
    read_users_step = pipeline.add_step(read_users, "users_table")
    filter_users_step = pipeline.add_step(filter_users, "filtered_users")
    pipeline.connect(read_users_step, filter_users_step)

    read_departments_step = pipeline.add_step(read_departments, "departments_table")

    join_users_to_departments_step = pipeline.add_step(
        join_users_to_departments, "joined"
    )
    pipeline.connect_many(
        [filter_users_step, read_departments_step], join_users_to_departments_step
    )

    return pipeline


if __name__ == "__main__":
    executable = get_users_pipeline().validate()
    context = PipelineContext(
        values=[
            UsersPath("gs://my-prod-bucket/users"),
            DepartmentsPath("gs://my-prod-bucket/departments"),
            RunDate(date(2026, 1, 1)),
        ]
    )
    outputs = executable.run(spark, context)
```

`ExecutablePipeline.run` takes the `SparkSession` and an optional
`PipelineContext`. A step with `Context`-annotated parameters raises a
`PipelineExecutionError` if no context is passed, or if a required
dependency isn't registered on the context.

