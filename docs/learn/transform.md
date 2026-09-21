# Transformations

Spark Joinery provides a decorator for defining transformations on DataFrames. This allows you to
annotate input and output schemas making it much clearer what the transformation does:

``` python
--8<-- "docs_src/learn/transform/decorator_example.py"
```

Which outputs:

``` python
--8<-- "docs_src/learn/transform/decorator_example_stdout.log"
```

## Defining a contract

You can define an input contract by annotating any input parameters with a valid [coercion mode](/reference/transform/#contracts) and
schema. This example shows a transform that will project any input dataframe but it will be strict
on it's output schema. This would be a transformation following the [robustness principle](https://en.wikipedia.org/wiki/Robustness_principle):

``` python
@transform
def filter_active_customers(
    customers: Annotated[DataFrame, ProjectAll(Customer)],
) -> Annotated[DataFrame, Strict(Customer)]:
    return customers.filter(customers.is_active)
```

For read steps of wide/nested tables you might want to use `ProjectAll` as the coercion mode
instead of writing out the full schema explicitly. This can be especially useful if you only
want to select a few fields of highly nested data:

``` python
@transform
def read_nested_event_data(
    spark: SparkSession,
    path: str
) -> Annotated[DataFrame, ProjectAll(Event)]:
    return spark.read.parquet(path)
```

# Running Transformations

You can use transformations like you would normally in a Spark job. For example:

``` python
--8<-- "docs_src/learn/transform/usage_example_happy.py"
```

Which outputs:

``` python
--8<-- "docs_src/learn/transform/usage_example_happy_stdout.log"
```

If something is wrong with your transformation chain then you will get a runtime error, because of
the lazy evaluation model in Spark this will happen even before any work is done*:

``` python
--8<-- "docs_src/learn/transform/usage_example_bad.py"
```

Which outputs:

``` python
--8<-- "docs_src/learn/transform/usage_example_bad_stdout.log"
```

!!! warning

    If your transformations perform actions, for example `collect()`, then Spark may do significant
    processing before the `SchemaCoercionError` occurs.

# Testing Transformations

You can unit test your transformations in the same way you would normally but now because
you have already defined the input and output schemas for your production code you can now
use them as a convinient way to produce input fixtures for your tests:

``` python
--8<-- "docs_src/learn/transform/test_example.py"
```

Which outputs:

``` python
--8<-- "docs_src/learn/transform/test_example_stdout.log"
```
