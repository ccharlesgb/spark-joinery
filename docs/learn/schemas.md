# Defining Schemas

This is the lowest level use of Spark Joinery. Schemas can be defined using [Pydantic](https://pydantic.dev/docs/validation/latest/concepts/models/#basic-model-usage) or Python [dataclasses](https://docs.python.org/3/library/dataclasses.html). Either will work but Pydantic has the
slight advantage of being able to define validation rules for test fixtures.

## Dataclasses

A dataclass schema is defined using the standard Python `dataclass` decorator. Each field in the dataclass corresponds to a column in the Spark dataframe.

``` python
--8<-- "docs_src/learn/schemas/dataclass_example.py"
```

Which outputs:

``` python
--8<-- "docs_src/learn/schemas/dataclass_example_stdout.log"
```

If your data has arrays or nested structures you can define these by nesting the dataclasses as you
would in native python:

``` python
--8<-- "docs_src/learn/schemas/dataclass_example_nested.py"
```

Which gives the schema:

``` python
-8<-- "docs_src/learn/schemas/dataclass_example_nested_stdout.log"
```

## Pydantic Models

Pydantic models work in the same way:

``` python
--8<-- "docs_src/learn/schemas/pydantic_example.py"
```

Which similarly gives:

``` python
--8<-- "docs_src/learn/schemas/pydantic_example_stdout.log"
```

??? warning "Pydantic field validation is not enforced"

    Spark Joinery does not enforce Pydantic validation rules. It can be useful to include them in your models for
    documentation and for generating valid test fixtures, but only the data type is enforced.

## Creating Dataframes from model instances

You can easily create example DataFrames from lists of model instances. This is particularly
useful for building test fixtures for a transformation. For example, given a customer model:

``` python
--8<-- "docs_src/learn/schemas/fixture_example.py"
```

Which outputs:

``` python
--8<-- "docs_src/learn/schemas/fixture_example_stdout.log"
```

## Default types and manually setting data types

By default Spark Joinery will use the documented [conversions](https://spark.apache.org/docs/latest/api/python/tutorial/sql/type_conversions.html#all-conversions) to map Python types to Spark types. However,
this might not be suitable for all use cases for example if you have a `DecimalType` or `VarcharType`. These types cannot be expressed in normal Python so you have to use `Annotated` to override the type in the schema.

There are also some Python types that can map to multiple spark types and this module has made a
choice. For example `int` -> `LongType` and `float` -> `DoubleType`. For a full list of default mappings
see [default type mappings.](/reference/type_mapping/)

## A note on nullability

Nullability is a bit tricky in Spark. This [write up](https://medium.com/@weshoffman/apache-spark-parquet-and-troublesome-nulls-28712b06f836) helps explain how `nullable=False` in a DataFrame schema is just an indicator for the query optimiser and not an actual constraint. Moreover if a DataFrame column is marked as not nullable and the column does contain nulls it can cause erroneous queries. This library makes the decision to always set
columns to be nullable by default.
