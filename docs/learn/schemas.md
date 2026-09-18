# Defining Schemas

This is the lowest level use of Spark Joinery. Schemas can be defined using [Pydantic](https://pydantic.dev/docs/validation/latest/concepts/models/#basic-model-usage) or Python [dataclasses](https://docs.python.org/3/library/dataclasses.html). Either will work but Pydantic has the 
slight advantage of being able to define validation rules for test fixtures.

## Defining dataclass schemas

A dataclass schema is defined using the standard Python `dataclass` decorator. Each field in the dataclass corresponds to a column in the Spark dataframe.

```python
--8<-- "docs_src/learn/schemas/dataclass_example.py"
```

Which outputs:

```python
StructType([
    StructField('customer_id', StringType(), True), 
    StructField('name', StringType(), True), StructField('is_active', BooleanType(), True), 
    StructField('discount_rate', FloatType(), True),
    StructField('max_billing_amount', DecimalType(10,2), True)
    ])
```

If your data has arrays or nested structures you can define these by nesting the dataclasses as you 
would in native python:

```python
--8<-- "docs_src/learn/schemas/dataclass_example_nested.py"
```

Which gives the schema:

```python
StructType([
    StructField('customer_id', StringType(), True), 
    StructField('name', StringType(), True), 
    StructField('employees', ArrayType(StructType([
        StructField('name', StringType(), True), 
        StructField('phone_number', StringType(), True)]), True), 
        True),
    StructField('location', 
        StructType([StructField('latitude', FloatType(), True), 
        StructField('longitude', FloatType(), True)]),True)
    ])
```

## Default types and manually setting data types

By default Spark Joinery will use the documented [conversions](https://spark.apache.org/docs/latest/api/python/tutorial/sql/type_conversions.html#all-conversions) to map Python types to Spark types. However, 
this might not be suitable for all use cases for example if you have a `DecimalType` or `VarcharType`. These types cannot be expressed in normal Python so you have to use `Annotated` to override the type in the schema.

