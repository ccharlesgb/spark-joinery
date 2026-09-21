# Type Mappings

There is no 1:1 mapping between Python types and Spark SQL data types. Therefore it is necessary to chose a
sensible default to go from the annotated model type to a Spark type. However you can always use `Annotated` to
override this default. Below is the [reference](https://spark.apache.org/docs/latest/api/python/tutorial/sql/type_conversions.html#all-conversions) mapping from pyspark types to Python types:

| Data type               | Value type in Python       | API to access or create a data type                    |
| ----------------------- | -------------------------- | ------------------------------------------------------ |
| **ByteType**            | int                        | ByteType()                                             |
| **ShortType**           | int                        | ShortType()                                            |
| **IntegerType**         | int                        | IntegerType()                                          |
| **LongType**            | int                        | LongType()                                             |
| **FloatType**           | float                      | FloatType()                                            |
| **DoubleType**          | float                      | DoubleType()                                           |
| **DecimalType**         | decimal.Decimal            | DecimalType()                                          |
| **StringType**          | string                     | StringType()                                           |
| **BinaryType**          | bytes                      | BinaryType()                                           |
| **BooleanType**         | bool                       | BooleanType()                                          |
| **TimestampType**       | datetime.datetime          | TimestampType()                                        |
| **TimestampNTZType**    | datetime.datetime          | TimestampNTZType()                                     |
| **DateType**            | datetime.date              | DateType()                                             |
| **DayTimeIntervalType** | datetime.timedelta         | DayTimeIntervalType()                                  |
| **ArrayType**           | list, tuple, or array      | ArrayType(_elementType_, [_containsNull_])             |
| **MapType**             | dict                       | MapType(_keyType_, _valueType_, [_valueContainsNull_]) |
| **StructType**          | list or tuple              | StructType(_fields_)                                   |
| **StructField**         | Mirrors data type of field | StructField(_name_, _dataType_, [_nullable_])          |

Whenever there is a many-to-one relationship between Python types and Spark SQL data types, the default mapping
will be used unless overridden with `Annotated`. The default mapping is currently:

| Python type          | Default Spark SQL type  |
| -------------------- | ----------------------- |
| `int`                | `LongType()`            |
| `str`                | `StringType()`          |
| `float`              | `DoubleType()`          |
| `bool`               | `BooleanType()`         |
| `datetime.datetime`  | `TimestampType()`       |
| `datetime.date`      | `DateType()`            |
| `datetime.timedelta` | `DayTimeIntervalType()` |
| `decimal.Decimal`    | `DecimalType(38, 18)`   |
| `bytes`              | `BinaryType()`          |
