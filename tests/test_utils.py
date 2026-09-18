from pyspark.sql import types

from spark_joinery.utils import get_callable_name, pretty_print_struct_type


def test_get_callable_name_on_named_function():
    def sample_function():
        pass

    assert get_callable_name(sample_function) == "sample_function"


def test_get_callable_name_on_lambda():
    sample_lambda = lambda x: x  # noqa
    assert get_callable_name(sample_lambda) is None


def test_get_callable_on_callable_class():
    class CallableClass:
        def __call__(self):
            pass

    instance = CallableClass()
    assert get_callable_name(instance) == "CallableClass"


def test_pretty_print_struct_type(capsys):
    schema = types.StructType(
        [
            types.StructField("name", types.StringType()),
            types.StructField(
                "location",
                types.StructType([types.StructField("latitude", types.DoubleType())]),
            ),
            types.StructField(
                "employees",
                types.ArrayType(
                    types.StructType(
                        [types.StructField("phone_number", types.StringType())]
                    )
                ),
            ),
        ]
    )

    pretty_print_struct_type(schema)

    assert (
        capsys.readouterr().out
        == """StructType([
    StructField('name', StringType(), True),
    StructField(
        'location',
        StructType([
            StructField('latitude', DoubleType(), True)
        ]),
        True
    ),
    StructField(
        'employees',
        ArrayType(
            StructType([
                StructField('phone_number', StringType(), True)
            ]),
            True
        ),
        True
    )
])
"""
    )
