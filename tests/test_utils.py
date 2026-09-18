from spark_joinery.utils import get_callable_name


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
