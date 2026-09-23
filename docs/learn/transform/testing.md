# Testing

You can unit test your transformations in the same way you would normally but now because
you have already defined the input and output schemas for your production code you can now
use them as a convinient way to produce input fixtures for your tests:

``` python
--8<-- "docs_src/learn/transform/testing/test_example.py"
```

:fontawesome-solid-code: Outputs:

``` python
--8<-- "docs_src/learn/transform/testing/test_example_stdout.log"
```
