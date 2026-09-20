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
