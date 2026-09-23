# Context

!!! example

    Pipeline syntax is experimental

Context allows you to inject additional dependencies or configuration into your transformations. You
can do this using the `Context()` marker in your transformation definition:

``` python
--8<-- "docs_src/learn/pipelines/context/context_example_paths.py"
```

:fontawesome-solid-code: Outputs:

``` md
--8<-- "docs_src/learn/pipelines/context/context_example_paths_stdout.log"
```

Context is automatically injected into your transformation from the `PipelineContext`. There is
currently no way to disambiguate between multiple context values of the same type, so you should
use your own class definitions instead of built-in types like `str` or `int`. One common example is
if you are reading day partitioned date and you want to process todays data. It would be best practice
to inherit from the `date` class to ensure the context is injected explicitly:

``` python
--8<-- "docs_src/learn/pipelines/context/context_example_run_date.py"
```

:fontawesome-solid-code: Outputs:

``` md
--8<-- "docs_src/learn/pipelines/context/context_example_run_date_stdout.log"
```

## Validating Context

If a transformation requires a context value but you don't provide it you will get an error:

``` python
--8<-- "docs_src/learn/pipelines/context/context_example_unsatisifed.py"
```

:fontawesome-solid-code: Outputs:

``` md
--8<-- "docs_src/learn/pipelines/context/context_example_unsatisifed_stdout.log"
```
