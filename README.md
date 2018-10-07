# dag

After `pip` installing the example pipeline, you can run it with the shell script in the runners directory, or directly with:

    python -m luigi --module dag.pipeline HoroscopeReportTask --local-scheduler

To see the DAG, use the `traversal` module
