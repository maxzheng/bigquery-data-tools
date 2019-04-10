# data-tools

Tools for data transformation

## Defaults and Options

By default, input data should be in the "data" directory. Transformed data will be written to "transformed-data"
directory. And 5 parallel processes are used to process the data concurrently.

They can be changed via CLI options. See usage info by passing `--help` to the command.

## Usage Metrics

This script will replace invalid keys (based on BigQuery column naming convention) with underscores and may remove some
not useful keys.  To do the transform, simply run:

    $ transform usage-metrics
    Transforming data files from "data" and writing them to "transformed-data" using 5 parallel processes
    ...
