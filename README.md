# data-tools

Tools for data transformation used by the Data Science team.

## Defaults and Options

By default, input data will be read from "data" directory. Transformed data will be written to "transformed-data"
directory. And 5 parallel processes are used to process the data concurrently.

They can be changed via CLI options. See usage info by passing `--help` to a command.

## Usage Metrics

This script will replace invalid keys (based on BigQuery column naming convention) with underscores and may remove some
not useful keys.  To do the transform, simply run:

    $ transform usage-metrics
    Transforming data files from "data" and writing them to "transformed-data" using 5 parallel processes
    ...

## Development

To contribute to the project, follow these steps to setup your development virtualenv to test your changes.

We are using [tox](https://tox.readthedocs.io/en/latest/) to manage our virtualenvs and
[pytest](https://docs.pytest.org/en/latest/) to run our tests. So let's set that up first:

    # Install Python3.7 if not already installed
    $ brew install python3

    # Install tox
    $ sudo pip install tox

Now we can setup our development venv and run tests by simply calling `tox`:

    $ tox

To run the scripts, activate the venv:

    $ source ~/.virtualenvs/data-tools/bin/activate

And then you can run the console scripts from [setup.py](setup.py) file, e.g.:

    $ transform --help

Now, you can make any changes in the source code, and it will be reflected in the scripts.
