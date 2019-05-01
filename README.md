# data-tools

Tools for data management and transformation used by the Data Science team.

# Installation

Install using pip -- ideally in a virtual env:

    $ pip install git+https://github.com/confluentinc/data-tools

# Data Management

## BigQuery

### Move/Rename/Copy Dataset

Normal tables and views are supported, but not tables that depends on external data source.

To move a dataset:

    $ bq-admin move-dataset project-name-123:dataset-name new-project-123

To rename a dataset, just use the same project name with different target dataset name:

    $ bq-admin move-dataset project-name-123:dataset-name project-name-123:new-dataset-name

To copy a dataset:

    $ bq-admin copy-dataset project-name-123:dataset-name new-project-123

### Create Table Views

First, create a JSON view spec based on example in [confluent/data/specs.py](confluent/data/specs.py). Let's say it's
saved to /tmp/view-specs.json, to create the table views, run:

    $ bq-admin create-views /tmp/view-specs.json

# Data Transformation

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

# Development

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

# License

This is licensed under [MIT License](LICENSE), so you are free to do anything that you want with the code. :smile:
