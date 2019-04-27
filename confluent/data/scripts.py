import warnings

import click

from confluent.data.transformers import Transformer, transform_usage_metrics
from confluent.data.managers import BigQueryManager


@click.group(help='Transforms various types of data to make them more usable')
def transform():
    pass


@transform.command(help='Replace invalid chars in keys and remove useless fields')
@click.option('--source-dir', default='data', help='Directory to read data files from')
@click.option('--sink-dir', default='transformed-data', help='Directory to write transformed data files to')
@click.option('--path-contains', help='Only process paths that contains the provided value')
@click.option('--select-fields', help='Comma separated list of fields to extract. Use a dot for nested fields. '
                                      'To exclude a field, prefix it with a negative sign ("-").')
def usage_metrics(source_dir, sink_dir, path_contains, select_fields):
    if select_fields:
        select_fields = set(select_fields.split(','))
    transformer = Transformer(transform_usage_metrics, source_dir, sink_dir, path_contains=path_contains,
                              select_fields=select_fields)
    transformer.transform()


@click.group(help='Manage BigQuery projects, datasets, etc')
def bq_admin():
    pass


@bq_admin.command(help='Move a dataset from one project to another')
@click.argument('from_dataset')
@click.argument('to_project_or_dataset')
def move_dataset(from_dataset, to_project_or_dataset):
    warnings.filterwarnings('ignore', '.*authenticated using end user credential.*',)
    man = BigQueryManager()
    man.move_dataset(from_dataset, to_project_or_dataset)
