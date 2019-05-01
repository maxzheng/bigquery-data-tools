import warnings

import click

from confluent.data.transformers import Transformer, transform_usage_metrics
from confluent.data.admins import BigQueryAdmin


##############################################################################################################
# Script entry points

@click.group(help='Transforms various types of data to make them more usable')
def transform():
    pass


@click.group(help='Complements `bq` command to administer BigQuery projects, datasets, etc')
def bq_admin():
    # This is low-volume API calls so it is easier to just use the user's GCP session instead of making them setup
    # service account credentials.
    warnings.filterwarnings('ignore', '.*authenticated using end user credential.*',)

##############################################################################################################
# Commands for scripts


@transform.command(help='Replace invalid chars in keys and remove useless fields')
@click.option('--source-dir', default='data', help='Directory to read data files from')
@click.option('--sink-dir', default='transformed-data', help='Directory to write transformed data files to')
@click.option('--path-contains', help='Only process paths that contains the provided value')
@click.option('--select-fields', default='-metric.another',
              help='Comma separated list of fields to extract. Use a dot for nested fields. '
                   'To exclude a field, prefix it with a negative sign ("-").')
def usage_metrics(source_dir, sink_dir, path_contains, select_fields):
    if select_fields:
        select_fields = set(select_fields.split(','))
    transformer = Transformer(transform_usage_metrics, source_dir, sink_dir, path_contains=path_contains,
                              select_fields=select_fields)
    transformer.transform()


@bq_admin.command(help='Move a dataset from one project to another')
@click.argument('from_dataset')
@click.argument('to_project_or_dataset')
def move_dataset(from_dataset, to_project_or_dataset):
    print(f'Moving {from_dataset} to {to_project_or_dataset}')

    admin = BigQueryAdmin()
    admin.move_dataset(from_dataset, to_project_or_dataset)


@bq_admin.command(help='Copy a dataset from one project to another. This will skip/continue on unsupported tables.')
@click.argument('from_dataset')
@click.argument('to_project_or_dataset')
def copy_dataset(from_dataset, to_project_or_dataset):
    print(f'Copying {from_dataset} to {to_project_or_dataset}')

    admin = BigQueryAdmin()
    admin.copy_dataset(from_dataset, to_project_or_dataset, error_on_unsupported=False)


@bq_admin.command(help='Create table views based on a view specifications JSON file. '
                       'See `confluent/data/specs.py` for the JSON schema')
@click.argument('view_specs_json_file')
def create_views(view_specs_json_file):
    admin = BigQueryAdmin()
    admin.create_views(view_specs_json_file)
