import click

from confluent.data.transformers import Transformer, transform_usage_metrics


@click.group(help='Transforms various types of data to make them more usable')
def transform():
    pass


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
