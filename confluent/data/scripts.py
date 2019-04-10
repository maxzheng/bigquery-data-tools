import click

from confluent.data.transformers import Transformer, transform_usage_metrics


@click.group()
def transform():
    pass


@transform.command(help='Replace invalid chars in keys and remove useless fields')
@click.option('--source-dir', default='data', help='Directory to read data files from')
@click.option('--sink-dir', default='transformed-data', help='Directory to write transformed data files to')
@click.option('--path-contains', help='Only process paths that contains the provided value')
def usage_metrics(source_dir, sink_dir, path_contains):
    transformer = Transformer(transform_usage_metrics, source_dir, sink_dir, path_contains=path_contains)
    transformer.transform()
