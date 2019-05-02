import re

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from confluent.data.specs import parse_view_specs


class AlreadyExistsError(Exception):
    """" Something that we are trying to create already exists """


class UnsupportedError(Exception):
    """" An operation that isn't supported yet """


class BigQueryAdmin:
    """ Manages BigQuery projects, datasets, etc """

    def __init__(self, client=None):
        self.client = client or bigquery.Client()

    def move_dataset(self, from_dataset, to_project_or_dataset):
        """
        Moves a dataset from a project to another

        :param str from_dataset: Fully qualified dataset name (project.dataset) to move from
        :param str to_project_or_dataset: Project or fully qualified dataset name (project.dataset) to move to.
        """
        self.copy_dataset(from_dataset, to_project_or_dataset)
        self._delete_dataset(from_dataset)

    def create_views(self, view_specs_json_file):
        """
        Create table views based on the given view specifications

        :param str view_specs_json_file: Path to JSON file with view specs
        """
        view_specs = parse_view_specs(view_specs_json_file)

        for view_spec in view_specs:
            dataset_ref = self.client.dataset(dataset_id=view_spec.dataset, project=view_spec.project)
            print(f'Creating views for {view_spec.project}.{view_spec.dataset}')

            for table_item in self.client.list_tables(dataset=dataset_ref):
                table_ref = table_item.reference

                table = self.client.get_table(table_ref)
                if table.table_type == 'TABLE' and not table.table_id.startswith('_'):
                    table_view_ref = dataset_ref.table(table_ref.table_id + '_view')
                    try:
                        self.client.get_table(table_view_ref)
                        print(f'  - {table_view_ref.table_id} (already exists)')
                        continue
                    except NotFound:
                        pass

                    print(f'  - {table_view_ref.table_id}')

                    fields = [f.name for f in table.schema]
                    table_view = bigquery.Table(table_view_ref)
                    table_view.view_query = view_spec.sql(table.table_id, fields)
                    self.client.create_table(table_view)

    def copy_dataset(self, from_dataset, to_project_or_dataset, error_on_unsupported=True):
        """
        Copies a dataset from a project to another

        :param str from_dataset: Fully qualified dataset name (project.dataset) to move from
        :param str to_project_or_dataset: Project or fully qualified dataset name (project.dataset) to move to.
        :param bool error_on_unsupported: Raise an error for unsupported tables (e.g. external)
        :raises AlreadyExistsError: If the destination dataset already exist
        """
        source_dataset_ref = self._to_dataset_ref(from_dataset)
        target_dataset_ref = self._to_dataset_ref(self._to_fqdn(to_project_or_dataset, source_dataset_ref.dataset_id))

        # Create destination dataset
        self.client.create_dataset(target_dataset_ref)

        table_views = []
        skipped_tables = []

        # Copy tables
        for source_table_item in self.client.list_tables(dataset=source_dataset_ref):
            source_table_ref = source_table_item.reference
            target_table_ref = target_dataset_ref.table(source_table_ref.table_id)

            source_table = self.client.get_table(source_table_ref)

            if source_table.table_type == 'TABLE':
                print(f'  - {source_table_ref.table_id}')
                job = self.client.copy_table(source_table_ref, target_table_ref)
                job.result()
                assert job.state == 'DONE'

                target_table = self.client.get_table(target_table_ref)
                source_table = self.client.get_table(source_table_ref)
                assert target_table.num_rows == source_table.num_rows, \
                    'Number of rows does not match'

            elif source_table.table_type == 'VIEW':
                table_views.append((source_table, source_table_ref, target_table_ref))

            else:
                print(f'  - {source_table_ref.table_id}')
                if error_on_unsupported:
                    raise UnsupportedError(f'Table type {source_table.table_type} is not supported for '
                                           f'table {source_table_ref.table_id}')
                else:
                    print(f'    Skipped due to unsupported table type: {source_table.table_type}')
                    skipped_tables.append(source_table_ref.table_id)

        for source_table, source_table_ref, target_table_ref in table_views:
            print(f'  - {source_table_ref.table_id} (view)')
            if any(table in source_table.view_query for table in skipped_tables):
                print('    Skipped as view is for an unsupported table that was not copied')
                skipped_tables.append(source_table_ref.table_id)
                continue

            target_view = bigquery.Table(target_table_ref)
            target_view.view_use_legacy_sql = source_table.view_use_legacy_sql
            target_view.view_query = source_table.view_query.replace(
                f'{source_dataset_ref.project}.{source_dataset_ref.dataset_id}',
                f'{target_dataset_ref.project}.{target_dataset_ref.dataset_id}')
            self.client.create_table(target_view)

        assert len(list(self.client.list_tables(dataset=source_dataset_ref))) == \
            (len(list(self.client.list_tables(dataset=target_dataset_ref))) + len(skipped_tables)), \
            'Number of tables does not match'

    def _to_dataset_ref(self, fqdn):
        """
        Convert a fully qualified dataset name (project.dataset) to a
        :cls:`google.cloud.bigquery.dataset.DatasetReference` object
        """
        parts = re.split(r'[.:]', fqdn, maxsplit=1)
        if len(parts) < 2 or len(parts) > 2:
            raise ValueError(f'Invalid dataset name ({fqdn}). Please provide a fully qualified '
                             'dataset name (project.dataset)')

        return self.client.dataset(dataset_id=parts[1], project=parts[0])

    def _to_fqdn(self, project, dataset):
        """
        Returns the fully qualified dataset name (project.name) for the given project and dataset name.
        If the project contains dataset already, it will be returned and the dataset param won't be used.
        """
        try:
            self._to_dataset_ref(project)
            return project
        except ValueError:
            return f'{project}.{dataset}'

    def _delete_dataset(self, dataset):
        """ Delete the given dataset """
        dataset_ref = self._to_dataset_ref(dataset)
        self.client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
