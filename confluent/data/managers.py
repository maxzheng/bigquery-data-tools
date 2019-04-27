import re

from google.cloud import bigquery


class AlreadyExistsError(Exception):
    """" Something that we are trying to create already exists """


class UnsupportedError(Exception):
    """" An operation that isn't supported yet """


class BigQueryManager:
    """ Manages BigQuery projects, datasets, etc """

    def __init__(self, client=None):
        self.client = client or bigquery.Client()

    def move_dataset(self, from_dataset, to_project_or_dataset):
        """
        Moves a dataset from a project to another

        :param str from_dataset: Fully qualified dataset name (project.dataset) to move from
        :param str to_project_or_dataset: Project or fully qualified dataset name (project.dataset) to move to.
        """
        source_dataset_ref = self._to_dataset_ref(from_dataset)
        target_dataset_ref = self._to_dataset_ref(self._to_fqdn(to_project_or_dataset, source_dataset_ref.dataset_id))
        print(f'Moving from {source_dataset_ref.project}.{source_dataset_ref.dataset_id} to '
              f'{target_dataset_ref.project}.{target_dataset_ref.dataset_id}')

        self.copy_dataset(from_dataset, to_project_or_dataset)
        self.delete_dataset(from_dataset)

    def copy_dataset(self, from_dataset, to_project_or_dataset):
        """
        Copies a dataset from a project to another

        :param str from_dataset: Fully qualified dataset name (project.dataset) to move from
        :param str to_project_or_dataset: Project or fully qualified dataset name (project.dataset) to move to.
        :raises AlreadyExistsError: If the destination dataset already exist
        """
        source_dataset_ref = self._to_dataset_ref(from_dataset)
        target_dataset_ref = self._to_dataset_ref(self._to_fqdn(to_project_or_dataset, source_dataset_ref.dataset_id))

        # Create destination dataset
        self.client.create_dataset(target_dataset_ref)

        # Copy tables
        for source_table_item in self.client.list_tables(dataset=source_dataset_ref):
            source_table_ref = source_table_item.reference
            target_table_ref = target_dataset_ref.table(source_table_ref.table_id)
            print(f'  - {source_table_ref.table_id}')

            source_table = self.client.get_table(source_table_ref)

            if source_table.table_type == 'TABLE':
                job = self.client.copy_table(source_table_ref, target_table_ref)
                job.result()
                assert job.state == 'DONE'

                target_table = self.client.get_table(target_table_ref)
                source_table = self.client.get_table(source_table_ref)
                assert target_table.num_rows == source_table.num_rows, \
                    'Number of rows does not match'

            elif source_table.table_type == 'VIEW':
                target_view = bigquery.Table(target_table_ref)
                target_view.view_query = source_table.view_query.replace(
                    f'{source_dataset_ref.project}.{source_dataset_ref.dataset_id}',
                    f'{target_dataset_ref.project}.{target_dataset_ref.dataset_id}')
                self.client.create_table(target_view)

            else:
                raise UnsupportedError('Table type {source_table.table_type} is not supported')

        assert len(list(self.client.list_tables(dataset=source_dataset_ref))) == \
            len(list(self.client.list_tables(dataset=target_dataset_ref))), 'Number of tables does not match'

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

    def delete_dataset(self, dataset):
        """ Delete the given dataset """
        dataset_ref = self._to_dataset_ref(dataset)
        self.client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
