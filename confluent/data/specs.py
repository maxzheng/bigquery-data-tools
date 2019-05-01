from functools import lru_cache
import json
from pathlib import Path


@lru_cache()
def parse_view_specs(json_file):
    """
    List of object representation for table view specs, which is a JSON that contains the following info::

        {
            "latest-record": {
                "$project_name": {
                    "$dataset_name": {
                        "ids": ["$table_ids"],
                        "datetime: "$datetime",
                    }
                }
            }
        }

    where:
        latest-record: Create a view that shows the latest record by the given unique ID sorted by datetime field.
            $project_name: Name of the project to create views in
            $dataset_name: Name of the dataset to create views in
            $table_ids: List of possible unique IDs to partition the view by -- only one of them must exist on the table
            $datetime: Field name of the datetime field to sort by to get latest record.


    :param str json_file: Path to view specs JSON
    :rtype: list[AbstractViewSpec]
    """
    with Path(json_file).open() as fp:
        view_specs = json.load(fp)

    specs = []

    for serial_key in view_specs:
        if serial_key == LatestRecordViewSpec.SERIAL_KEY:
            for project in view_specs[serial_key]:
                for dataset in view_specs[serial_key][project]:
                    view_spec = LatestRecordViewSpec(project, dataset,
                                                     view_specs[serial_key][project][dataset]['ids'],
                                                     view_specs[serial_key][project][dataset]['datetime'])
                    specs.append(view_spec)

        else:
            raise ValueError(f'Unsupported serial key in views specs: {serial_key}')

    return specs


class AbstractViewSpec:
    """ Abstract object representation for a table view spec for all view specs """
    def __init__(self, project, dataset):
        """
        :param str project: Project name
        :param str dataset: Dataset name
        """
        self.project = project
        self.dataset = dataset

    def sql(self, table, table_fields):
        """
        :param str table: Name of the table
        :param set[str] table_fields: List of fields in the table
        :return: SQL statement that can be used to create the view.
        """
        raise NotImplementedError('Sub-class should implement to return a SQL that creates the view')


class LatestRecordViewSpec(AbstractViewSpec):
    """ A table view spec for latest record """

    #: Serialized key that represents this view
    SERIAL_KEY = 'latest-record'

    def __init__(self, project, dataset, id_fields, datetime_field):
        """
        :param str project: Project name
        :param str dataset: Dataset name
        :param set[str] id_fields: List of possible unique IDs to partition the view by -- only one of them must
                                   exist on the table.
        :param str datetime_field: Table field for datetime to sort by
        """
        super().__init__(project, dataset)
        self.id_fields = set(id_fields)
        self.datetime_field = datetime_field

    def sql(self, table, table_fields):
        """
        :param str table: Name of the table from `self.project` and `self.dataset`
        :param set[str] table_fields: List of fields in the table
        :return: SQL statement that can be used to create the view.
        """
        common_fields = self.id_fields.intersection(table_fields)

        if len(common_fields) < 1:
            raise ValueError(f'None of the possible unique IDs ({self.id_fields}) are found in: {table_fields}')

        elif len(common_fields) > 1:
            raise ValueError(f'Multiple unique IDs ({common_fields}) are found, but only one is expected: '
                             f'{table_fields}')

        id_field = next(iter(common_fields))
        return f"""
SELECT * EXCEPT (ROW_NUMBER)
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY {id_field} ORDER BY {self.datetime_field} DESC) ROW_NUMBER
    FROM `{self.project}.{self.dataset}.{table}`
)
WHERE ROW_NUMBER = 1
"""
