from google.api_core.exceptions import NotFound
from mock import Mock
import pytest

from confluent.data.scripts import bq_admin


@pytest.fixture
def bq_client(monkeypatch):
    client = Mock()
    client().dataset.return_value = Mock(table=Mock(side_effect=[
        Mock(table_id='table1'),
        Mock(table_id='table2'),
        Mock(table_id='table3'),
        Mock(table_id='table4'),
        Mock(table_id='table5'),
        Mock(table_id='table6'),
        ]))
    client().list_tables.return_value = [
        Mock(reference=Mock(table_id='table1')),
        Mock(reference=Mock(table_id='table2'))]
    monkeypatch.setattr('google.cloud.bigquery.Client', client)
    return client


def test_create_views(bq_client, cli_runner, test_data):
    name_mock = Mock()
    name_mock.name = 'id'
    bq_client().get_table.side_effect = [
        Mock(table_type='TABLE', table_id='table1', schema=[name_mock, Mock()]),
        NotFound('table1 was not found'),
        Mock(table_type='TABLE', table_id='table2', schema=[name_mock, Mock()]),
        Mock(table_type='TABLE', table_id='table2', schema=[name_mock, Mock()]),
        Mock(table_type='TABLE', table_id='table3', schema=[name_mock, Mock()]),
        NotFound('table3 was not found'),
        Mock(table_type='TABLE', table_id='table4', schema=[name_mock, Mock()]),
        NotFound('table4 was not found'),
        Mock(table_type='TABLE', table_id='table5', schema=[name_mock, Mock()]),
        NotFound('table5 was not found'),
        Mock(table_type='TABLE', table_id='table6', schema=[name_mock, Mock()]),
        NotFound('table6 was not found'),
    ]
    result = cli_runner.invoke_and_assert_exit(
        0, bq_admin, ['create-views', str(test_data.path('bigquery-view-specs.json'))])
    assert result.stdout == """\
Creating views for project-12345.marketo
  - table1
  - table2 (already exists)
Creating views for project-12345.salesforce
  - table3
  - table4
"""


def test_move_dataset(bq_client, cli_runner):
    bq_client().copy_table.side_effect = [
        Mock(state='DONE'),
        Mock(state='DONE')
    ]
    bq_client().get_table.side_effect = [
        Mock(table_type='TABLE', table_id='table1', num_rows=10),
        Mock(table_type='TABLE', table_id='table1', num_rows=10),
        Mock(table_type='TABLE', table_id='table1', num_rows=10),
        Mock(table_type='TABLE', table_id='table2', num_rows=20),
        Mock(table_type='TABLE', table_id='table2', num_rows=20),
        Mock(table_type='TABLE', table_id='table2', num_rows=20),
    ]
    result = cli_runner.invoke_and_assert_exit(0, bq_admin, ['move-dataset', 'project-1:dataset', 'project-2'])
    assert result.stdout == """\
Moving project-1:dataset to project-2
  - table1
  - table2
"""
