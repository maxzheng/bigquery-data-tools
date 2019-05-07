import json
import gzip
import os

import pytest
from utils.fs import in_temp_dir

from confluent.data.transformers import Transformer, transform_usage_metrics
from confluent.data.scripts import transform


@pytest.fixture
def mock_data():
    with in_temp_dir():
        os.mkdir('data')
        with gzip.open('data/test.json.gz', 'wt') as fp:
            for _ in range(10):
                fp.write(json.dumps({"value": "a", "@timestamp": "b", "id": "c", "source": "d", "@version": "e",
                                     "metric": {
                                        "request": "f", "user": "g",
                                        "physicalstatefulcluster.core.confluent.cloud/version": "h",
                                        "statefulset.kubernetes.io/pod-name": "i", "type": "j",
                                        "_deltaSeconds": "k", "job": "l", "pod-name": "m",
                                        "physicalstatefulcluster.core.confluent.cloud/name": "n",
                                        "source": "o", "tenant": "p", "clusterId": "q", "_metricname": "r",
                                        "another": "s", "instance": "t", "pscVersion": "u"},
                                     "timestamp": 1234567}) + '\n')

        yield


def test_usage_metrics_without_options(cli_runner, mock_data):
    result = cli_runner.invoke_and_assert_exit(0, transform, ['usage-metrics'])

    assert ('Transforming data files from "data" and writing them to "transformed-data" '
            'using 5 parallel processes\n') in result.output
    assert 'Transformed 1 data file(s)' in result.output

    expected_record = {'value': 'a', 'datetime_pt': '1970-01-14 22:56:07', 'id': 'c', 'source': 'd', '_version': 'e',
                       'metric': {
                          'request': 'f', 'user': 'g',
                          'physicalstatefulcluster_core_confluent_cloud_version': 'h',
                          'statefulset_kubernetes_io_pod_name': 'i',
                          'type': 'j', '_deltaSeconds': 'k', 'job': 'l', 'pod_name': 'm',
                          'physicalstatefulcluster_core_confluent_cloud_name': 'n', 'source': 'o',
                          'tenant': 'p', 'clusterId': 'q', '_metricname': 'r', 'instance': 't',
                          'pscVersion': 'u'},
                       'timestamp': 1234567}

    count = 0
    for serialized_record in gzip.open('transformed-data/test.json.gz'):
        record = json.loads(serialized_record)
        assert expected_record == record
        count += 1

    assert count == 10

    # Re-run by calling the `_transform_file` method ourselves as text coverage does not see the code as tested as
    # the above code runs them in a child process.
    transformer = Transformer(transform_usage_metrics, 'data', 'updated-data')
    transformer._transform_file('data/test.json.gz')
    expected_record['metric']['another'] = 's'

    count = 0
    for serialized_record in gzip.open('updated-data/test.json.gz'):
        record = json.loads(serialized_record)
        assert expected_record == record
        count += 1

    assert count == 10


def test_usage_metrics_select_fields(cli_runner, mock_data):
    result = cli_runner.invoke_and_assert_exit(0, transform, [
        'usage-metrics', '--select-fields', 'id,metric,metric.user,metric.type,timestamp,@version'])

    assert ('Transforming data files from "data" and writing them to "transformed-data" '
            'using 5 parallel processes\n') in result.output
    assert 'Transformed 1 data file(s)' in result.output
    assert 'Only extracting these fields: @version, id, metric, metric.type, metric.user, timestamp' in result.output

    expected_record = {'id': 'c',
                       '_version': 'e',
                       'metric': {
                          'user': 'g',
                          'type': 'j'},
                       'timestamp': 1234567}

    count = 0
    for serialized_record in gzip.open('transformed-data/test.json.gz'):
        record = json.loads(serialized_record)
        assert expected_record == record
        count += 1

    assert count == 10


def test_usage_metrics_exclude_fields_with_others(cli_runner, mock_data):
    result = cli_runner.invoke_and_assert_exit(0, transform, [
        'usage-metrics', '--select-fields', 'id,metric,-metric.user,metric.type,-timestamp,@version'])

    assert ('Transforming data files from "data" and writing them to "transformed-data" '
            'using 5 parallel processes\n') in result.output
    assert 'Transformed 1 data file(s)' in result.output
    assert 'Only extracting these fields: @version, id, metric, metric.type' in result.output
    assert 'Excluding these fields: metric.user, timestamp' in result.output

    expected_record = {'id': 'c',
                       '_version': 'e',
                       'metric': {
                          'type': 'j'}}

    count = 0
    for serialized_record in gzip.open('transformed-data/test.json.gz'):
        record = json.loads(serialized_record)
        assert expected_record == record
        count += 1

    assert count == 10


def test_usage_metrics_exclude_fields_only(cli_runner, mock_data):
    result = cli_runner.invoke_and_assert_exit(0, transform, [
        'usage-metrics', '--select-fields', '-metric.user'])

    assert ('Transforming data files from "data" and writing them to "transformed-data" '
            'using 5 parallel processes\n') in result.output
    assert 'Transformed 1 data file(s)' in result.output
    assert 'Only extracting these fields' not in result.output
    assert 'Excluding these fields: metric.user' in result.output

    expected_record = {'value': 'a', 'datetime_pt': '1970-01-14 22:56:07', 'id': 'c', 'source': 'd', '_version': 'e',
                       'metric': {
                          'request': 'f',
                          'physicalstatefulcluster_core_confluent_cloud_version': 'h',
                          'statefulset_kubernetes_io_pod_name': 'i',
                          'type': 'j', '_deltaSeconds': 'k', 'job': 'l', 'pod_name': 'm',
                          'physicalstatefulcluster_core_confluent_cloud_name': 'n', 'source': 'o',
                          'tenant': 'p', 'clusterId': 'q', '_metricname': 'r', 'another': 's', 'instance': 't',
                          'pscVersion': 'u'},
                       'timestamp': 1234567}

    count = 0
    for serialized_record in gzip.open('transformed-data/test.json.gz'):
        record = json.loads(serialized_record)
        assert expected_record == record
        count += 1

    assert count == 10


def test_usage_metrics_path_contains(cli_runner, mock_data):
    # No match
    result = cli_runner.invoke_and_assert_exit(0, transform, ['usage-metrics', '--path-contains', 'blah'])
    assert 'No data files found in "data" dir matching "blah"' in result.output

    result = cli_runner.invoke_and_assert_exit(0, transform, ['usage-metrics', '--path-contains', 'data'])
    assert ('Transforming data files from "data" and writing them to "transformed-data" '
            'using 5 parallel processes\n') in result.output
