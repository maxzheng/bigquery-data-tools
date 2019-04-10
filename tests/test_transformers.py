import json
import gzip
import os

from utils.fs import in_temp_dir

from confluent.data.transformers import Transformer, transform_usage_metrics
from confluent.data.scripts import transform


def test_usage_metrics(cli_runner):
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

        result = cli_runner.invoke_and_assert_exit(0, transform, ['usage-metrics'])

        assert ('Transforming data files from "data" and writing them to "transformed-data" '
                'using 5 parallel processes\n') in result.output
        assert 'Transformed 1 data file(s)' in result.output

        expected_record = {'value': 'a', '_timestamp': 'b', 'id': 'c', 'source': 'd', '_version': 'e',
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

        # No match
        result = cli_runner.invoke_and_assert_exit(0, transform, ['usage-metrics', '--path-contains', 'blah'])
        assert 'No data files found in "data" dir matching "blah"' in result.output

        # Match
        result = cli_runner.invoke_and_assert_exit(0, transform, ['usage-metrics', '--path-contains', 'data'])
        assert ('Transforming data files from "data" and writing them to "transformed-data" '
                'using 5 parallel processes\n') in result.output

        # Re-run by calling the `_transform_file` method ourselves as text coverage does not see the code as tested as
        # the above code runs them in a child process.
        transformer = Transformer(transform_usage_metrics, 'data', 'updated-data')
        transformer._transform_file('data/test.json.gz')

        count = 0
        for serialized_record in gzip.open('updated-data/test.json.gz'):
            record = json.loads(serialized_record)
            assert expected_record == record
            count += 1

        assert count == 10
