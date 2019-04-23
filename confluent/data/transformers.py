import gzip
import json
import multiprocessing
import os
import re


INVALID_KEY_CHARS_RE = re.compile('[^a-zA-Z0-9_]')


class Transformer:
    """ Manager for transforming data files in parallel """

    def __init__(self, transform, source_dir, sink_dir, path_contains=None, select_fields=None, parallel_processes=5):
        """
        Run transforms in parallel in multiple processes

        :param callable transform: A callable that accepts an input file and output file and transforms the input to
                                   output.
        :param str source_dir: Directory to read data files from
        :param str sink_dir: Directory to write data files to
        :param str|None path_contains: Only process paths that contains the given value
        :param set|None select_fields: A set of fields to extract from data files. Use a dot for nested fields.
                                       To exclude a field, prefix it with a negative sign ("-").
        :param int parallel_processes: Number of processes to use
        """
        self._transform = transform
        self.source_dir = source_dir
        self.sink_dir = sink_dir
        self.path_contains = path_contains
        self.parallel_processes = parallel_processes

        # Split select vs exclude fields
        self.select_fields = select_fields
        self.exclude_fields = None
        if self.select_fields:
            fields_with_exclude_prefix = set(f for f in self.select_fields if f.startswith('-'))
            self.exclude_fields = set(f.lstrip('-') for f in fields_with_exclude_prefix)
            self.select_fields = self.select_fields - fields_with_exclude_prefix

    def transform(self):
        """ Transform data files if not already done """
        print(f'Transforming data files from "{self.source_dir}" and writing them to "{self.sink_dir}" '
              f'using {self.parallel_processes} parallel processes')
        if self.select_fields:
            print('Only extracting these fields:', ', '.join(sorted(self.select_fields)))
        if self.exclude_fields:
            print('Excluding these fields:', ', '.join(sorted(self.exclude_fields)))

        data_files = []
        for (dirpath, dirnames, filenames) in os.walk(self.source_dir):
            if not self.path_contains or self.path_contains in dirpath:
                data_files.extend([os.path.join(dirpath, name) for name in filenames])

        if data_files:
            print('-' * 80)
            try:
                process_pool = multiprocessing.Pool(self.parallel_processes)
                process_pool.map(self._transform_file, data_files)

                process_pool.close()
                process_pool.join()

                print('Transformed', len(data_files), 'data file(s)')

            except KeyboardInterrupt:
                process_pool.terminate()
                process_pool.join()
                raise

        else:
            match_criteria = f'matching "{self.path_contains}"' if self.path_contains else ''
            print(f'No data files found in "{self.source_dir}" dir {match_criteria}')

    def _transform_file(self, input_file):
        """ Wraps self._transform callable to do exception/output file handling """
        output_file = os.path.join(self.sink_dir, input_file[len(self.source_dir)+1:])
        if os.path.exists(output_file):
            print(f'Skipping transform as output file already exists: {output_file}')
            return

        print('Transforming', input_file)

        try:
            temp_file = os.path.join(os.path.dirname(output_file), '.' + os.path.basename(output_file))
            os.makedirs(os.path.dirname(temp_file), exist_ok=True)

            self._transform(input_file, temp_file, select_fields=self.select_fields, exclude_fields=self.exclude_fields)

            os.rename(temp_file, output_file)

        except (KeyboardInterrupt, Exception) as e:
            if isinstance(e, KeyboardInterrupt):
                print(f'ERROR: Could not transform {input_file}: {e}')

            try:
                os.unlink(temp_file)
                os.unlink(output_file)
            except Exception:
                pass


def transform_usage_metrics(input_file, output_file, select_fields=None, exclude_fields=None):
    def _clean_keys(data, parent_key=None, select_fields=None, exclude_fields=None):
        """
        Replace invalid characters (based on BigQuery) in keys with underscore and remove unnecessary keys ("another")

        Example input record:
            {"value":"",
             "@timestamp":"",
             "id":"",
             "source":",
             "@version":"",
             "metric":{
                "request":"","user":"",
                 "physicalstatefulcluster.core.confluent.cloud/version":"",
                 "statefulset.kubernetes.io/pod-name":"","type":"",
                 "_deltaSeconds":"",
                 "job":"",
                 "pod-name":"",
                 "physicalstatefulcluster.core.confluent.cloud/name":"",
                 "source":"",
                 "tenant":"",
                 "clusterId":"",
                 "_metricname":"",
                 "instance":"",
                 "pscVersion":""},
             "timestamp":1234567}
        """

        clean_data = {}

        for key, value in data.items():
            full_key = f'{parent_key}.{key}' if parent_key else key
            key = INVALID_KEY_CHARS_RE.sub('_', key)

            if (select_fields and full_key not in select_fields
                    or exclude_fields and full_key in exclude_fields):
                continue

            if type(value) == dict:
                value = _clean_keys(value, parent_key=full_key, select_fields=select_fields,
                                    exclude_fields=exclude_fields)

            clean_data[key] = value

        return clean_data

    with gzip.open(output_file, 'wt') as fp:
        for line in gzip.open(input_file, 'rt'):
            data = json.loads(line)
            new_data = _clean_keys(data, select_fields=select_fields, exclude_fields=exclude_fields)
            fp.write(json.dumps(new_data) + '\n')
