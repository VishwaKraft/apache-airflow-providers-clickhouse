#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import unittest

from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook


class ClickHouseHookTestCase(unittest.TestCase):
    def test_execute(self):
        return_value = ClickHouseHook().execute(
            'SELECT sum(value) * %(multiplier)s AS output FROM ext',
            params={'multiplier': 2},
            with_column_types=True,
            external_tables=[{
                'name': 'ext',
                'structure': [('value', 'Int32')],
                'data': [{'value': 1}, {'value': 2}],
            }],
            query_id='airflow-clickhouse-plugin-test',
            types_check=True,
            columnar=True,
        )
        self.assertTupleEqual(([(6,)], [('output', 'Int64')]), return_value)


if __name__ == '__main__':
    unittest.main()
