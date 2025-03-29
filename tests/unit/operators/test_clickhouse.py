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
from unittest import mock

from airflow.providers.clickhouse.operators.clickhouse import \
    ClickHouseOperator


class ClickHouseOperatorTestCase(unittest.TestCase):
    def test_arguments(self):
        return_value = ClickHouseOperator(
            task_id='test1',  # required by Airflow
            sql='SELECT 1',
            parameters=[('test-param', 1)],
            with_column_types=True,
            external_tables=[{'name': 'ext'}],
            query_id='test-query-id',
            settings={'test-setting': 1},
            types_check=True,
            columnar=True,
            clickhouse_conn_id='test-conn-id',
            database='test-database',
        ).execute(context={})
        with self.subTest('ClickHouseHook.__init__'):
            self._hook_cls_mock.assert_called_once_with(
                clickhouse_conn_id='test-conn-id',
                database='test-database',
            )
        with self.subTest('ClickHouseHook.execute'):
            self._hook_cls_mock.return_value.execute.assert_called_once_with(
                'SELECT 1',
                [('test-param', 1)],
                True,
                [{'name': 'ext'}],
                'test-query-id',
                {'test-setting': 1},
                True,
                True,
            )
        with self.subTest('return value'):
            self.assertIs(
                return_value,
                self._hook_cls_mock.return_value.execute.return_value,
            )

    def test_defaults(self):
        ClickHouseOperator(
            task_id='test2',  # required by Airflow
            sql='SELECT 2',
        ).execute(context={})
        with self.subTest('ClickHouseHook.__init__'):
            self._hook_cls_mock.assert_called_once_with(
                clickhouse_conn_id='clickhouse_default',
                database=None,
            )
        with self.subTest('ClickHouseHook.execute'):
            self._hook_cls_mock.return_value.execute.assert_called_once_with(
                'SELECT 2',
                None,
                False,
                None,
                None,
                None,
                False,
                False,
            )

    def setUp(self):
        self._hook_cls_patcher = mock.patch('.'.join((
            'airflow.providers.clickhouse.operators',
            'clickhouse.ClickHouseHook',
        )))
        self._hook_cls_mock = self._hook_cls_patcher.start()

    def tearDown(self):
        self._hook_cls_patcher.stop()


class ClickHouseOperatorClassTestCase(unittest.TestCase):
    def test_template_fields(self):
        self.assertSetEqual(
            {
                '_sql',
                '_parameters',
                '_external_tables',
                '_query_id',
                '_settings',
                '_database',
            },
            frozenset(ClickHouseOperator.template_fields),
        )


if __name__ == '__main__':
    unittest.main()
