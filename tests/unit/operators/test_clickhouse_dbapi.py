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

from airflow.providers.clickhouse.operators.clickhouse_dbapi import \
    ClickHouseBaseDbApiOperator


class ClickHouseBaseDbApiOperatorTestCase(unittest.TestCase):
    def test_arguments(self):
        return_value = ClickHouseBaseDbApiOperator(
            task_id='test1',  # required by Airflow
            conn_id='test-conn-id',
            database='test-database',
            hook_params={'test_param': 'test-param-value'},
        ).get_db_hook()
        self._hook_cls_mock.assert_called_once_with(
            clickhouse_conn_id ='test-conn-id',
            schema='test-database',
            test_param='test-param-value',
        )
        self.assertIs(return_value, self._hook_cls_mock.return_value)

    def test_defaults(self):
        ClickHouseBaseDbApiOperator(
            task_id='test2',  # required by Airflow
        ).get_db_hook()
        self._hook_cls_mock.assert_called_once_with(schema=None)

    def setUp(self):
        self._hook_cls_patcher = mock.patch('.'.join((
            'airflow.providers.clickhouse.operators',
            'clickhouse_dbapi.ClickHouseDbApiHook',
        )))
        self._hook_cls_mock = self._hook_cls_patcher.start()

    def tearDown(self):
        self._hook_cls_patcher.stop()


if __name__ == '__main__':
    unittest.main()
