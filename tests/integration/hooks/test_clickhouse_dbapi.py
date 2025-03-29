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

from airflow.providers.clickhouse.hooks.clickhouse_dbapi import ClickHouseDbApiHook


class ClickHouseDbApiHookTestCase(unittest.TestCase):
    def test_get_records(self):
        records = ClickHouseDbApiHook().get_records(
            '''
                SELECT number * %(multiplier)s AS output
                FROM system.numbers
                LIMIT 1 OFFSET 1
            ''',
            parameters={'multiplier': 2},
        )
        self.assertListEqual([(2,)], records)


if __name__ == '__main__':
    unittest.main()
