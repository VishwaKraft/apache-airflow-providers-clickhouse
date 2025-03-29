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
import clickhouse_driver  # type: ignore
from airflow.providers.common.sql.hooks.sql import DbApiHook

from airflow.providers.clickhouse.hooks.clickhouse import conn_to_kwargs, \
    default_conn_name
from typing import Optional

# Ignore type checking for clickhouse_driver
from clickhouse_driver import Client  # type: ignore


class ClickHouseDbApiHook(DbApiHook):
    conn_name_attr = 'clickhouse_conn_id'
    clickhouse_conn_id: str  # set by DbApiHook.__init__
    default_conn_name = default_conn_name

    def __init__(self, *args, schema: Optional[str] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._schema = schema

    def get_conn(self) -> clickhouse_driver.dbapi.Connection:
        airflow_conn = self.get_connection(self.clickhouse_conn_id)
        return clickhouse_driver.dbapi \
            .connect(**conn_to_kwargs(airflow_conn, self._schema))
