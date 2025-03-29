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
import typing as t
from typing import Optional, Union  # Add Optional and Union imports

from airflow.providers.common.sql.operators import sql
from airflow.providers.common.sql.operators.sql import BaseSQLOperator

from airflow.providers.clickhouse.hooks.clickhouse_dbapi import \
    ClickHouseDbApiHook


class ClickHouseDbApiHookMixin(object):
    # these attributes are defined in both BaseSQLOperator and SqlSensor
    conn_id: Optional[str] = None  # Use Optional[str] instead of str | None
    hook_params: t.Dict[str, t.Any]  # Match BaseSQLOperator's type

    def _get_clickhouse_db_api_hook(self, **extra_hook_params) -> ClickHouseDbApiHook:
        hook_kwargs = {}
        if self.conn_id is not None:
            hook_kwargs['clickhouse_conn_id'] = self.conn_id
        if self.hook_params is not None:
            hook_kwargs.update(self.hook_params)
        hook_kwargs.update(extra_hook_params)
        return ClickHouseDbApiHook(**hook_kwargs)


class ClickHouseBaseDbApiOperator(ClickHouseDbApiHookMixin, sql.BaseSQLOperator):
    def get_db_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook(schema=self.database)


class ClickHouseSQLExecuteQueryOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLExecuteQueryOperator,
):
    pass


class ClickHouseSQLColumnCheckOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLColumnCheckOperator,
):
    pass


class ClickHouseSQLTableCheckOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLTableCheckOperator,
):
    pass


class ClickHouseSQLCheckOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLCheckOperator,
):
    pass


class ClickHouseSQLValueCheckOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLValueCheckOperator,
):
    pass


class ClickHouseSQLIntervalCheckOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLIntervalCheckOperator,
):
    pass


class ClickHouseSQLThresholdCheckOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLThresholdCheckOperator,
):
    pass


class ClickHouseBranchSQLOperator(
    ClickHouseBaseDbApiOperator,
    sql.BranchSQLOperator,
):
    pass
