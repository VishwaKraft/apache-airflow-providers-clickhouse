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
from typing import TYPE_CHECKING, Optional, Iterable, Sequence, Union, List, Dict, Tuple, Any

from airflow.models import BaseOperator

from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook, \
    ExecuteParamsT, ExecuteReturnT, default_conn_name, ExternalTable

from airflow.utils.context import Context

class BaseClickHouseOperator(BaseOperator):
    """
    A superclass for operator classes. Defines __init__ with common arguments.

    Includes arguments of clickhouse_driver.Client.execute.
    """

    template_fields = (  # all str-containing arguments
        '_sql',
        '_parameters',
        '_external_tables',
        '_query_id',
        '_settings',
        '_database',
    )
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {
        '_sql': 'sql',
        '_parameters': 'json',
        '_external_tables': 'json',
        '_settings': 'json',
    }

    def __init__(
            self,
            *args,
            sql: Union[str, Iterable[str]],
            # arguments of clickhouse_driver.Client.execute
            parameters: Optional[ExecuteParamsT] = None,
            with_column_types: bool = False,
            external_tables: Optional[List[ExternalTable]] = None,
            query_id: Optional[str] = None,
            settings: Optional[Dict[str, Any]] = None, 
            types_check: bool = False,
            columnar: bool = False,
            # arguments of ClickHouseHook.__init__
            clickhouse_conn_id: str = default_conn_name,
            database: Optional[str] = None,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self._sql = sql

        self._parameters = parameters
        self._with_column_types = with_column_types
        self._external_tables = external_tables
        self._query_id = query_id
        self._settings = settings
        self._types_check = types_check
        self._columnar = columnar

        self._clickhouse_conn_id = clickhouse_conn_id
        self._database = database

    def _hook_execute(self) -> Union[
        int,
        List[Tuple[Any, ...]],
        Tuple[List[Tuple[Any, ...]], List[Tuple[str, str]]],
    ]:
        hook = ClickHouseHook(
            clickhouse_conn_id=self._clickhouse_conn_id,
            database=self._database,
        )
        result = hook.execute(
            self._sql,
            self._parameters,
            self._with_column_types,
            self._external_tables,
            self._query_id,
            self._settings,
            self._types_check,
            self._columnar,
        )
        if result is None:
            raise ValueError("ClickHouseHook.execute returned None, which is not allowed.")
        return result


class ClickHouseOperator(BaseClickHouseOperator, BaseOperator):
    def execute(self, context: Context) -> Union[
        int,
        List[Tuple[Any, ...]],
        Tuple[List[Tuple[Any, ...]], List[Tuple[str, str]]],
    ]:
        return self._hook_execute()
