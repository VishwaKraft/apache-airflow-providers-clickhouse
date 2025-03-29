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
from typing import TYPE_CHECKING, Optional, Callable

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator

from airflow.providers.clickhouse.hooks.clickhouse import ExecuteReturnT
from airflow.providers.clickhouse.operators.clickhouse import \
    BaseClickHouseOperator

from airflow.utils.context import Context

class ClickHouseSensor(BaseClickHouseOperator, BaseSensorOperator):
    """ Pokes using clickhouse_driver.Client.execute. """

    def __init__(
            self,
            *args,
            is_failure: Optional[Callable[[ExecuteReturnT], bool]] = None,
            is_success: Optional[Callable[[ExecuteReturnT], bool]] = None,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._is_failure = is_failure
        self._is_success = is_success if is_success is not None else bool

    def poke(self, context: Context) -> bool:
        result = self._hook_execute()
        if self._is_failure is not None:
            is_failure = self._is_failure(result)
            if is_failure:
                raise AirflowException(f'is_failure returned {is_failure}')
        return self._is_success(result)
