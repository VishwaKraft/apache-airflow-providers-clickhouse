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
import contextlib
import typing as t
from itertools import islice
from typing import Optional, Union, List, Tuple, Dict, Generator, Any, TypeVar, ContextManager, Callable, Iterator

import clickhouse_driver  # type: ignore  # Suppress missing type stub warning
from airflow.hooks.base import BaseHook
from airflow.models import Connection

# Replace NewType with TypeAlias for better compatibility
_ParamT = Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]
ExecuteParamsT = Union[
    List[_ParamT], Tuple[_ParamT, ...], Generator[_ParamT, None, None], Dict[Any, Any]
]
ExecuteReturnT = Union[
    int,  # number of inserted rows
    List[Tuple[Any, ...]],  # list of tuples with rows/columns
    Tuple[List[Tuple[Any, ...]], List[Tuple[str, str]]],  # with_column_types
]


class ExternalTable(t.TypedDict):
    name: str
    structure: List[Tuple[str, str]]
    data: List[Dict[str, Any]]


default_conn_name = 'clickhouse_default'


class ClickHouseHook(BaseHook):
    def __init__(
            self,
            *args,
            clickhouse_conn_id: str = default_conn_name,
            database: Optional[str] = None,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._clickhouse_conn_id = clickhouse_conn_id
        self._database = database

    def get_conn(self) -> clickhouse_driver.Client:
        conn = self.get_connection(self._clickhouse_conn_id)
        return clickhouse_driver.Client(**conn_to_kwargs(conn, self._database))

    def execute(
            self,
            sql: Union[str, t.Iterable[str]],
            params: Optional[ExecuteParamsT] = None,
            with_column_types: bool = False,
            external_tables: Optional[List[ExternalTable]] = None,
            query_id: Optional[str] = None,
            settings: Optional[Dict[str, Any]] = None,
            types_check: bool = False,
            columnar: bool = False,
    ) -> Optional[ExecuteReturnT]:  # Allow Optional return type
        """
        Passes arguments to ``clickhouse_driver.Client.execute``.

        Allows execution of multiple queries, if ``sql`` argument is an
        iterable. Returns results of the last query's execution.
        """
        if isinstance(sql, str):
            sql = (sql,)
        with _disconnecting(self.get_conn()) as conn:
            last_result = None
            for query in sql:
                self.log.info(_format_query_log(query, params or {}))  # Ensure params is not None
                last_result = conn.execute(
                    query,
                    params=params,
                    with_column_types=with_column_types,
                    external_tables=external_tables,
                    query_id=query_id,
                    settings=settings,
                    types_check=types_check,
                    columnar=columnar,
                )
        return last_result  # Ensure return type matches Optional[ExecuteReturnT]


def conn_to_kwargs(conn: Connection, database: Optional[str]) -> Dict[str, Any]:
    """ Translate Airflow Connection to clickhouse-driver Connection kwargs. """
    connection_kwargs = conn.extra_dejson.copy()
    # Connection attributes can be parsed to empty strings by urllib.unparse
    connection_kwargs['host'] = conn.host or 'localhost'
    if conn.port:
        connection_kwargs.update(port=conn.port)
    if conn.login:
        connection_kwargs.update(user=conn.login)
    if conn.password:
        connection_kwargs.update(password=conn.password)
    if database is not None:
        connection_kwargs.update(database=database)
    elif conn.schema:
        connection_kwargs.update(database=conn.schema)
    return connection_kwargs


def _format_query_log(query: str, params: ExecuteParamsT) -> str:
    return ''.join((query, f' with {_format_params(params)}' if params else ''))


def _format_params(params: ExecuteParamsT, limit: int = 10) -> str:
    if isinstance(params, t.Generator) or len(params) <= limit:
        return str(params)
    if isinstance(params, dict):
        head = dict(islice(params.items(), limit))
    else:
        head = params[:limit]
    head_str = str(head)
    closing_paren = head_str[-1]
    return f'{head_str[:-1]} … and {len(params) - limit} ' \
        f'more parameters{closing_paren}'


_DisconnectingT = TypeVar('_DisconnectingT', bound=Any)  # Ensure bound is Any

@contextlib.contextmanager
def _disconnecting(thing: _DisconnectingT) -> Iterator[_DisconnectingT]:  # Fix return type
    """
    Context to automatically disconnect something at the end of a block.

    Similar to ``contextlib.closing`` but calls .disconnect() method on exit.

    Code like this:

    >>> with _disconnecting(<module>.open(<arguments>)) as f:
    >>>     <block>

    is equivalent to this:

    >>> f = <module>.open(<arguments>)
    >>> try:
    >>>     <block>
    >>> finally:
    >>>     f.disconnect()
    """
    try:
        yield thing
    finally:
        thing.disconnect()  # Ensure disconnect exists on _DisconnectingT
