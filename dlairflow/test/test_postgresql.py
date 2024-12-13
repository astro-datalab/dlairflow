# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.postgresql.
"""
import os
import pytest
from ..postgresql import pg_dump_schema, pg_restore_schema
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator


class MockConnection(object):

    def __init__(self, connection):
        foo = connection.split(',')
        self.login = foo[0]
        self.password = foo[1]
        self.host = foo[2]
        self.schema = foo[3]
        return


@pytest.mark.parametrize('task_function,dump_dir', [(pg_dump_schema, None), (pg_dump_schema, 'dump_dir'),
                                                    (pg_restore_schema, None), (pg_restore_schema, 'dump_dir')])
def test_pg_dump_schema(monkeypatch, task_function, dump_dir):
    """Test pg_dump task with alternate directory.
    """
    def mock_connection(connection):
        conn = MockConnection(connection)
        return conn

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    test_operator = task_function("login,password,host,schema", "dump_schema", dump_dir)

    assert isinstance(test_operator, BashOperator)
    assert test_operator.env['PGHOST'] == 'host'
    assert test_operator.params['schema'] == 'dump_schema'
    if dump_dir is None:
        assert test_operator.params['dump_dir'] == '/data0/datalab/' + os.environ['USER']
    else:
        assert test_operator.params['dump_dir'] == 'dump_dir'
