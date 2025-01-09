# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.postgresql.
"""
import os
import pytest
from importlib import import_module


class MockConnection(object):

    def __init__(self, connection):
        foo = connection.split(',')
        self.login = foo[0]
        self.password = foo[1]
        self.host = foo[2]
        self.schema = foo[3]
        return


@pytest.fixture(scope="module")
def temporary_airflow_home(tmp_path_factory):
    """Avoid creating ``${HOME}/airflow`` during tests.
    """
    os.environ['AIRFLOW__CORE__UNIT_TEST_MODE'] = 'True'
    airflow_home = tmp_path_factory.mktemp("airflow_home")
    os.environ['AIRFLOW_HOME'] = str(airflow_home)
    yield airflow_home
    #
    # Clean up as module exists.
    #
    del os.environ['AIRFLOW__CORE__UNIT_TEST_MODE']
    del os.environ['AIRFLOW_HOME']


@pytest.mark.parametrize('task_function,dump_dir', [('pg_dump_schema', None),
                                                    ('pg_dump_schema', 'dump_dir'),
                                                    ('pg_restore_schema', None),
                                                    ('pg_restore_schema', 'dump_dir')])
def test_pg_dump_schema(monkeypatch, temporary_airflow_home, task_function, dump_dir):
    """Test pg_dump tasks in various combinations.
    """
    def mock_connection(connection):
        conn = MockConnection(connection)
        return conn

    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    from airflow.hooks.base import BaseHook
    from airflow.operators.bash import BashOperator

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    p = import_module('..postgresql', package='dlairflow.test')

    tf = p.__dict__[task_function]
    test_operator = tf("login,password,host,schema", "dump_schema", dump_dir)

    assert isinstance(test_operator, BashOperator)
    assert test_operator.env['PGHOST'] == 'host'
    assert test_operator.params['schema'] == 'dump_schema'
    if dump_dir is None:
        assert test_operator.params['dump_dir'] == '/data0/datalab/' + os.environ['USER']
    else:
        assert test_operator.params['dump_dir'] == 'dump_dir'
