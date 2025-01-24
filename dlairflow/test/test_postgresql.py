# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.postgresql.
"""
import os
import pytest
from importlib import import_module
from jinja2 import Environment, FileSystemLoader


class MockConnection(object):
    """Convert a string into an object with attributes.
    """
    def __init__(self, connection):
        foo = connection.split(',')
        self.login = foo[0]
        self.password = foo[1]
        self.host = foo[2]
        self.schema = foo[3]
        return


def mock_connection(connection):
    """Used to monkeypatch get_connection() methods.
    """
    conn = MockConnection(connection)
    return conn


@pytest.fixture(scope="function")
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
    """Test pg_dump and pg_restore tasks in various combinations.
    """
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


def test_q3c_index(monkeypatch, temporary_airflow_home):
    """Test the q3c_index function.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    from airflow.hooks.base import BaseHook
    from airflow.providers.postgres.operators.postgres import PostgresOperator

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    p = import_module('..postgresql', package='dlairflow.test')

    tf = p.__dict__['q3c_index']
    test_operator = tf("login,password,host,schema", 'q3c_schema', 'q3c_table')
    assert isinstance(test_operator, PostgresOperator)
    assert os.path.exists(str(temporary_airflow_home / 'dags' / 'sql' / 'dlairflow.postgresql.q3c_index.sql'))
    assert test_operator.task_id == 'q3c_index'
    assert test_operator.sql == 'sql/dlairflow.postgresql.q3c_index.sql'
    env = Environment(loader=FileSystemLoader(searchpath=str(temporary_airflow_home / 'dags')),
                      keep_trailing_newline=True)
    tmpl = env.get_template(test_operator.sql)
    expected_render = """--
-- Created by dlairflow.postgresql.q3c_index().
--
CREATE INDEX q3c_table_q3c_ang2ipix
    ON q3c_schema.q3c_table (q3c_ang2ipix("ra", "dec"))
    WITH (fillfactor=100);
CLUSTER q3c_table_q3c_ang2ipix ON q3c_schema.q3c_table;
"""
    assert tmpl.render(params=test_operator.params) == expected_render
