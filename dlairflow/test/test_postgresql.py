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


@pytest.mark.parametrize('task_function,dump_dir', [('pg_dump_schema', 'dump_dir'),
                                                    ('pg_restore_schema', 'dump_dir')])
def test_pg_dump_schema(monkeypatch, temporary_airflow_home, task_function, dump_dir):
    """Test pg_dump and pg_restore tasks in various combinations.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    from airflow.hooks.base import BaseHook
    try:
        from airflow.providers.standard.operators.bash import BashOperator
    except ImportError:
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


@pytest.mark.parametrize('overwrite', [(False, ), (True, )])
def test_q3c_index(monkeypatch, temporary_airflow_home, overwrite):
    """Test the q3c_index function.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    from airflow.hooks.base import BaseHook
    try:
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
    except ImportError:
        from airflow.providers.postgres.operators.postgres import PostgresOperator

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    p = import_module('..postgresql', package='dlairflow.test')

    tf = p.__dict__['q3c_index']
    test_operator = tf("login,password,host,schema", 'q3c_schema', 'q3c_table',
                       overwrite=overwrite)
    assert isinstance(test_operator, PostgresOperator)
    assert os.path.exists(str(temporary_airflow_home / 'dags' / 'sql' / 'dlairflow.postgresql.q3c_index.sql'))
    assert test_operator.task_id == 'q3c_index'
    assert test_operator.sql == 'sql/dlairflow.postgresql.q3c_index.sql'
    env = Environment(loader=FileSystemLoader(searchpath=str(temporary_airflow_home / 'dags')),
                      keep_trailing_newline=True)
    tmpl = env.get_template(test_operator.sql)
    expected_render = """--
-- Created by dlairflow.postgresql.q3c_index().
-- Call q3c_index(..., overwrite=True) to replace this file.
--
CREATE INDEX q3c_table_q3c_ang2ipix
    ON q3c_schema.q3c_table (q3c_ang2ipix("ra", "dec"))
    WITH (fillfactor=100);
CLUSTER q3c_table_q3c_ang2ipix ON q3c_schema.q3c_table;
"""
    assert tmpl.render(params=test_operator.params) == expected_render


@pytest.mark.parametrize('overwrite', [(False, ), (True, )])
def test_index_columns(monkeypatch, temporary_airflow_home, overwrite):
    """Test the index_columns function.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    from airflow.hooks.base import BaseHook
    try:
        from airflow.providers.postgres.operators.postgres import PostgresOperator
    except ImportError:
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    p = import_module('..postgresql', package='dlairflow.test')

    tf = p.__dict__['index_columns']
    test_operator = tf("login,password,host,schema", 'test_schema', 'test_table',
                       columns=['ra', 'dec',
                                ('id', 'survey', 'program'),
                                12345,
                                {'test_schema.uint64': 'specobjid'}],
                       overwrite=overwrite)
    assert isinstance(test_operator, PostgresOperator)
    assert os.path.exists(str(temporary_airflow_home / 'dags' / 'sql' /
                              'dlairflow.postgresql.index_columns.sql'))
    assert test_operator.task_id == 'index_columns'
    assert test_operator.sql == 'sql/dlairflow.postgresql.index_columns.sql'
    env = Environment(loader=FileSystemLoader(searchpath=str(temporary_airflow_home / 'dags')),
                      keep_trailing_newline=True)
    tmpl = env.get_template(test_operator.sql)
    expected_render = """--
-- Created by dlairflow.postgresql.index_columns().
-- Call index_columns(..., overwrite=True) to replace this file.
--

CREATE INDEX test_table_ra_idx
    ON test_schema.test_table ("ra")
    WITH (fillfactor=100);

CREATE INDEX test_table_dec_idx
    ON test_schema.test_table ("dec")
    WITH (fillfactor=100);

CREATE INDEX test_table_id_survey_program_idx
    ON test_schema.test_table ("id", "survey", "program")
    WITH (fillfactor=100);

-- Unknown type: 12345.

CREATE_INDEX test_table_test_schema_uint64_specobjid_idx
    ON test_schema.test_table (test_schema.uint64(specobjid))
    WITH (fillfactor=100);


"""
    assert tmpl.render(params=test_operator.params) == expected_render


@pytest.mark.parametrize('overwrite', [(False, ), (True, )])
def test_primary_key(monkeypatch, temporary_airflow_home, overwrite):
    """Test the primary_key function.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    from airflow.hooks.base import BaseHook
    try:
        from airflow.providers.postgres.operators.postgres import PostgresOperator
    except ImportError:
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    p = import_module('..postgresql', package='dlairflow.test')

    tf = p.__dict__['primary_key']
    test_operator = tf("login,password,host,schema", 'test_schema',
                       {"table1": "column1",
                        "table2": ("column1", "column2"),
                        "table3": 12345},
                       overwrite=overwrite)
    assert isinstance(test_operator, PostgresOperator)
    assert os.path.exists(str(temporary_airflow_home / 'dags' / 'sql' /
                              'dlairflow.postgresql.primary_key.sql'))
    assert test_operator.task_id == 'primary_key'
    assert test_operator.sql == 'sql/dlairflow.postgresql.primary_key.sql'
    env = Environment(loader=FileSystemLoader(searchpath=str(temporary_airflow_home / 'dags')),
                      keep_trailing_newline=True)
    tmpl = env.get_template(test_operator.sql)
    expected_render = """--
-- Created by dlairflow.postgresql.primary_key().
-- Call primary_key(..., overwrite=True) to replace this file.
--

ALTER TABLE test_schema.table1 ADD PRIMARY KEY ("column1");

ALTER TABLE test_schema.table2 ADD PRIMARY KEY ("column1", "column2");

-- Unknown type: 12345.

"""
    assert tmpl.render(params=test_operator.params) == expected_render


@pytest.mark.parametrize('tables,full,overwrite', [('table1', False, False),
                                                   (['table1', 'table2'], True, True)])
def test_vacuum_analyze(monkeypatch, temporary_airflow_home, tables, full, overwrite):
    """Test the vacuum_analyze function.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    from airflow.hooks.base import BaseHook
    try:
        from airflow.providers.postgres.operators.postgres import PostgresOperator
    except ImportError:
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    p = import_module('..postgresql', package='dlairflow.test')

    tf = p.__dict__['vacuum_analyze']
    test_operator = tf("login,password,host,schema", 'test_schema', tables,
                       full=full, overwrite=overwrite)
    assert isinstance(test_operator, PostgresOperator)
    assert os.path.exists(str(temporary_airflow_home / 'dags' / 'sql' /
                              'dlairflow.postgresql.vacuum_analyze.sql'))
    assert test_operator.task_id == 'vacuum_analyze'
    assert test_operator.sql == 'sql/dlairflow.postgresql.vacuum_analyze.sql'
    env = Environment(loader=FileSystemLoader(searchpath=str(temporary_airflow_home / 'dags')),
                      keep_trailing_newline=True)
    tmpl = env.get_template(test_operator.sql)
    expected_render = """--
-- Created by dlairflow.postgresql.vacuum_analyze().
-- Call vacuum_analyze(..., overwrite=True) to replace this file.
--

VACUUM {0} VERBOSE ANALYZE test_schema.table1;

""".format('FULL' if full else '')
    if full:
        expected_render += "VACUUM FULL VERBOSE ANALYZE test_schema.table2;\n\n"
    assert tmpl.render(params=test_operator.params) == expected_render
