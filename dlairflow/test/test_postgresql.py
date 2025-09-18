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


def test__PostgresOperatorWrapper(monkeypatch):
    """Test translation of PostgresOperator keyword arguments.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    from airflow.hooks.base import BaseHook

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    p = import_module('..postgresql', package='dlairflow.test')

    def return_kwargs(**kwargs):
        return kwargs

    monkeypatch.setattr(p, '_legacy_postgres', True)
    monkeypatch.setattr(p, 'PostgresOperator', return_kwargs)

    kw = p._PostgresOperatorWrapper(conn_id='foo')
    assert 'postgres_conn_id' in kw
    assert kw['postgres_conn_id'] == 'foo'


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


@pytest.mark.parametrize('overwrite,tablespace', [(False, None), (True, None),
                                                  (False, 'data3'), (True, 'data3')])
def test_q3c_index(monkeypatch, temporary_airflow_home, overwrite, tablespace):
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
    function_name = 'q3c_index'
    tf = p.__dict__[function_name]
    test_operator = tf("login,password,host,schema", 'q3c_schema', 'q3c_table',
                       tablespace=tablespace, overwrite=overwrite)
    assert isinstance(test_operator, PostgresOperator)
    assert os.path.exists(str(temporary_airflow_home / 'dags' / 'sql' /
                              f'dlairflow.postgresql.{function_name}.sql'))
    assert test_operator.task_id == function_name
    assert test_operator.sql == f'sql/dlairflow.postgresql.{function_name}.sql'
    env = Environment(loader=FileSystemLoader(searchpath=str(temporary_airflow_home / 'dags')),
                      keep_trailing_newline=True)
    tmpl = env.get_template(test_operator.sql)
    if tablespace:
        expected_render = f"""--
-- Created by dlairflow.postgresql.{function_name}().
-- Call {function_name}(..., overwrite=True) to replace this file.
--
CREATE INDEX q3c_table_q3c_ang2ipix
    ON q3c_schema.q3c_table (q3c_ang2ipix("ra", "dec"))
    WITH (fillfactor=100) TABLESPACE {tablespace};
CLUSTER q3c_table_q3c_ang2ipix ON q3c_schema.q3c_table;
"""
    else:
        expected_render = f"""--
-- Created by dlairflow.postgresql.{function_name}().
-- Call {function_name}(..., overwrite=True) to replace this file.
--
CREATE INDEX q3c_table_q3c_ang2ipix
    ON q3c_schema.q3c_table (q3c_ang2ipix("ra", "dec"))
    WITH (fillfactor=100);
CLUSTER q3c_table_q3c_ang2ipix ON q3c_schema.q3c_table;
"""
    assert tmpl.render(params=test_operator.params) == expected_render


@pytest.mark.parametrize('overwrite,tablespace', [(False, None), (True, None),
                                                  (False, 'data3'), (True, 'data3')])
def test_index_columns(monkeypatch, temporary_airflow_home, overwrite, tablespace):
    """Test the index_columns function.
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
    function_name = 'index_columns'
    tf = p.__dict__[function_name]
    test_operator = tf("login,password,host,schema", 'test_schema', 'test_table',
                       columns=['ra', 'dec',
                                ('id', 'survey', 'program'),
                                12345,
                                {'test_schema.uint64': 'specobjid'}],
                       tablespace=tablespace, overwrite=overwrite)
    assert isinstance(test_operator, PostgresOperator)
    assert os.path.exists(str(temporary_airflow_home / 'dags' / 'sql' /
                              f'dlairflow.postgresql.{function_name}.sql'))
    assert test_operator.task_id == function_name
    assert test_operator.sql == f'sql/dlairflow.postgresql.{function_name}.sql'
    env = Environment(loader=FileSystemLoader(searchpath=str(temporary_airflow_home / 'dags')),
                      keep_trailing_newline=True)
    tmpl = env.get_template(test_operator.sql)
    if tablespace:
        expected_render = f"""--
-- Created by dlairflow.postgresql.{function_name}().
-- Call {function_name}(..., overwrite=True) to replace this file.
--

CREATE INDEX test_table_ra_idx
    ON test_schema.test_table ("ra")
    WITH (fillfactor=100) TABLESPACE {tablespace};

CREATE INDEX test_table_dec_idx
    ON test_schema.test_table ("dec")
    WITH (fillfactor=100) TABLESPACE {tablespace};

CREATE INDEX test_table_id_survey_program_idx
    ON test_schema.test_table ("id", "survey", "program")
    WITH (fillfactor=100) TABLESPACE {tablespace};

-- Unknown type: 12345.

CREATE_INDEX test_table_test_schema_uint64_specobjid_idx
    ON test_schema.test_table (test_schema.uint64(specobjid))
    WITH (fillfactor=100) TABLESPACE {tablespace};


"""
    else:
        expected_render = f"""--
-- Created by dlairflow.postgresql.{function_name}().
-- Call {function_name}(..., overwrite=True) to replace this file.
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


@pytest.mark.parametrize('overwrite,tablespace', [(False, None), (True, None),
                                                  (False, 'data3'), (True, 'data3')])
def test_primary_key(monkeypatch, temporary_airflow_home, overwrite, tablespace):
    """Test the primary_key function.
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
    function_name = 'primary_key'
    tf = p.__dict__[function_name]
    test_operator = tf("login,password,host,schema", 'test_schema',
                       {"table1": "column1",
                        "table2": ("column1", "column2"),
                        "table3": 12345},
                       tablespace=tablespace, overwrite=overwrite)
    assert isinstance(test_operator, PostgresOperator)
    assert os.path.exists(str(temporary_airflow_home / 'dags' / 'sql' /
                              f'dlairflow.postgresql.{function_name}.sql'))
    assert test_operator.task_id == function_name
    assert test_operator.sql == f'sql/dlairflow.postgresql.{function_name}.sql'
    env = Environment(loader=FileSystemLoader(searchpath=str(temporary_airflow_home / 'dags')),
                      keep_trailing_newline=True)
    tmpl = env.get_template(test_operator.sql)
    if tablespace:
        expected_render = f"""--
-- Created by dlairflow.postgresql.{function_name}().
-- Call {function_name}(..., overwrite=True) to replace this file.
--

ALTER TABLE test_schema.table1 ADD PRIMARY KEY ("column1")
    WITH (fillfactor=100) USING INDEX TABLESPACE {tablespace};

ALTER TABLE test_schema.table2 ADD PRIMARY KEY ("column1", "column2")
    WITH (fillfactor=100) USING INDEX TABLESPACE {tablespace};

-- Unknown type: 12345.

"""
    else:
        expected_render = f"""--
-- Created by dlairflow.postgresql.{function_name}().
-- Call {function_name}(..., overwrite=True) to replace this file.
--

ALTER TABLE test_schema.table1 ADD PRIMARY KEY ("column1")
    WITH (fillfactor=100);

ALTER TABLE test_schema.table2 ADD PRIMARY KEY ("column1", "column2")
    WITH (fillfactor=100);

-- Unknown type: 12345.

"""
    assert tmpl.render(params=test_operator.params) == expected_render


@pytest.mark.parametrize('tables,restart,cascade,overwrite', [('table1', False, False, True),
                                                              (['table1', 'table2'], True, False, False),
                                                              (['table1', 'table2'], False, True, False),
                                                              (['table1', 'table2'], True, True, False),
                                                              (False, False, False, False)])
def test_truncate_table(monkeypatch, temporary_airflow_home, tables, restart, cascade, overwrite):
    """Test the truncate_table function.
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
    function_name = 'truncate_table'
    tf = p.__dict__[function_name]
    if tables:
        test_operator = tf("login,password,host,schema", 'test_schema', tables,
                           restart=restart, cascade=cascade, overwrite=overwrite)
        assert isinstance(test_operator, PostgresOperator)
        assert os.path.exists(str(temporary_airflow_home / 'dags' / 'sql' /
                                  f'dlairflow.postgresql.{function_name}.sql'))
        assert test_operator.task_id == function_name
        assert test_operator.sql == f'sql/dlairflow.postgresql.{function_name}.sql'
        env = Environment(loader=FileSystemLoader(searchpath=str(temporary_airflow_home / 'dags')),
                          keep_trailing_newline=True)
        tmpl = env.get_template(test_operator.sql)
        if isinstance(tables, list):
            st = ', '.join(['test_schema.' + t for t in tables])
        else:
            st = 'test_schema.' + tables
        expected_render = """--
-- Created by dlairflow.postgresql.{0}().
-- Call {0}(..., overwrite=True) to replace this file.
--
TRUNCATE TABLE {1}
    {2} IDENTITY
    {3};
""".format(function_name, st,
           'RESTART' if restart else 'CONTINUE',
           'CASCADE' if cascade else 'RESTRICT')
        assert tmpl.render(params=test_operator.params) == expected_render
    else:
        with pytest.raises(ValueError) as excinfo:
            test_operator = tf("login,password,host,schema", 'test_schema', tables,
                               restart=restart, cascade=cascade, overwrite=overwrite)
        assert excinfo.value.args[0] == "Unknown type for table, must be string or list-like!"


@pytest.mark.parametrize('tables,full,overwrite', [('table1', False, False),
                                                   (['table1', 'table2'], True, True),
                                                   (False, False, False)])
def test_vacuum_analyze(monkeypatch, temporary_airflow_home, tables, full, overwrite):
    """Test the vacuum_analyze function.
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
    function_name = 'vacuum_analyze'
    tf = p.__dict__[function_name]
    if tables:
        test_operator = tf("login,password,host,schema", 'test_schema', tables,
                           full=full, overwrite=overwrite)
        assert isinstance(test_operator, PostgresOperator)
        assert os.path.exists(str(temporary_airflow_home / 'dags' / 'sql' /
                                  f'dlairflow.postgresql.{function_name}.sql'))
        assert test_operator.task_id == function_name
        assert test_operator.sql == f'sql/dlairflow.postgresql.{function_name}.sql'
        env = Environment(loader=FileSystemLoader(searchpath=str(temporary_airflow_home / 'dags')),
                          keep_trailing_newline=True)
        tmpl = env.get_template(test_operator.sql)
        expected_render = """--
-- Created by dlairflow.postgresql.{0}().
-- Call {0}(..., overwrite=True) to replace this file.
--

VACUUM {1} VERBOSE ANALYZE test_schema.table1;

""".format(function_name, 'FULL' if full else '')
        if full:
            expected_render += "VACUUM FULL VERBOSE ANALYZE test_schema.table2;\n\n"
        assert tmpl.render(params=test_operator.params) == expected_render
    else:
        with pytest.raises(ValueError) as excinfo:
            test_operator = tf("login,password,host,schema", 'test_schema', tables,
                               full=full, overwrite=overwrite)
        assert excinfo.value.args[0] == "Unknown type for table, must be string or list-like!"
