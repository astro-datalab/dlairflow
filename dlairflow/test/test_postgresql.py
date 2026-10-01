# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.postgresql.
"""
import os
import pytest
from importlib import import_module


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
def test_pg_dump_schema(temporary_airflow_home, task_function, dump_dir):
    """Test pg_dump and pg_restore tasks in various combinations.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    try:
        from airflow.providers.standard.operators.bash import BashOperator
    except ImportError:
        from airflow.operators.bash import BashOperator

    p = import_module('..postgresql', package='dlairflow.test')

    tf = p.__dict__[task_function]
    test_operator = tf("connection_name", "dump_schema", dump_dir)

    assert isinstance(test_operator, BashOperator)
    assert test_operator.env['PGHOST'] == '{{ conn.get("connection_name").host }}'
    assert test_operator.params['schema'] == 'dump_schema'
    if dump_dir is None:
        assert test_operator.params['dump_dir'] == '/data0/datalab/' + os.environ['USER']
    else:
        assert test_operator.params['dump_dir'] == 'dump_dir'


@pytest.mark.parametrize('schema,table,tablespace', [('q3c_schema', 'q3c_table', None),
                                                     ('params.q3c_schema', 'params.q3c_table', None),
                                                     ('q3c_schema', 'q3c_table', 'data3'),
                                                     ('q3c_schema', 'q3c_table', 'params.index_tablespace')])
def test_q3c_index(temporary_airflow_home, schema, table, tablespace):
    """Test the q3c_index function.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    try:
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
    except ImportError:
        from airflow.providers.postgres.operators.postgres import PostgresOperator

    p = import_module('..postgresql', package='dlairflow.test')
    function_name = 'q3c_index'
    tf = p.__dict__[function_name]
    test_operator = tf("login,password,host,schema", schema, table,
                       tablespace=tablespace)
    assert isinstance(test_operator, PostgresOperator)
    assert test_operator.task_id == function_name
    if tablespace:
        if tablespace.startswith('params.'):
            if_tablespace = (f"{{%- if {tablespace} %}} USING INDEX TABLESPACE " +
                             f"{{{{ {tablespace} }}}}{{%- endif -%}}")
        else:
            if_tablespace = ("{%- if params._q3c_tablespace %} USING INDEX " +
                             "TABLESPACE {{ params._q3c_tablespace }}{%- endif -%}")
    else:
        if_tablespace = ''
    if schema.startswith('params.'):
        schema = f'{{{{ {schema} }}}}'
    if table.startswith('params.'):
        table = f'{{{{ {table} }}}}'
    expected_render = f"""--
-- Created by dlairflow.postgresql.{function_name}().
--
CREATE INDEX {table}_q3c_ang2ipix
    ON {schema}.{table} (q3c_ang2ipix("{{{{ params._q3c_ra }}}}", "{{{{ params._q3c_dec }}}}"))
    WITH (fillfactor=100){if_tablespace};
CLUSTER {table}_q3c_ang2ipix ON {schema}.{table};
"""
    assert test_operator.sql == expected_render


@pytest.mark.parametrize('schema,table,tablespace', [('ic_schema', 'ic_table', None),
                                                     ('params.schema', 'params.table', None),
                                                     ('ic_schema', 'ic_table', 'data3'),
                                                     ('params.schema', 'params.table', 'params.index_tablespace')])
def test_index_columns(temporary_airflow_home, schema, table, tablespace):
    """Test the index_columns function.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    try:
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
    except ImportError:
        from airflow.providers.postgres.operators.postgres import PostgresOperator

    p = import_module('..postgresql', package='dlairflow.test')
    function_name = 'index_columns'
    tf = p.__dict__[function_name]
    test_operator = tf("login,password,host,schema", schema, table,
                       columns=['ra', 'dec',
                                ('id', 'survey', 'program'),
                                12345,
                                {'test_schema.uint64': 'specobjid'}],
                       tablespace=tablespace)
    assert isinstance(test_operator, PostgresOperator)
    assert test_operator.task_id == function_name
    if tablespace:
        if tablespace.startswith('params.'):
            if_tablespace = (f"{{%- if {tablespace} %}} USING INDEX TABLESPACE " +
                             f"{{{{ {tablespace} }}}}{{%- endif -%}}")
        else:
            if_tablespace = ("{%- if params._ic_tablespace %} USING INDEX " +
                             "TABLESPACE {{ params._ic_tablespace }}{%- endif -%}")
    else:
        if_tablespace = ''
    if schema.startswith('params.'):
        schema = f'{{{{ {schema} }}}}'
    if table.startswith('params.'):
        table = f'{{{{ {table} }}}}'
    expected_render = f"""--
-- Created by dlairflow.postgresql.{function_name}().
--
{{% for col in params.columns %}}
{{% if col is string -%}}
CREATE INDEX {table}_{{{{ col }}}}_idx
    ON {schema}.{table} ("{{{{ col }}}}")
    WITH (fillfactor=100){if_tablespace};
{{% elif col is mapping -%}}
{{% for key, value in col.items() -%}}
CREATE_INDEX {table}_{{{{ key|replace('.', '_') }}}}_{{{{ value }}}}_idx
    ON {schema}.{table} ({{{{ key }}}}({{{{ value }}}}))
    WITH (fillfactor=100){if_tablespace};
{{% endfor %}}
{{% elif col is sequence -%}}
CREATE INDEX {table}_{{{{ col|join("_") }}}}_idx
    ON {schema}.{table} ("{{{{ col|join('", "') }}}}")
    WITH (fillfactor=100){if_tablespace};
{{% else -%}}
-- Unknown type: {{{{ col }}}}.
{{% endif -%}}
{{% endfor %}}
"""
    assert test_operator.sql == expected_render


@pytest.mark.parametrize('keys,schema,tablespace', [({'params.table': 'pk'}, None, None),
                                                    ({'table1': 'pk'}, 'schema1', None),
                                                    ({'params.table': 'pk'}, None, 'data3'),
                                                    ({'table1': 'pk'}, 'schema1', 'data3'),
                                                    ({'params.table': 'pk'}, None, 'params.index_tablespace'),
                                                    ({'table1': 'pk'}, 'schema1', 'params.index_tablespace')])
def test_primary_key(temporary_airflow_home, keys, schema, tablespace):
    """Test the primary_key function.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    try:
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
    except ImportError:
        from airflow.providers.postgres.operators.postgres import PostgresOperator

    p = import_module('..postgresql', package='dlairflow.test')
    function_name = 'primary_key'
    tf = p.__dict__[function_name]
    test_operator = tf("login,password,host,schema",
                       keys,
                       schema=schema,
                       tablespace=tablespace)
    assert isinstance(test_operator, PostgresOperator)
    assert test_operator.task_id == function_name
    if schema is None:
        schema_name = '{{ params.schema }}'
    else:
        schema_name = schema
    if tablespace:
        if tablespace.startswith('params.'):
            if_tablespace = (f"{{%- if {tablespace} %}} USING INDEX TABLESPACE " +
                             f"{{{{ {tablespace} }}}}{{%- endif -%}}")
        else:
            if_tablespace = ("{%- if params._pk_tablespace %} USING INDEX " +
                             "TABLESPACE {{ params._pk_tablespace }}{%- endif -%}")
    else:
        if_tablespace = ''
    if 'params.table' in keys:
        expected_render = f"""--
-- Created by dlairflow.postgresql.{function_name}().
--
{{% if params._pk_columns is string -%}}
ALTER TABLE {schema_name}.{{{{ params.table }}}} ADD PRIMARY KEY ("{{{{ params._pk_columns }}}}")
    WITH (fillfactor=100){if_tablespace};
{{% elif params._pk_columns is sequence -%}}
ALTER TABLE {schema_name}.{{{{ params.table }}}} ADD PRIMARY KEY ("{{{{ params._pk_columns|join('", "') }}}}")
    WITH (fillfactor=100){if_tablespace};
{{% else -%}}
-- Unknown type: {{{{ params._pk_columns }}}}.
{{% endif -%}}
"""
    else:
        expected_render = f"""--
-- Created by dlairflow.postgresql.{function_name}().
--
{{% for table, columns in params._pk_primary_keys.items() %}}
{{% if columns is string -%}}
ALTER TABLE {schema_name}.{{{{ table }}}} ADD PRIMARY KEY ("{{{{ columns }}}}")
    WITH (fillfactor=100){if_tablespace};
{{% elif columns is sequence -%}}
ALTER TABLE {schema_name}.{{{{ table }}}} ADD PRIMARY KEY ("{{{{ columns|join('", "') }}}}")
    WITH (fillfactor=100){if_tablespace};
{{% else -%}}
-- Unknown type: {{{{ columns }}}}.
{{% endif -%}}
{{% endfor %}}
"""
    assert test_operator.sql == expected_render


@pytest.mark.parametrize('schema,tables,restart,cascade', [('schema_name', 'table1', False, False),
                                                           (None, None, False, False),
                                                           (None, ['table1', 'table2'], True, False),
                                                           ('schema_name', ['table1', 'table2'], False, True),
                                                           (None, ['table1', 'table2'], True, True),
                                                           (None, False, False, False)])
def test_truncate_table(temporary_airflow_home, schema, tables, restart, cascade):
    """Test the truncate_table function.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    try:
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
    except ImportError:
        from airflow.providers.postgres.operators.postgres import PostgresOperator

    p = import_module('..postgresql', package='dlairflow.test')
    function_name = 'truncate_table'
    tf = p.__dict__[function_name]
    if tables or tables is None:
        test_operator = tf("login,password,host,schema", schema, tables,
                           restart=restart, cascade=cascade)
        assert isinstance(test_operator, PostgresOperator)
        assert test_operator.task_id == function_name
        if schema is None:
            schema_name = '{{ params.schema }}'
        else:
            schema_name = schema
        if isinstance(tables, list):
            st = ', '.join([f"{schema_name}.{t}" for t in tables])
        elif tables is None:
            st = f"{schema_name}.{{{{ params.table }}}}"
        else:
            st = f"{schema_name}.{tables}"
        expected_render = """--
-- Created by dlairflow.postgresql.{0}().
--
TRUNCATE TABLE
    {1}
    {{% if params._tt_restart -%}}RESTART{{%- else -%}}CONTINUE{{%- endif %}} IDENTITY
    {{% if params._tt_cascade -%}}CASCADE{{%- else -%}}RESTRICT{{%- endif %}};
""".format(function_name, st)
        assert test_operator.sql == expected_render
    else:
        with pytest.raises(ValueError) as excinfo:
            test_operator = tf("login,password,host,schema", 'test_schema', tables,
                               restart=restart, cascade=cascade)
        assert excinfo.value.args[0] == "Unknown type for table, must be string or list-like!"


@pytest.mark.parametrize('schema,tables,full', [(None, None, False),
                                                ('schema1', None, False),
                                                (None, 'table1', False),
                                                ('schema1', ['table1', 'table2'], True),
                                                (None, False, False)])
def test_vacuum_analyze(temporary_airflow_home, schema, tables, full):
    """Test the vacuum_analyze function.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    try:
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
    except ImportError:
        from airflow.providers.postgres.operators.postgres import PostgresOperator

    p = import_module('..postgresql', package='dlairflow.test')
    function_name = 'vacuum_analyze'
    tf = p.__dict__[function_name]
    if tables or tables is None:
        test_operator = tf("login,password,host,schema", schema=schema, table=tables,
                           full=full)
        assert isinstance(test_operator, PostgresOperator)
        assert test_operator.task_id == function_name
        if schema is None:
            schema_name = '{{ params.schema }}'
        else:
            schema_name = schema
        if tables is None:
            expected_render = f"""--
-- Created by dlairflow.postgresql.vacuum_analyze().
--
VACUUM {{% if params._va_full -%}}FULL{{%- endif %}} VERBOSE ANALYZE {schema_name}.{{{{ params.table }}}};
"""
        else:
            expected_render = """--
-- Created by dlairflow.postgresql.vacuum_analyze().
--
{% for table in params._va_tables %}
VACUUM {% if params._va_full -%}FULL{%- endif %} VERBOSE ANALYZE {{ params._va_schema }}.{{ table }};
{% endfor %}
"""
        assert test_operator.sql == expected_render
    else:
        with pytest.raises(ValueError) as excinfo:
            test_operator = tf("login,password,host,schema", schema='test_schema',
                               table=tables, full=full)
        assert excinfo.value.args[0] == "Unknown type for table, must be string or list-like!"
