# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
dlairflow.postgresql
====================

Standard tasks for working with PostgreSQL that can be imported into a DAG.
"""
import os
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
try:
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    _legacy_postgres = True
except ImportError:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
    _legacy_postgres = False
from .util import user_scratch, ensure_sql


def _connection_to_environment(connection):
    """Convert a database connection to environment variables.

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string.

    Returns
    -------
    :class:`dict`
        A dictionary suitable for passing to the ``env`` keyword on, *e.g.*
        :class:`~airflow.operators.bash.BashOperator`.
    """
    conn = BaseHook.get_connection(connection)
    env = {'PGUSER': conn.login,
           'PGPASSWORD': conn.password,
           'PGHOST': conn.host,
           'PGDATABASE': conn.schema}
    return env


def pg_dump_schema(connection, schema, dump_dir):
    """Dump an entire database schema using :command:`pg_dump`.

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string.
    schema : :class:`str`
        The name of the database schema.
    dump_dir : :class:`str`
        Place the dump file in this directory.

    Returns
    -------
    :class:`~airflow.operators.bash.BashOperator`
        A BashOperator that will execute :command:`pg_dump`.
    """
    pg_env = _connection_to_environment(connection)
    return BashOperator(task_id="pg_dump_schema",
                        bash_command=("[[ -f {{ params.dump_dir }}/{{ params.schema }}.dump ]] || " +
                                      "pg_dump --schema={{ params.schema }} --format=c " +
                                      "--file={{ params.dump_dir }}/{{ params.schema }}.dump"),
                        params={'schema': schema,
                                'dump_dir': dump_dir},
                        env=pg_env,
                        append_env=True)


def pg_restore_schema(connection, schema, dump_dir):
    """Restore a database schema using :command:`pg_restore`.

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string.
    schema : :class:`str`
        The name of the database schema.
    dump_dir : :class:`str`
        Find the dump file in this directory.

    Returns
    -------
    :class:`~airflow.operators.bash.BashOperator`
        A BashOperator that will execute :command:`pg_restore`.
    """
    pg_env = _connection_to_environment(connection)
    return BashOperator(task_id="pg_restore_schema",
                        bash_command=("[[ -f {{ params.dump_dir }}/{{ params.schema }}.dump ]] && " +
                                      "pg_restore {{ params.dump_dir }}/{{ params.schema }}.dump"),
                        params={'schema': schema,
                                'dump_dir': dump_dir},
                        env=pg_env,
                        append_env=True)


def q3c_index(connection, schema, table, ra='ra', dec='dec', overwrite=False):
    """Create a q3c index on `schema`.`table`.

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string.
    schema : :class:`str`
        The name of the database schema.
    table : :class:`str`
        The name of the table in `schema`.
    ra : :class:`str`, optional
        Name of the column containing Right Ascension, default 'ra'.
    dec : :class:`str`, optional
        Name of the column containing Declination, default 'dec'.
    overwrite : :class:`bool`, optional
        If ``True`` replace any existing SQL template file.

    Returns
    -------
    :class:`~airflow.providers.postgres.operators.postgres.PostgresOperator`
        A task to create a q3c index.
    """
    sql_dir = ensure_sql()
    sql_basename = "dlairflow.postgresql.q3c_index.sql"
    sql_file = os.path.join(sql_dir, sql_basename)
    if overwrite or not os.path.exists(sql_file):
        sql_data = """--
-- Created by dlairflow.postgresql.q3c_index().
-- Call q3c_index(..., overwrite=True) to replace this file.
--
CREATE INDEX {{ params.table }}_q3c_ang2ipix
    ON {{ params.schema }}.{{ params.table }} (q3c_ang2ipix("{{ params.ra }}", "{{ params.dec }}"))
    WITH (fillfactor=100);
CLUSTER {{ params.table }}_q3c_ang2ipix ON {{ params.schema }}.{{ params.table }};
"""
        with open(sql_file, 'w') as s:
            s.write(sql_data)
    if _legacy_postgres:
        return PostgresOperator(task_id="q3c_index",
                                postgres_conn_id=connection,
                                sql=f"sql/{sql_basename}",
                                params={'schema': schema, 'table': table, 'ra': ra, 'dec': dec})

    else:
        return PostgresOperator(sql=f"sql/{sql_basename}",
                                params={'schema': schema, 'table': table, 'ra': ra, 'dec': dec},
                                conn_id=connection,
                                task_id="q3c_index")


def index_columns(connection, schema, table, columns, overwrite=False):
    """Create "generic" indexes for a set of columns

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string.
    schema : :class:`str`
        The name of the database schema.
    table : :class:`str`
        The name of the table in `schema`.
    columns : :class:`list`
        A list of columns to index. See below for the possible entries in
        the list of columns.
    overwrite : :class:`bool`, optional
        If ``True`` replace any existing SQL template file.

    Returns
    -------
    :class:`~airflow.providers.postgres.operators.postgres.PostgresOperator`
        A task to create several indexes.

    Notes
    -----
    `columns` may be a list containing multiple types:

    * :class:`str`: create an index on one column.
    * :class:`tuple`: create an index on the set of columns in the tuple.
    * :class:`dict`: create a *function* index. The key is the name of the function
      and the value is the column that is the argument to the function.
    * Any other type in `columns` will be ignored.
    """
    sql_dir = ensure_sql()
    sql_basename = "dlairflow.postgresql.index_columns.sql"
    sql_file = os.path.join(sql_dir, sql_basename)
    if overwrite or not os.path.exists(sql_file):
        sql_data = """--
-- Created by dlairflow.postgresql.index_columns().
-- Call index_columns(..., overwrite=True) to replace this file.
--
{% for col in params.columns %}
{% if col is string -%}
CREATE INDEX {{ params.table }}_{{ col }}_idx
    ON {{ params.schema }}.{{ params.table }} ("{{ col }}")
    WITH (fillfactor=100);
{% elif col is mapping -%}
{% for key, value in col.items() -%}
CREATE_INDEX {{ params.table }}_{{ key|replace('.', '_') }}_{{ value }}_idx
    ON {{ params.schema }}.{{ params.table }} ({{ key }}({{ value }}))
    WITH (fillfactor=100);
{% endfor %}
{% elif col is sequence -%}
CREATE INDEX {{ params.table }}_{{ col|join("_") }}_idx
    ON {{ params.schema }}.{{ params.table }} ("{{ col|join('", "') }}")
    WITH (fillfactor=100);
{% else -%}
-- Unknown type: {{ col }}.
{% endif -%}
{% endfor %}
"""
        with open(sql_file, 'w') as s:
            s.write(sql_data)
    if _legacy_postgres:
        return PostgresOperator(task_id="index_columns",
                                postgres_conn_id=connection,
                                sql=f"sql/{sql_basename}",
                                params={'schema': schema, 'table': table, 'columns': columns})

    else:
        return PostgresOperator(sql=f"sql/{sql_basename}",
                                params={'schema': schema, 'table': table, 'columns': columns},
                                conn_id=connection,
                                task_id="index_columns")


def primary_key(connection, schema, primary_keys, overwrite=False):
    """Create a primary key on one or more tables in `schema`.

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string.
    schema : :class:`str`
        The name of the database schema.
    primary_keys : :class:`dict`
        A dictionary containing the of the table in `schema` mapped to the
        primary key column(s). See below for details.
    overwrite : :class:`bool`, optional
        If ``True`` replace any existing SQL template file.

    Returns
    -------
    :class:`~airflow.providers.postgres.operators.postgres.PostgresOperator`
        A task to create a primary key.

    Notes
    -----
    `primary_keys` may be a :class:`dict` containing multiple types:

    * The key is the table name within `schema`.
    * The value can be:

      - :class:`str`: create a primary key on one column.
      - :class:`tuple`: create a primary key on the set of columns in the tuple.
      - Any other type will be ignored.
    """
    sql_dir = ensure_sql()
    sql_basename = "dlairflow.postgresql.primary_key.sql"
    sql_file = os.path.join(sql_dir, sql_basename)
    if overwrite or not os.path.exists(sql_file):
        sql_data = """--
-- Created by dlairflow.postgresql.primary_key().
-- Call primary_key(..., overwrite=True) to replace this file.
--
{% for table, columns in params.primary_keys.items() %}
{% if columns is string -%}
ALTER TABLE {{ params.schema }}.{{ table }} ADD PRIMARY KEY ("{{ columns }}");
{% elif columns is sequence -%}
ALTER TABLE {{ params.schema }}.{{ table }} ADD PRIMARY KEY ("{{ columns|join('", "') }}");
{% else -%}
-- Unknown type: {{ columns }}.
{% endif -%}
{% endfor %}
"""
        with open(sql_file, 'w') as s:
            s.write(sql_data)
    if _legacy_postgres:
        return PostgresOperator(task_id="primary_key",
                                postgres_conn_id=connection,
                                sql=f"sql/{sql_basename}",
                                params={'schema': schema, 'primary_keys': primary_keys})

    else:
        return PostgresOperator(sql=f"sql/{sql_basename}",
                                params={'schema': schema, 'primary_keys': primary_keys},
                                conn_id=connection,
                                task_id="primary_key")


def vacuum_analyze(connection, schema, table, full=False, overwrite=False):
    """Run ``VACUUM`` and ``ANALYZE`` on one or more tables in `schema`.

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string.
    schema : :class:`str`
        The name of the database schema.
    table : :class:`str` or :class:`list`
        The tables to operate on.
    full : :class:`bool`, optional
        If ``True``, run ``VACUUM FULL``.
    overwrite : :class:`bool`, optional
        If ``True`` replace any existing SQL template file.

    Returns
    -------
    :class:`~airflow.providers.postgres.operators.postgres.PostgresOperator`
        A task to run a ``VACUUM`` command.

    Notes
    -----
    The returned :class:`~airflow.providers.postgres.operators.postgres.PostgresOperator`
    has `autocommit=True` set, which inhibits execution of SQL commands in a
    transaction block. Normally a transaction block is a good thing, but ``VACUUM``
    cannot be run in a transaction block.
    """
    if isinstance(table, str):
        tables = [table]
    else:
        tables = table
    sql_dir = ensure_sql()
    sql_basename = "dlairflow.postgresql.vacuum_analyze.sql"
    sql_file = os.path.join(sql_dir, sql_basename)
    if overwrite or not os.path.exists(sql_file):
        sql_data = """--
-- Created by dlairflow.postgresql.vacuum_analyze().
-- Call vacuum_analyze(..., overwrite=True) to replace this file.
--
{% for table in params.tables %}
VACUUM {% if params.full -%}FULL{%- endif %} VERBOSE ANALYZE {{ params.schema }}.{{ table }};
{% endfor %}
"""
        with open(sql_file, 'w') as s:
            s.write(sql_data)
    if _legacy_postgres:
        return PostgresOperator(task_id="vacuum_analyze",
                                postgres_conn_id=connection,
                                sql=f"sql/{sql_basename}",
                                autocommit=True,
                                params={'schema': schema,
                                        'tables': tables,
                                        'full': full})

    else:
        return PostgresOperator(sql=f"sql/{sql_basename}",
                                autocommit=True,
                                params={'schema': schema,
                                        'tables': tables,
                                        'full': full},
                                conn_id=connection,
                                task_id="vacuum_analyze")
