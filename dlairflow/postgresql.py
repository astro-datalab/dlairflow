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
from airflow.providers.postgres.operators.postgres import PostgresOperator
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


def pg_dump_schema(connection, schema, dump_dir=None):
    """Dump an entire database schema using :command:`pg_dump`.

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string.
    schema : :class:`str`
        The name of the database schema.
    dump_dir : :class:`str`, optional
        Place the dump file in this directory. If not specified, a standard
        scratch directory will be chosen.

    Returns
    -------
    :class:`~airflow.operators.bash.BashOperator`
        A BashOperator that will execute :command:`pg_dump`.
    """
    if dump_dir is None:
        dump_dir = user_scratch()
    pg_env = _connection_to_environment(connection)
    return BashOperator(task_id="pg_dump_schema",
                        bash_command=("[[ -f {{ params.dump_dir }}/{{ params.schema }}.dump ]] || " +
                                      "pg_dump --schema={{ params.schema }} --format=c " +
                                      "--file={{ params.dump_dir }}/{{ params.schema }}.dump"),
                        params={'schema': schema,
                                'dump_dir': dump_dir},
                        env=pg_env,
                        append_env=True)


def pg_restore_schema(connection, schema, dump_dir=None):
    """Restore a database schema using :command:`pg_restore`.

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string.
    schema : :class:`str`
        The name of the database schema.
    dump_dir : :class:`str`, optional
        Find the dump file in this directory. If not specified, a standard
        scratch directory will be chosen.

    Returns
    -------
    :class:`~airflow.operators.bash.BashOperator`
        A BashOperator that will execute :command:`pg_restore`.
    """
    if dump_dir is None:
        dump_dir = user_scratch()
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
        A task to create a q3c index
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
    return PostgresOperator(task_id="q3c_index",
                            postgres_conn_id=connection,
                            sql=f"sql/{sql_basename}",
                            params={'schema': schema, 'table': table, 'ra': ra, 'dec': dec})


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
    return PostgresOperator(task_id="index_columns",
                            postgres_conn_id=connection,
                            sql=f"sql/{sql_basename}",
                            params={'schema': schema, 'table': table, 'columns': columns})
