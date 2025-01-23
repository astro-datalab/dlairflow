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


def q3c_index(connection, schema, table, ra='ra', dec='dec'):
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

    Returns
    -------
    :class:`~airflow.providers.postgres.operators.postgres.PostgresOperator`
        A task to create a q3c index
    """
    sql_dir = ensure_sql()
    sql_file = os.path.join(sql_dir, "dlairflow.postgresql.q3c_index.sql")
    if not os.path.exists(sql_file):
        sql_data = """--
-- Created by dlairflow.postgresql.q3c_index().
--
CREATE INDEX {{ params.table }}_q3c_ang2ipix ON {{ params['schema'] }}.{{ params['table'] }} (q3c_ang2ipix("{{ params['ra'] }}", "{{ params['dec'] }}")) WITH (fillfactor=100);
CLUSTER {{ params.table }}_q3c_ang2ipix ON {{ params['schema'] }}.{{ params['table'] }};
"""
        with open(sql_file, 'w') as s:
            s.write(sql_data)
    op = PostgresOperator(task_id="q3c_index",
                          postgres_conn_id=connection,
                          sql="sql/dlairflow.postgresql.q3c_index.sql",
                          params={'schema': schema, 'table': table, 'ra': ra, 'dec': dec})
    return op
