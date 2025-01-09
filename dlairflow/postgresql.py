# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
postgresql
==========

Standard tasks for working with PostgreSQL that can be imported into a DAG.
"""
import os
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from .util import user_scratch


def pg_dump_schema(connection, schema, dump_dir=None):
    """Dump an entire database schema using :command:`pg_dump`.

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string
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
    conn = BaseHook.get_connection(connection)
    return BashOperator(task_id="pg_dump",
                        bash_command=("[[ -f {{ params.dump_dir }}/{{ params.schema }}.dump ]] || " +
                                      "pg_dump --schema={{ params.schema }} --format=c " +
                                      "--file={{ params.dump_dir }}/{{ params.schema }}.dump"),
                        params={'schema': schema,
                                'dump_dir': dump_dir},
                        env={'PGUSER': conn.login,
                             'PGPASSWORD': conn.password,
                             'PGHOST': conn.host,
                             'PGDATABASE': conn.schema},
                        append_env=True)


def pg_restore_schema(connection, schema, dump_dir=None):
    """Restore a database schema using :command:`pg_restore`.

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string
    schema : :class:`str`
        The name of the database schema.
    dump_dir : :class:`str`, optional
        Find the dump file in this directory. If not specified, a standard
        scratch directory will be chosen.

    Returns
    -------
    :class:`~airflow.operators.bash.BashOperator`
        A BashOperator that will execute :command:`pg_dump`.
    """
    if dump_dir is None:
        dump_dir = user_scratch()
    conn = BaseHook.get_connection(connection)
    return BashOperator(task_id="pg_restore",
                        bash_command=("[[ -f {{ params.dump_dir }}/{{ params.schema }}.dump ]] && " +
                                      "pg_restore {{ params.dump_dir }}/{{ params.schema }}.dump"),
                        params={'schema': schema,
                                'dump_dir': dump_dir},
                        env={'PGUSER': conn.login,
                             'PGPASSWORD': conn.password,
                             'PGHOST': conn.host,
                             'PGDATABASE': conn.schema},
                        append_env=True)
