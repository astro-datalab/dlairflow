# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
dlairflow.load
==============

Tasks that involve ingesting data.
"""
from airflow.operators.bash import BashOperator
from .postgresql import _connection_to_environment
from .util import user_scratch


def load_table_with_fits2db(connection, schema, table, load_dir=None):
    """Create a task to load a database table with :command:`fits2db`.

    This function assumes that a FITS file is defined by::

        f"{load_dir}/{schema}.{table}.fits"

    This function also assumes that :command:`fits2db` and :command:`psql` are
    available in the :envvar:`PATH` seen by the Airflow jobs.

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string. This is needed to set
        environment variables.
    schema : :class:`str`
        The schema in which `table` is defined.
    table : :class:`str`
        The name of the table.
    load_dir : :class:`str`, optional
        FITS file to load is in this directory.

    Returns
    -------
    :class:`~airflow.operators.bash.BashOperator`
        A BashOperator that will execute :command:`fits2db`.
    """
    if load_dir is None:
        load_dir = user_scratch()
    load_table_template = ("fits2db -t {{ params.schema }}.{{ params.table }} " +
                           "{{ params.load_dir }}/{{ params.schema }}.{{ params.table }}.fits " +
                           "| psql")
    pg_env = _connection_to_environment(connection)
    load_table = BashOperator(task_id='load_table_with_fits2db',
                              bash_command=load_table_template,
                              params={'load_dir': load_dir,
                                      'schema': schema,
                                      'table': table},
                              env=pg_env,
                              append_env=True)
    return load_table
