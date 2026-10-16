# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
dlairflow.load
==============

Tasks that involve ingesting data.
"""
# _legacy_bash = False
try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    # _legacy_bash = True
from .postgresql import _connection_to_environment


def load_table_with_fits2db(connection, schema=None, table=None, load_dir=None):
    """Create a task to load a database table with :command:`fits2db`.

    This function assumes that a FITS file is defined by::

        f"{load_dir}/{schema}.{table}.fits"

    This function also assumes that :command:`fits2db` and :command:`psql` are
    available in the :envvar:`PATH` seen by the Airflow jobs.

    Any undefined keyword arguments are assumed to be runtime DAG parameters,
    accessed via *e.g.*::

        {{ params.schema }}.{{ params.table }}

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string. This is needed to set
        environment variables. If the connection string is prepended with
        ``params.`` the actual connection is assumed to be set via a
        runtime parameter.
    schema : :class:`str`, optional
        The schema in which `table` is defined.
    table : :class:`str`, optional
        The name of the table.
    load_dir : :class:`str`, optional
        FITS file to load is in this directory.

    Returns
    -------
    :class:`~airflow.providers.standard.operators.bash.BashOperator`
        A BashOperator that will execute :command:`fits2db`.

    Notes
    -----
    * This function leverages the fact that the ``env`` keyword argument to
      :class:`~airflow.providers.standard.operators.bash.BashOperator`
      is templated.
    * Apparently :command:`fits2db` does not return a non-zero error code when it
      encounters an error. This means standard methods to terminate the bash pipeline
      don't work.
    """
    if schema is None:
        schema = '{{ params.schema }}'
    if table is None:
        table = '{{ params.table }}'
    if load_dir is None:
        load_dir = '{{ params.load_dir }}'
    env = _connection_to_environment(connection)
    env['FITS2DB_TABLE'] = f'{schema}.{table}'
    env['FITS2DB_FILE'] = f'{load_dir}/{schema}.{table}.fits'
    load_table_template = "[[ -f ${FITS2DB_FILE} ]] && (fits2db -t ${FITS2DB_TABLE} ${FITS2DB_FILE} | psql)"
    return BashOperator(task_id='load_table_with_fits2db',
                        bash_command=load_table_template,
                        env=env,
                        append_env=True,
                        do_xcom_push=False)
