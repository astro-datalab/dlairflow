# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
dlairflow.util
==============

Generic, low-level utility functions. Some functions may be intended
for internal use by the package itself.
"""
import os


def user_scratch():
    """A standard, per-user scratch directory.

    This function simply returns a path. It does not guarantee the directory exists.

    Returns
    -------
    :class:`str`
        The name of the directory.
    """
    return os.path.join('/data0', 'datalab', os.environ['USER'])


def ensure_sql():
    """Ensure that ``${AIRFLOW_HOME}/dags/sql`` exists.

    Returns
    -------
    :class:`str`
        The full path to the directory.

    Raises
    ------
    KeyError
        If :envvar:`AIRFLOW_HOME` is not defined.
    """
    sql_dir = os.path.join(os.environ['AIRFLOW_HOME'], 'dags', 'sql')
    os.makedirs(sql_dir, exist_ok=True)
    return sql_dir
