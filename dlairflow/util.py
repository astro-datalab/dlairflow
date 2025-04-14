# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
dlairflow.util
==============

Generic, low-level utility functions. Some functions may be intended
for internal use by the package itself.
"""
import os


def user_scratch(user_key):
    """A standard, per-user scratch directory.

    This function simply returns a path. It does not guarantee the directory exists.
    The environment variable :envvar:`DLAIRFLOW_SCRATCH_ROOT` must be set.

    Parameters
    ----------
    user_key : :class:`str`
        The key associated with the a user's scratch space. This can be any
        arbitrary string such as a single DAG owner, *e.g.* ``dag.owner``.

    Returns
    -------
    :class:`str`
        The name of the directory.

    Raises
    ------
    KeyError
        If :envvar:`DLAIRFLOW_SCRATCH_ROOT` is not set.
    ValueError
        If :envvar:`DLAIRFLOW_SCRATCH_ROOT` is set but *empty*.
    """
    root = os.environ['DLAIRFLOW_SCRATCH_ROOT']
    if not root:
        raise ValueError("DLAIRFLOW_SCRATCH_ROOT is set but empty!")
    return os.path.join(root, user_key)


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
