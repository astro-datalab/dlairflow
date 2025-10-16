# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.meta.
"""
import pytest
from importlib import import_module
from .test_postgresql import MockConnection, temporary_airflow_home  # noqa: F401


@pytest.mark.parametrize('task_function,filename', [('fitsverify', 'filename.fits'),])
def test_fitsverify(monkeypatch, temporary_airflow_home, task_function, filename):  # noqa: F811
    """Test various loading functions.
    """
    def mock_connection(connection):
        conn = MockConnection(connection)
        return conn

    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    from airflow.hooks.base import BaseHook
    try:
        from airflow.providers.standard.operators.bash import BashOperator
    except ImportError:
        from airflow.operators.bash import BashOperator

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    p = import_module('..meta', package='dlairflow.test')

    tf = p.__dict__[task_function]
    test_operator = tf(filename)

    assert isinstance(test_operator, BashOperator)
    # assert test_operator.env['PGHOST'] == 'host'
    assert test_operator.params['filename'] == 'filename.fits'
    # assert test_operator.params['load_dir'] == 'load_dir'
