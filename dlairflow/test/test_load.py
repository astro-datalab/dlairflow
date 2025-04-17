# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.load.
"""
import os
import pytest
from importlib import import_module
from .test_postgresql import MockConnection, temporary_airflow_home


@pytest.mark.parametrize('task_function,load_dir', [('load_table_with_fits2db', 'load_dir'),])
def test_load_table(monkeypatch, temporary_airflow_home, task_function, load_dir):
    """Test various loading functions.
    """
    def mock_connection(connection):
        conn = MockConnection(connection)
        return conn

    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    from airflow.hooks.base import BaseHook
    from airflow.operators.bash import BashOperator

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    p = import_module('..load', package='dlairflow.test')

    tf = p.__dict__[task_function]
    test_operator = tf("login,password,host,schema", "schema", "table", load_dir)

    assert isinstance(test_operator, BashOperator)
    assert test_operator.env['PGHOST'] == 'host'
    assert test_operator.params['schema'] == 'schema'
    assert test_operator.params['load_dir'] == 'load_dir'
