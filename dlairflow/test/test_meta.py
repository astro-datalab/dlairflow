# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.meta.
"""
import pytest
from importlib import import_module
from .test_postgresql import MockConnection, temporary_airflow_home  # noqa: F401
from ..meta import get as meta_get


@pytest.mark.parametrize('task_function,filename', [('fitsverify', 'filename.fits'),])
def test_fitsverify(temporary_airflow_home, task_function, filename):  # noqa: F811
    """Test the fitsverify task.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    from airflow.hooks.base import BaseHook
    try:
        from airflow.providers.standard.operators.bash import BashOperator
    except ImportError:
        from airflow.operators.bash import BashOperator

    p = import_module('..meta', package='dlairflow.test')

    tf = p.__dict__[task_function]
    test_operator = tf(filename)

    assert isinstance(test_operator, BashOperator)
    assert test_operator.params['filename'] == 'filename.fits'


# def test_get():
#     """Test the meta.get() function.
#     """
#     t = meta_get('filename', 'item')
#     assert isinstance(t, dict)
