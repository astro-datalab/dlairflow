# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.util.
"""
import os
import pytest
from ..util import user_scratch, ensure_sql
from .test_postgresql import temporary_airflow_home


def test_user_scratch(monkeypatch):
    """Test scratch dir.
    """
    monkeypatch.setenv('DLAIRFLOW_SCRATCH_ROOT', '/data0/datalab')
    assert user_scratch('owner') == os.path.join('/data0', 'datalab', 'owner')


def test_user_scratch_exception():
    """Test scratch dir when environment variable is missing.
    """
    with pytest.raises(KeyError):
        foo = user_scratch('owner')


def test_ensure_sql(temporary_airflow_home):
    """Test SQL directory creation.
    """
    assert ensure_sql() == str(temporary_airflow_home / 'dags' / 'sql')
    assert os.path.isdir(str(temporary_airflow_home / 'dags' / 'sql'))


def test_ensure_sql_no_home():
    """Test ensure_sql without AIRFLOW_HOME.
    """
    with pytest.raises(KeyError):
        foo = ensure_sql()
