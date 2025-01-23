# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.util.
"""
import os
from ..util import user_scratch, ensure_sql
from .test_postgresql import temporary_airflow_home


def test_user_scratch():
    """Test scratch dir.
    """
    assert user_scratch() == os.path.join('/data0', 'datalab', os.environ['USER'])


def test_ensure_sql(temporary_airflow_home):
    """Test SQL directory creation.
    """
    assert ensure_sql() == str(temporary_airflow_home / 'dags' / 'sql')
    assert os.path.isdir(str(temporary_airflow_home / 'dags' / 'sql'))
