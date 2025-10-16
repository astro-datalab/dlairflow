# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.scripts.
"""
import os
import sys
import pytest  # noqa: F401
from ..scripts import clean_dlairflow_sql_templates
from .test_postgresql import temporary_airflow_home  # noqa: F401


def test_clean_sql_templates(temporary_airflow_home, monkeypatch, capsys):  # noqa: F811
    """Test clean_sql_templates.
    """
    sql_dir = str(temporary_airflow_home / 'dags' / 'sql')
    os.makedirs(sql_dir, exist_ok=True)
    function_names = ('one', 'two', 'three')
    for function_name in function_names:
        full_name = os.path.join(sql_dir, f'dlairflow.postgresql.{function_name}.sql')
        with open(full_name, 'w') as SQL:
            SQL.write(f'--\n-- {function_name}\n--\n')
        assert os.path.exists(full_name)
    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['clean_dlairflow_sql_templates', '--debug'])
        assert clean_dlairflow_sql_templates() == 0
        captured = capsys.readouterr()
        assert (captured.out.split('\n')[0] ==
                f"DEBUG: template_files = glob.glob(os.path.join('{sql_dir}', 'dlairflow.postgresql.*.sql'))")
        for function_name in function_names:
            full_name = os.path.join(sql_dir, f'dlairflow.postgresql.{function_name}.sql')
            assert not os.path.exists(full_name)
