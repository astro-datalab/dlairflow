# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.postgresql.
"""
import os
import warnings
from ..postgresql import pg_dump_schema, pg_restore_schema
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator


class MockConnection(object):

    def __init__(self, connection):
        foo = connection.split(',')
        self.login = foo[0]
        self.password = foo[1]
        self.host = foo[2]
        self.schema = foo[3]
        return


def test_pg_dump_schema(monkeypatch):
    """Test pg_dump task.
    """
    def mock_connection(connection):
        conn = MockConnection(connection)
        return conn

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    test_operator = pg_dump_schema("login,password,host,schema", "dump_schema")

    assert isinstance(test_operator, BashOperator)
    assert test_operator.env['PGHOST'] == 'host'
    assert test_operator.params['schema'] == 'dump_schema'


def test_pg_dump_schema_alt_dir(monkeypatch):
    """Test pg_dump task with alternate directory.
    """
    def mock_connection(connection):
        conn = MockConnection(connection)
        return conn

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    test_operator = pg_dump_schema("login,password,host,schema", "dump_schema", "dump_dir")

    assert isinstance(test_operator, BashOperator)
    assert test_operator.env['PGHOST'] == 'host'
    assert test_operator.params['dump_dir'] == 'dump_dir'


def test_pg_restore_schema(monkeypatch):
    """Test pg_restore task.
    """
    def mock_connection(connection):
        conn = MockConnection(connection)
        return conn

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    test_operator = pg_restore_schema("login,password,host,schema", "dump_schema")

    assert isinstance(test_operator, BashOperator)
    assert test_operator.env['PGHOST'] == 'host'
    assert test_operator.params['schema'] == 'dump_schema'


def test_pg_restore_schema_alt_dir(monkeypatch):
    """Test pg_restore task with alternate directory.
    """
    def mock_connection(connection):
        conn = MockConnection(connection)
        return conn

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    test_operator = pg_restore_schema("login,password,host,schema", "dump_schema", "dump_dir")

    assert isinstance(test_operator, BashOperator)
    assert test_operator.env['PGHOST'] == 'host'
    assert test_operator.params['dump_dir'] == 'dump_dir'
