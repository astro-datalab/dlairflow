# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.postgresql.
"""
import os
from ..postgresql import pg_dump_schema, pg_restore_schema
from airflow.hooks.base import BaseHook


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

    print(dir(test_operator))


def test_pg_dump_schema_alt_dir(monkeypatch):
    """Test pg_dump task with alternate directory.
    """
    def mock_connection(connection):
        conn = MockConnection(connection)
        return conn

    monkeypatch.setattr(BaseHook, "get_connection", mock_connection)

    test_operator = pg_dump_schema("login,password,host,schema", "dump_schema", "dump_dir")

    print(dir(test_operator))
