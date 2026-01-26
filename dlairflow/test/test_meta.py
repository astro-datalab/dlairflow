# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.meta.
"""
import pytest
import os
from importlib import import_module
from .test_postgresql import MockConnection, temporary_airflow_home  # noqa: F401


class MockCursor(object):
    """Simulate a database cursor object.
    """
    def __init__(self, hook):
        self.hook = hook

    def execute(self, query, parameters):
        """Simulate executing a query.
        """
        return

    def fetchall(self):
        """Simulate returning rows.
        """
        return [('a',), ('b',), ('c',)]

    @property
    def description(self):
        return [('a',), ('b',), ('c',)]


class MockConn(object):
    """Simulate a database connection object.
    """

    def __init__(self, hook):
        self.hook = hook

    def cursor(self):
        """Return a mock cursor object.
        """
        return MockCursor(self.hook)


class MockHook(MockConnection):
    """Simulate a PostgresHook object.
    """

    def get_conn(self):
        """Return a connection object, which is only used to get a cursor object.
        """
        return MockCursor(self)


@pytest.fixture(scope="function")
def temporary_felis_file(tmp_path_factory):
    """Create a temporary felis file.
    """
    data = """name: temporary_schema
description: "This is a test."

tables:
    - name: table1
      columns:
          - name: id1
            datatype: "long"
            description: "Unique identifier"
          - name: data1
            datatype: "real"
            description: "Real data"
    - name: table2
      columns:
          - name: id2
            datatype: "long"
            description: "Unique identifier"
          - name: data2
            datatype: "real"
            description: "Real data"
"""
    filename = tmp_path_factory.mktemp('felis') / 'felis.yaml'
    with open(filename, 'w') as FELIS:
        FELIS.write(data)
    yield filename
    os.remove(filename)


@pytest.fixture
def mock_postgres(monkeypatch):
    """Configure a mock class to intercept database calls.
    """
    monkeypatch.setattr('dlairflow.meta.PostgresHook', MockHook)


@pytest.mark.parametrize('task_function,filename', [('fitsverify', 'filename.fits'),])
def test_fitsverify(temporary_airflow_home, task_function, filename):  # noqa: F811
    """Test the fitsverify task.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    try:
        from airflow.providers.standard.operators.bash import BashOperator
    except ImportError:
        from airflow.operators.bash import BashOperator

    p = import_module('..meta', package='dlairflow.test')

    tf = p.__dict__[task_function]
    test_operator = tf(filename)

    assert isinstance(test_operator, BashOperator)
    assert test_operator.params['filename'] == 'filename.fits'


@pytest.mark.parametrize('test_source,item', [('felis.yaml', 'name1'),
                                              ('felis.yaml', 'name1.name2'),
                                              ('felis.yaml', 'name1.name2.name3'),
                                              ('felis.yaml', 'name1.name2.name3.name4'),])
def test_get(temporary_airflow_home, temporary_felis_file, mock_postgres, test_source, item):  # noqa: F811
    """Test the get function.
    """
    #
    # Import inside the function to avoid creating $HOME/airflow.
    #
    # try:
    #     from airflow.providers.standard.operators.bash import BashOperator
    # except ImportError:
    #     from airflow.operators.bash import BashOperator

    p = import_module('..meta', package='dlairflow.test')

    get = p.__dict__['get']

    if test_source == 'felis.yaml':
        source = temporary_felis_file
    else:
        source = test_source
    if 'name4' in item:
        with pytest.raises(ValueError) as excinfo:
            meta = get(source, item)
        assert excinfo.value.args[0] == f"Could not split string '{item}' into schema, table, etc."
    elif 'name3' in item:
        meta = get(source, item)
        assert meta['schema'] == 'name1'
        assert meta['table'] == 'name2'
        assert meta['column'] == 'name3'
    elif 'name2' in item:
        meta = get(source, item)
        assert meta['schema'] == 'name1'
        assert meta['table'] == 'name2'
        assert meta['column'] is None
    else:
        meta = get(source, item)
        assert meta['schema'] == 'name1'
        assert meta['table'] is None
        assert meta['column'] is None
