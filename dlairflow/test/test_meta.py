# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.meta.
"""
import pytest
import os
from importlib import import_module
from .test_postgresql import MockConnection, temporary_airflow_home  # noqa: F401
has_felis = True
try:
    from felis import Schema, Table, Column
except ImportError:
    has_felis = False


class MockCursor(object):
    """Simulate a database cursor object.
    """
    def __init__(self, hook):
        self.hook = hook
        self.last_query = None
        self.last_parameters = None
        return

    def execute(self, query, parameters):
        """Simulate executing a query.
        """
        self.last_query = query
        self.last_parameters = parameters
        return

    def fetchall(self):
        """Simulate returning rows.
        """
        if 'information_schema.schemata' in self.last_query:
            if self.last_parameters[0] == 'no_such_schema':
                return []
            else:
                return [(self.hook.schema, self.last_parameters[0], 'owner'),]
        elif 'information_schema.tables' in self.last_query:
            if 'table_name = %s' in self.last_query:
                if self.last_parameters[1] == 'no_such_table':
                    return []
                else:
                    return [(self.hook.schema, self.last_parameters[0], self.last_parameters[1], 'BASE TABLE'),]
            else:
                if self.last_parameters[0] == 'has_no_tables':
                    return []
                else:
                    return [(self.hook.schema, self.last_parameters[0], 'name1', 'BASE TABLE'),
                            (self.hook.schema, self.last_parameters[0], 'name2', 'BASE TABLE'),
                            (self.hook.schema, self.last_parameters[0], 'name3', 'BASE TABLE')]
        elif 'information_schema.columns' in self.last_query:
            if 'column_name = %s' in self.last_query:
                if self.last_parameters[2] == 'no_such_column':
                    return []
                elif self.last_parameters[2] == 'unknown_type':
                    return [(self.hook.schema, self.last_parameters[0],
                            self.last_parameters[1], self.last_parameters[2], 'ARRAY'),]
                else:
                    return [(self.hook.schema, self.last_parameters[0],
                            self.last_parameters[1], self.last_parameters[2], 'real'),]
            else:
                if self.last_parameters[1] == 'has_no_columns':
                    return []
                else:
                    return [(self.hook.schema, self.last_parameters[0], self.last_parameters[1], 'name1', 'bigint'),
                            (self.hook.schema, self.last_parameters[0], self.last_parameters[1], 'name2', 'integer'),
                            (self.hook.schema, self.last_parameters[0], self.last_parameters[1], 'name3', 'real')]
        else:
            pass

    @property
    def description(self):
        """Column names, etc. associated with the last query.
        """
        if 'information_schema.schemata' in self.last_query:
            return [('catalog_name',), ('schema_name',), ('schema_owner',)]
        elif 'information_schema.tables' in self.last_query:
            return [('table_catalog',), ('table_schema',), ('table_name',), ('table_type',)]
        else:
            # information_schema.columns
            return [('table_catalog',), ('table_schema',), ('table_name',), ('column_name',), ('data_type',)]


class MockConn(object):
    """Simulate a database connection object.
    """

    def __init__(self, hook):
        self.hook = hook
        self.closed = False

    def cursor(self):
        """Return a mock cursor object.
        """
        return MockCursor(self.hook)

    def close(self):
        """Simulate closing.
        """
        self.closed = True
        return


class MockHook(MockConnection):
    """Simulate a PostgresHook object.
    """
    _conn = None

    def get_conn(self):
        """Return a connection object, which is only used to get a cursor object.
        """
        self._conn = MockConn(self)
        return self._conn


@pytest.fixture(scope="function")
def temporary_felis_file(tmp_path_factory):
    """Create a temporary felis file.
    """
    data = """name: name1
description: "This is a test."
"@id": name1

tables:
    - name: name2
      description: "name2 in name1"
      "@id": name1.name2
      columns:
          - name: id1
            datatype: "long"
            description: "Unique identifier"
            "@id": name1.name2.id1
          - name: name3
            datatype: "float"
            description: "Real data"
            "@id": name1.name2.name3
    - name: table2
      description: "table2 in name1"
      "@id": name1.table2
      columns:
          - name: id2
            datatype: long
            description: "Unique identifier"
            "@id": name1.table2.id2
          - name: data2
            datatype: double
            description: "Double data"
            "@id": name1.table2.data2
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
                                              ('felis.yaml', 'name1.name2.name3.name4'),
                                              ('login,password,host,database', 'no_such_schema'),
                                              ('login,password,host,database', 'name1'),
                                              ('login,password,host,database', 'has_no_tables'),
                                              ('login,password,host,database', 'name1.no_such_table'),
                                              ('login,password,host,database', 'name1.name2'),
                                              ('login,password,host,database', 'name1.has_no_columns'),
                                              ('login,password,host,database', 'name1.name2.no_such_column'),
                                              ('login,password,host,database', 'name1.name2.unknown_type'),
                                              ('login,password,host,database', 'name1.name2.name3'),])
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
    if not has_felis:
        pytest.skip("Felis is not installed in the environment.")


    p = import_module('..meta', package='dlairflow.test')

    get = p.__dict__['get']

    if test_source == 'felis.yaml':
        source = temporary_felis_file
        if 'name4' in item:
            with pytest.raises(ValueError) as excinfo:
                meta = get(source, item)
            assert excinfo.value.args[0] == f"Could not split string '{item}' into schema, table, etc."
        else:
            meta = get(source, item)
            if 'name3' in item:
                assert isinstance(meta, Column)
                assert meta.name == 'name3'
                assert meta.id == 'name1.name2.name3'
            elif 'name2' in item:
                assert isinstance(meta, Table)
                assert meta.name == 'name2'
                assert meta.id == 'name1.name2'
            else:
                assert isinstance(meta, Schema)
                assert meta.name == 'name1'
                assert meta.id == 'name1'
    else:
        source = test_source
        if item == 'no_such_schema':
            with pytest.raises(ValueError) as excinfo:
                meta = get(source, item)
            assert excinfo.value.args[0] == f"Could not find a schema matching '{item}'."
        elif item == 'has_no_tables':
            with pytest.warns(UserWarning) as warninfo:
                meta = get(source, item)
            assert isinstance(meta, Schema)
            assert len(meta.tables) == 0
            assert len(warninfo) == 1
            assert warninfo[0].message.args[0] == "Schema 'has_no_tables' has no tables."
        elif item == 'name1.no_such_table':
            with pytest.raises(ValueError) as excinfo:
                meta = get(source, item)
            # assert meta['table'] is None
            assert excinfo.value.args[0] == "Could not find a table matching 'no_such_table' in schema 'name1'."
        elif item == 'name1.name2':
            meta = get(source, item)
            assert isinstance(meta, Table)
            assert len(meta.columns) == 3
        elif item == 'name1.has_no_columns':
            with pytest.warns(UserWarning) as warninfo:
                meta = get(source, item)
            assert isinstance(meta, Table)
            assert len(meta.columns) == 0
            assert len(warninfo) == 1
            assert warninfo[0].message.args[0] == "Table 'name1.has_no_columns' has no columns. This is unusual."
        elif item == 'name1.name2.unknown_type':
            with pytest.warns(UserWarning) as warninfo:
                meta = get(source, item)
            assert isinstance(meta, Column)
            assert len(warninfo) == 1
            assert warninfo[0].message.args[0] == ("Column 'unknown_type' in table " +
                                                   "'name1.name2' has type 'ARRAY' " +
                                                   "which does not correspond to " +
                                                   "any felis type; using 'text'.")
        elif item == 'name1.name2.no_such_column':
            with pytest.raises(ValueError) as excinfo:
                meta = get(source, item)
            assert excinfo.value.args[0] == ("Could not find a column matching " +
                                             "'no_such_column' in table 'name1.name2'.")
        elif item == 'name1.name2.name3':
            meta = get(source, item)
            assert isinstance(meta, Column)
            assert meta.name == 'name3'
            assert meta.id == 'name1.name2.name3'
        else:
            meta = get(source, item)
            assert isinstance(meta, Schema)
            assert len(meta.tables) == 3
            assert len(meta.tables[0].columns) == 3
