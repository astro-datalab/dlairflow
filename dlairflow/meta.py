# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
dlairflow.meta
==============

Tasks that involve metadata, verification, etc.

.. todo::

    **Implement meta.validate_schema_file**

    Validate Felis yaml file: is the file syntactically correct? The tests
    (done by Felis calls) likely include: self-consistency, column datatypes and
    UCDs have allowed values, etc. The command to use is likely:
    :command:`felis validate [options] schema.yaml`. See also `Schema Validation`_.

.. _`Schema Validation`: https://felis.lsst.io/user-guide/validation.html

.. todo::

    **Implement meta.validate_data_files**

    Validate initial data against its Felis yaml file: are the data files and
    the yaml file compatible? The calling signature should take both the path
    of the Felis yaml file, and the path(s) of data file(s) that are presumably
    described by the yaml file. For each data file, verify that its content
    (*e.g.*, fields, dtypes, indices, etc.) is commensurate with the yaml file.
    The function could either take a single data file path, or a list of such data
    file paths.

.. todo::

    **Implement meta.validate_db_schema**

    Validate DB schema contents against its Felis yaml file.
    This could potentially use Felis' diff funtionality:
    :command:`felis diff --engine-url sqlite:///test.db schema.yaml`
    (see `Command Line Interface`_ )

    Ensure that all of the following are true:

    * All tables and columns as defined in the yaml file for a given schema are
      present in the DB under that schema.
    * No additional tables and column are present in the DB which aren't part
      of the yaml schema file.
    * All column datatypes in the DB correspond to the datatypes defined
      in the yaml file.
    * All columns in the TapSchema in the DB have a column description,
      and that it is identical to the column descriptions in the yaml file.
    * Ensure that for every column in the DB which has a UCD defined,
      the UCD corresponds to the one defined for said column in the yaml file.

.. _`Command Line Interface`: https://felis.lsst.io/user-guide/cli.html#felis-diff
"""
import os
import warnings
try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
_has_felis = True
try:
    from felis import Schema, Table, Column
except ImportError:
    _has_felis = False


_postgresql_to_felis = {'double precision': 'double',
                        'real': 'float',
                        'bigint': 'long',
                        'integer': 'int',
                        'smallint': 'short',
                        'boolean': 'boolean',
                        'timestamp with time zone': 'timestamp',
                        'timestamp without time zone': 'timestamp',
                        'character': 'char',
                        'character varying': 'string',
                        'text': 'text'}


def fitsverify(filename):
    """Run :command:`fitsverify` on `filename`.

    Parameters
    ----------
    filename : :class:`str`
        Name of a FITS file to verify.

    Returns
    -------
    :class:`~airflow.providers.standard.operators.bash.BashOperator`
        A BashOperator that will execute :command:`fitsverify`.
    """
    fitsverify_template = "fitsverify -l {{params.filename}}"
    return BashOperator(task_id='fitsverify',
                        bash_command=fitsverify_template,
                        params={'filename': filename})


def get(source, item):
    """Obtain metadata about `item` from `source`.

    Parameters
    ----------
    source : :class:`str`
        The name of the metadata source. This could be a Felis YAML file or
        a database connection ID.
    item : :class:`str`
        What metadata to extract. See the Notes below for the format of this
        string.

    Returns
    -------
    :class:`~felis.datamodel.Schema` or :class:`~felis.datamodel.Table` or :class:`~felis.datamodel.Column`
        A Felis ``datamodel`` object containing the metadata.

    Raises
    ------
    :exc:`ValueError`
        If `item` does not match the expected format.

    Notes
    -----
    Formats for `item`:

    name1
        The metadata associated with the entire schema 'name1' will be extracted.
    name1.name2
        The metadata associated with table 'name2' in schema 'name1' will be extracted.
    name1.name2.name3
        The metadata associated with column 'name3' in table 'name2' in schema 'name1' will be extracted.
    """
    parts = item.split('.', maxsplit=3)
    if len(parts) > 3:
        raise ValueError(f"Could not split string '{item}' into schema, table, etc.")
    schema, table, column = [*parts, None, None, None][:3]
    if os.path.isfile(source):
        #
        # Treat source as a file.
        #
        felis_schema = Schema.from_uri(source)
        if table is None and column is None:
            return felis_schema
        if len(felis_schema.tables) == 0:
            warnings.warn(f"Schema '{schema}' has no tables.", UserWarning)
            return felis_schema
        table_search = [i for i, t in enumerate(felis_schema.tables) if t.name == table]
        if len(table_search) != 1:
            raise ValueError(f"Could not find a table matching '{table}' in schema '{schema}'.")
        found_table = felis_schema.tables[table_search[0]]
        if column is None:
            if len(found_table.columns) == 0:
                # A schema without tables is possible, but a table without columns is weird.
                warnings.warn(f"Table '{schema}.{table}' has no columns. This is unusual.", UserWarning)
            return found_table
        column_search = [i for i, c in enumerate(found_table.columns) if c.name == column]
        if len(column_search) != 1:
            raise ValueError(f"Could not find a column matching '{column}' in table '{schema}.{table}'.")
        return found_table.columns[column_search[0]]
    else:
        #
        # Treat source as a database connection ID.
        #
        hook = PostgresHook(source)
        conn = hook.get_conn()
        cursor = conn.cursor()
        #
        # Get schema information.
        #
        schema_query = "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE schema_name = %s;"
        schema_parameters = (schema,)
        cursor.execute(schema_query, schema_parameters)
        rows = cursor.fetchall()
        if len(rows) == 0:
            conn.close()
            raise ValueError(f"Could not find a schema matching '{schema}'.")
        felis_schema = Schema(name=schema, id=schema, tables=[])
        #
        # Get table information.
        #
        if table is None:
            # Find all tables in schema.
            table_query = ("SELECT table_catalog, table_schema, table_name, table_type " +
                           "FROM information_schema.tables WHERE table_schema = %s;")
            table_parameters = (schema,)
        else:
            table_query = ("SELECT table_catalog, table_schema, table_name, table_type " +
                           "FROM information_schema.tables WHERE table_schema = %s AND table_name = %s;")
            table_parameters = (schema, table)
        cursor.execute(table_query, table_parameters)
        rows = cursor.fetchall()
        if len(rows) == 0:
            conn.close()
            if table is None:
                warnings.warn(f"Schema '{schema}' has no tables.", UserWarning)
                return felis_schema
            else:
                # Table isn't there, this is more serious.
                raise ValueError(f"Could not find a table matching '{table}' in schema '{schema}'.")
        for row in rows:
            felis_schema.tables.append(Table(name=row[2], id=f"{schema}.{row[2]}", columns=[]))
        #
        # Get column information.
        #
        for t in felis_schema.tables:
            # felis_table_index = [i for i, ft in enumerate(felis_schema.tables) if ft.name == t.name][0]
            if column is None:
                column_query = ("SELECT table_catalog, table_schema, table_name, column_name, data_type " +
                                "FROM information_schema.columns " +
                                "WHERE table_schema = %s AND table_name = %s;")
                column_parameters = (schema, t.name)
            else:
                column_query = ("SELECT table_catalog, table_schema, table_name, column_name, data_type " +
                                "FROM information_schema.columns " +
                                "WHERE table_schema = %s AND table_name = %s AND column_name = %s;")
                column_parameters = (schema, t.name, column)
            cursor.execute(column_query, column_parameters)
            rows = cursor.fetchall()
            if len(rows) == 0:
                conn.close()
                if column is None:
                    # A schema without tables is possible, but a table without columns is weird.
                    warnings.warn(f"Table '{schema}.{table}' has no columns. This is unusual.", UserWarning)
                    return t
                else:
                    raise ValueError(f"Could not find a column matching '{column}' in table '{schema}.{table}'.")
            for row in rows:
                # Map data types back to felis.
                try:
                    felis_data_type = _postgresql_to_felis[row[4]]
                except KeyError:
                    warnings.warn(f"Column '{column}' in table '{schema}.{table}' " +
                                  f"has type '{row[4]}' which does not correspond " +
                                  "to any felis type; using 'text'.", UserWarning)
                    felis_data_type = 'text'
                felis_column = Column(name=row[3], id=f"{schema}.{table}.{row[3]}", datatype=felis_data_type)
                t.columns.append(felis_column)
        #
        # Figure out what to return
        #
        conn.close()
        if column is not None:
            # There should be only one table and one column.
            return felis_schema.tables[0].columns[0]
        if table is not None:
            # There should be only one table.
            return felis_schema.tables[0]
        return felis_schema
