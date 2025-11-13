# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
dlairflow.meta
==============

Tasks that involve metadata, verification, etc.
"""
import os
import warnings
try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def fitsverify(filename):
    """Run :command:`fitsverify` on `filename`.

    Parameters
    ----------
    filename : :class:`str`
        Name of a FITS file to verify.

    Returns
    -------
    :class:`~airflow.operators.bash.BashOperator`
        A BashOperator that will execute :command:`fitsverify`.
    """
    fitsverify_template = "fitsverify {{params.filename}}"
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
    :class:`dict`
        A dictionary containing the metadata. The dictionary can then be
        JSON-encoded.

    Raises
    ------
    ValueError
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
    items = item.split('.')
    schema = items.pop(0)
    try:
        table = items.pop(0)
    except IndexError:
        table = None
    try:
        column = items.pop(0)
    except IndexError:
        column = None
    if items:
        raise ValueError(f"Could not split string '{item}' into schema, table, etc.")
    metadata = {'schema': schema, 'table': table, 'column': column}
    if os.path.isfile(source):
        # Treat source as a file.
        pass
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
        schema_query = "SELECT * FROM information_schema.schemata WHERE schema_name = %s;"
        schema_parameters = (schema,)
        cursor.execute(schema_query, schema_parameters)
        rows = cursor.fetchall()
        if len(rows) == 0:
            raise ValueError(f"Could not find a schema matching '{schema}'.")
        metadata['schema'] = dict(zip([d[0] for d in cursor.description], rows[0]))
        #
        # Get table information.
        #
        if table is None:
            # Find all tables in schema.
            table_query = "SELECT * FROM information_schema.tables WHERE table_schema = %s;"
            table_parameters = (schema,)
        else:
            table_query = "SELECT * FROM information_schema.tables WHERE table_schema = %s AND table_name = %s;"
            table_parameters = (schema, table)
        cursor.execute(table_query, table_parameters)
        rows = cursor.fetchall()
        if len(rows) == 0:
            if table is None:
                warnings.warn(f"Schema '{schema}' has no tables.", UserWarning)
                return metadata
            else:
                # Table isn't there, this is more serious.
                raise ValueError(f"Could not find a table matching '{table}' in schema '{schema}'.")
        metadata['table'] = list()
        for row in rows:
            metadata['table'].append(dict(zip([d[0] for d in cursor.description], row)))
        #
        # Get column information.
        #
        for t in metadata['table']:
            if column is None:
                column_query = ("SELECT * FROM information_schema.columns WHERE " +
                    "table_schema = %s AND table_name = %s;")
                column_parameters = (t['table_schema'], t['table_name'])
            else:
                column_query = ("SELECT * FROM information_schema.columns WHERE " +
                    "table_schema = %s AND table_name = %s AND column_name = %s;")
                column_parameters = (t['table_schema'], t['table_name'], column)
            cursor.execute(column_query, column_parameters)
            rows = cursor.fetchall()
            if len(rows) == 0:
                if column is None:
                    # A schema without tables is possible, but a table without columns is weird.
                    warnings.warn(f"Table '{schema}.{table}' has no columns. This is unusual.", UserWarning)
                    return metadata
                else:
                    raise ValueError(f"Could not find a column matching '{column}' in table '{schema}.{table}'.")
            metadata['column'] = list()
            for row in rows:
                metadata['column'].append(dict(zip([d[0] for d in cursor.description], row)))
    return metadata
