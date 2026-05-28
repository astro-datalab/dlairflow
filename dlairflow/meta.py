# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
dlairflow.meta
==============

Tasks that involve metadata, verification, etc.

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
import csv
import os
import pathlib
import warnings
import yaml
from astropy.io import fits
from sqlalchemy import MetaData
from .postgresql import _connection_to_environment
try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
_has_felis = True
try:
    from felis import Schema, Table, Column
    from felis.db.database_context import create_database_context
    from felis.diff import DatabaseDiff
    # from pydantic import ValidationError
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


def validate_schema_file(filename,
                         check_description=False,
                         check_redundant_datatypes=False,
                         check_tap_table_indexes=False,
                         check_tap_principal=False):
    """Perform validation on `filename`.

    The file should be a YAML file.

    Parameters
    ----------
    filename : :class:`str`
        Name of a file to validate.
    check_description : :class:`bool`, optional
        Check that all objects have a valid description
    check_redundant_datatypes : :class:`bool`, optional
        Check for redundant type overrides.
    check_tap_table_indexes : :class:`bool`, optional
        Check that every table has a unique TAP table index.
    check_tap_principal : :class:`bool`, optional
        Check that at least one column per table is flagged as TAP principal.

    Returns
    -------
    None

    Raises
    ------
    :exc:`TypeError`
        If `filename` contains invalid types (*e.g.* not 'real', 'double' etc.).
    :exc:`~pydantic.ValidationError`
        If `filename` is invalid according to Felis.

    Notes
    -----
    This function is equivalent to :command:`felis validate [options] schema.yaml`.
    See also `Schema Validation`_.

    .. _`Schema Validation`: https://felis.lsst.io/user-guide/validation.html
    """
    with open(filename, 'r') as YML:
        data = yaml.safe_load(YML)
    context = {'check_description': check_description,
               'check_redundant_datatypes': check_redundant_datatypes,
               'check_tap_table_indexes': check_tap_table_indexes,
               'check_tap_principal': check_tap_principal}
    schema = Schema.model_validate(data, context=context)  # noqa: F841
    return


def _convert_bool(input):
    """Execute :class:`bool` on `input`.

    Parameters
    ----------
    input : :class:`str`
        An arbitrary string.

    Returns
    -------
    :class:`bool`
        The converted value of input.

    Raises
    ------
    :exc:`ValueError`
        If `input` could not be converted in an unambigous way.
    """
    if input.lower() in ('t', 'true', 'yes', '1'):
        return True
    elif input.lower() in ('f', 'false', 'no', '0'):
        return False
    else:
        raise ValueError(f"could not convert string to bool: '{input}'")


def _validate_fits_file(table, filename, hdu, column_order=False):
    """Compare `table` to FITS file `filename`.

    Parameters
    ----------
    table : :class:`~felis.datamodel.Table`
        A table definition from a Felis schema file.
    filename : :class:`str`
        The name of a FITS file.
    hdu : :class:`int` or :class:`str`
        The HDU of `filename` to compare to `table`. If `filename` has
        ``EXTNAME`` keywords set, `hdu` can be a string.
    column_order : :class:`bool`, optional
        If ``True``, the order of columns in `filename` should match the
        order of columns in `table`.
    """
    felis_column_names = [c.name.lower() for c in table.columns]
    with fits.open(filename) as hdulist:
        data = hdulist[hdu].data
    fits_column_names = [c.lower() for c in data.columns.names]
    if column_order:
        compatible_names = felis_column_names == fits_column_names
    else:
        compatible_names = set(felis_column_names) == set(fits_column_names)
    if not compatible_names:
        raise KeyError(f"The columns in '{filename}' do not match the columns of '{table.name}'!")
    map_datatypes = {'short': 'I', 'int': 'J', 'long': 'K',
                     'float': 'E', 'double': 'D', 'boolean': 'L'}
    for k, column in enumerate(table.columns):
        fits_column = data.columns.names[fits_column_names.index(felis_column_names[k])]
        fits_datatype = data.columns.formats[fits_column_names.index(felis_column_names[k])]
        compatible = False
        try:
            compatible = map_datatypes[str(column.datatype)] == fits_datatype
        except KeyError:
            if column.datatype in ('char', 'string', 'text', 'timestamp') and 'A' in fits_datatype:
                # Eventually compare string lengths here.
                compatible = True
        if not compatible:
            raise TypeError(f"The column '{fits_column}' in '{filename}' has an " +
                            f"incompatible type (expected '{column.datatype}')!")
    return


def _validate_csv_file(table, filename, column_order=False):
    """Compare `table` to CSV file `filename`.

    Parameters
    ----------
    table : :class:`~felis.datamodel.Table`
        A table definition from a Felis schema file.
    filename : :class:`str`
        The name of a CSV file.
    column_order : :class:`bool`, optional
        If ``True``, the order of columns in `filename` should match the
        order of columns in `table`.

    Returns
    -------
    None

    Raises
    ------
    :exc:`KeyError`
        If the column names in `filename` are not compatible with the
        column names in `table`.
    :exc:`TypeError`
        If `filename` contains values in columns that don't match the type
        specified in `table`.
    """
    felis_column_names = [c.name.lower() for c in table.columns]
    with open(filename, newline='') as CSV:
        reader = csv.DictReader(CSV)
        row = next(reader)
    csv_column_names = [c.lower() for c in reader.fieldnames]
    if column_order:
        compatible_names = felis_column_names == csv_column_names
    else:
        compatible_names = set(felis_column_names) == set(csv_column_names)
    if not compatible_names:
        raise KeyError(f"The columns in '{filename}' do not match the columns of '{table.name}'!")
    for k, column in enumerate(table.columns):
        csv_column = reader.fieldnames[csv_column_names.index(felis_column_names[k])]
        csv_value = row[csv_column]
        if column.datatype in ('short', 'int', 'long'):
            convert_type = int
        elif column.datatype in ('float', 'double'):
            convert_type = float
        elif column.datatype in ('boolean'):
            convert_type = _convert_bool
        elif column.datatype in ('char', 'string', 'text', 'timestamp'):
            convert_type = str
        try:
            felis_value = convert_type(csv_value)  # noqa: F841
        except ValueError:
            raise TypeError(f"The column '{csv_column}' in '{filename}' has an " +
                            f"incompatible type (expected '{column.datatype}')!")
    return


def validate_data_files(schema_file, table_name, data_files,
                        data_format='fits',
                        hdu=1,
                        column_order=False):
    """Validate one or more data files against `schema_file`.

    Parameters
    ----------
    schema_file : :class:`str`
        Name of a valid Felis schema file.
    table_name : :class:`str`
        The name of a table defined within `schema_file`.
    data_files : :class:`str` or :class:`list`
        One or more data files to validate.
    data_format : :class:`str`, optional
        Format of `data_files`. Could be 'fits' or 'csv'.
    hdu : :class:`int` or :class:`str`, optional
        The HDU of the data files to compare to `table`. If the files have
        ``EXTNAME`` keywords set, `hdu` can be a string. This keyword is ignored
        if `data_format` is not 'fits'.
    column_order : :class:`bool`, optional
        If ``True``, the order of columns in `data_files` should match the
        order of columns in `schema_file`.

    Raises
    ------
    :exc:`KeyError`
        If the column names in the data files are not compatible with the
        column names defined for `table_name`.
    :exc:`TypeError`
        If the data files contain values in columns that don't match the type
        specified in `schema_file`.
    :exc:`ValueError`
        If `table_name` is not defined in `schema_file`, or if `data_format` is
        an unknown format.
    """
    with open(schema_file, 'r') as YML:
        schema_data = yaml.safe_load(YML)
    schema = Schema.model_validate(schema_data)
    found_table = [t for t in schema.tables if t.name == table_name]
    if len(found_table) != 1:
        raise ValueError(f"{table_name!r} is not defined in {schema_file!r}!")
    table = found_table[0]
    if isinstance(data_files, (str, pathlib.Path)):
        data_files = [data_files]
    if data_format not in ('fits', 'csv'):
        raise ValueError(f"Unknown type {data_format!r} for data files!")
    for data in data_files:
        if data_format == 'fits':
            _validate_fits_file(table, data, hdu=hdu, column_order=column_order)
        # elif data_format == 'csv':
        else:
            _validate_csv_file(table, data, column_order=column_order)
    return


def _connection_to_sqlalchemy_url(connection, db_type='postgresql'):
    """Convert a database connection to a `SQLAlchemy Database URL`_.

    .. _`SQLAlchemy Database URL`: https://docs.sqlalchemy.org/en/20/core/engines.html

    Parameters
    ----------
    connection : :class:`str`
        An Airflow database connection string.
    db_type : :class:`str`, optional
        The "dialect plus driver" of the database.

    Returns
    -------
    :class:`str`
        A connection URL as a string.

    Notes
    -----
    Although ``PostgresHook`` objects have a ``sqlalchemy_url`` attribute,
    not all subclasses of ``BaseHook`` are guaranteed to have such an attribute.
    """
    env = _connection_to_environment(connection)
    return f"{db_type}://{env['PGUSER']}:{env['PGPASSWORD']}@{env['PGHOST']}/{env['PGDATABASE']}"


def validate_database(schema_file, connection, db_type='postgresql', id_generation=False):
    """Validate the table(s) defined in `schema_file` against a database.

    Parameters
    ----------
    schema_file : :class:`str`
        Name of a valid Felis schema file.
    connection : :class:`str`
        An Airflow database connection string.
    db_type : :class:`str`, optional
        The "dialect plus driver" of the database.
    id_generation : :class:`bool`, optional
        If ``True`` generate or assume the generation of IDs for objects
        that do not have them.

    Raises
    ------
    :exc:`ValueError`
        If the tables in `connection` do not match `schema_file`.
    """
    schema = Schema.from_uri(schema_file,
                             context={"id_generation": id_generation})
    database_url = _connection_to_sqlalchemy_url(connection, db_type=db_type)
    metadata = MetaData()
    with create_database_context(database_url, metadata) as db:
        diff = DatabaseDiff(schema, db.engine)
    if diff.has_changes:
        raise ValueError(f"The database '{database_url}' does not match {schema_file}!")
    return
