.. dlairflow documentation master file, created by
   sphinx-quickstart on Thu Dec 12 16:04:44 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

=======================
dlairflow documentation
=======================

Reusable code components for building `Apache Airflow®`_ DAGs.

.. _`Apache Airflow®`: https://airflow.apache.org

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   api.rst
   changes.rst

Requirements
------------

Apache Airflow
~~~~~~~~~~~~~~

This package is intended to work with `Apache Airflow®`_. If you install ``dlairflow``
with :command:`pip`, it should install Airflow for you.

Felis
~~~~~

This package is intended to work with `Felis`_. If you install ``dlairflow`` with
:command:`pip`, it should install Felis for you.

fits2db
~~~~~~~

fits2db_ converts FITS_ files into data that can be streamed (piped) directly into
a database. :command:`fits2db` requires a C compiler,
development libraries for PostgreSQL_, MySQL_ and SQLite_, and the cfitsio_ library.
See the README_ file for compile instructions. Once compiled, ensure that
:command:`fits2db` is present in :envvar:`PATH`.


PostgreSQL
~~~~~~~~~~

Clients
+++++++

:command:`fits2db` is often used in conjunction with :command:`psql`, the
PostgreSQL_ command-line client. :command:`psql` must be installed on the system
and present in :envvar:`PATH`. There are PostgreSQL client packages available
for most standard Linux and macOS systems.

In addition to :command:`psql`, ``dlairflow`` requires :command:`pg_dump`
and :command:`pg_restore`.  These are usually all included together in the
same client package.

Airflow support
+++++++++++++++

The package apache-airflow-providers-postgres_ must be installed.
If you install ``dlairflow`` with :command:`pip`, it should install
apache-airflow-providers-postgres_ automatically

.. _apache-airflow-providers-postgres: https://pypi.org/project/apache-airflow-providers-postgres/


Scratch space
~~~~~~~~~~~~~

Some ``dlairflow`` functions and returned task will need to create intermediate
files. We refer to this as "scratch" space.

Optional packages
-----------------

fitsverify
~~~~~~~~~~

fitsverify_ checks FITS_ files for compliance with the `FITS Standard`_.
:command:`fitsverify` requires a C compiler and the cfitsio_ library.
Once compiled, ensure that :command:`fitsverify` is present in :envvar:`PATH`.

Environment Variables
---------------------

.. envvar:: DLAIRFLOW_SCRATCH_ROOT

   Used to specify per-user scratch space. See :func:`dlairflow.util.user_scratch`
   for further details.

.. envvar:: PATH

   This is the standard shell ``PATH`` variable. Several command-line utilities
   used by ``dlairflow`` need to be in ``PATH``, see above.

.. _Felis: https://felis.lsst.io
.. _FITS: https://heasarc.gsfc.nasa.gov/docs/heasarc/fits.html
.. _`FITS Standard`: https://fits.gsfc.nasa.gov/fits_standard.html
.. _fitsverify: https://heasarc.gsfc.nasa.gov/docs/software/ftools/fitsverify/
.. _fits2db: https://github.com/astro-datalab/fits2db
.. _cfitsio: https://heasarc.gsfc.nasa.gov/docs/software/fitsio/fitsio.html
.. _PostgreSQL: https://www.postgresql.org
.. _MySQL: https://www.mysql.com
.. _SQLite: https://www.sqlite.org/index.html
.. _README: https://github.com/astro-datalab/fits2db/blob/master/README.md
