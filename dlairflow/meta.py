# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
dlairflow.meta
==============

Tasks that involve metadata, verification, etc.
"""
try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator


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
