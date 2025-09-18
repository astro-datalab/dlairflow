# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
dlairflow.scripts
=================

Entry points for command-line scripts.
"""
import os
import glob
from .util import ensure_sql


def clean_dlairflow_sql_templates():
    """Entry-point for :command:`clean_dlairflow_sql_templates`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    sql_dir = ensure_sql()
    template_files = glob.glob(os.path.join(sql_dir, 'dlairflow.postgresql.*.sql'))
    for tf in template_files:
        os.remove(tf)
    return 0
