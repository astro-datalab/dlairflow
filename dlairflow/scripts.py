# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
dlairflow.scripts
=================

Entry points for command-line scripts.
"""
import os
import sys
import glob
from argparse import ArgumentParser
from .util import ensure_sql
from . import __version__ as dlairflow_version


def clean_dlairflow_sql_templates():
    """Entry-point for :command:`clean_dlairflow_sql_templates`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    prsr = ArgumentParser(prog=os.path.basename(sys.argv[0]),
                          description="Clean up dlairflow SQL templates.")
    prsr.add_argument('-d', '--debug', action='store_true',
                      help="Print debugging information.")
    prsr.add_argument('-g', '--glob', action='store', metavar='GLOB',
                      default='dlairflow.postgresql.*.sql',
                      help='Remove files matching GLOB, default "%(default)s".')
    prsr.add_argument('-t', '--test', action='store_true',
                      help='Do not remove anything, only show what would be removed.')
    prsr.add_argument('-V', '--version', action='version', version=f'%(prog)s {dlairflow_version}')
    options = prsr.parse_args()
    sql_dir = ensure_sql()
    if options.debug or options.test:
        print(f"DEBUG: template_files = glob.glob(os.path.join('{sql_dir}', '{options.glob}'))")
    template_files = glob.glob(os.path.join(sql_dir, options.glob))
    for tf in template_files:
        if options.debug or options.test:
            print(f"DEBUG: os.remove('{tf}')")
        if not options.test:
            os.remove(tf)
    return 0
