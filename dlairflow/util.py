# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""
dlairflow.util
==============

Generic, low-level utility functions. Some functions may be intended
for internal use by the package itself.
"""
import os


def user_scratch():
    """A standard, per-user scratch directory.

    This function simply returns a path. It does not guarantee the directory exists.

    Returns
    -------
    :class:`str`
        The name of the directory.
    """
    return os.path.join('/data0', 'datalab', os.environ['USER'])
