# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test top-level dlairflow functions.
"""
import re
from .. import __version__ as theVersion


def test_version():
    """Ensure the version conforms to PEP386/PEP440.
    """
    versionre = re.compile(r'([0-9]+!)?([0-9]+)(\.[0-9]+)*((a|b|rc|\.post|\.dev)[0-9]+)?')
    assert versionre.match(theVersion) is not None
