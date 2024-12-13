# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test dlairflow.util.
"""
import os
from ..util import user_scratch


def test_user_scratch():
    """Test scratch dir.
    """
    assert user_scratch() == os.path.join('/data0', 'datalab', os.environ['USER'])
