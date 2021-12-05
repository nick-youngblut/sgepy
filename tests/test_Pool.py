#!/usr/bin/env python
# import
from __future__ import print_function
## batteries
import os
import sys
import pytest
import subprocess
## package
from sgepy import SGE

# tests
def test_pool():
    """
    test of pooling function
    """
    subprocess.run(['sgepy-test.py', '--test', 'pool'])
