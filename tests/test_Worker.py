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

test_dir = os.path.dirname(__file__)
test_exe = os.path.join(os.path.dirname(test_dir), 'sgepy', 'tests.py')

# tests
def test_lambda():        
    subprocess.run([test_exe])

def test_kwargs():        
    subprocess.run([test_exe, '--test', 'kwargs'])

def test_pool():        
    subprocess.run([test_exe, '--test', 'pool'])
