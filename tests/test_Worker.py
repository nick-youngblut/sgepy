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
def test_help():
     subprocess.run(['sgepy-test.py', '-h'])

def test_lambda():
    """
    simple lambda function
    """
    subprocess.run(['sgepy-test.py', '--test', 'lambda'])

def test_kwargs():
    """
    test using kwargs
    """
    subprocess.run(['sgepy-test.py', '--test', 'kwargs'])

def test_mem():
    """
    test using kwargs & escalating mem resource
    """
    subprocess.run(['sgepy-test.py', '--test', 'mem'])

def test_time():
    """
    test using kwargs & escalating time resource
    """
    subprocess.run(['sgepy-test.py', '--test', 'time'])
    
def test_error():
    """
    test job error 
    """
    subprocess.run(['sgepy-test.py', '--test', 'error'])
