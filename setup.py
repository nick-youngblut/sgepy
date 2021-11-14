#!/usr/bin/env python
from setuptools import setup, find_packages
import os
import glob
import codecs

# dependencies
#install_reqs = [
#    'numpy>=1.17.0'
#]

# getting version from __init__.py
def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()

def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError('Unable to find version string.')
    
## install main application
desc = 'haptag-mg package'
setup(
    name = 'sgepy',
    version = get_version('sgepy/__init__.py'),
    description = desc,
    long_description = desc + '\n See README for more information.',
    author = 'Nick Youngblut',
    author_email = 'nyoungb2@gmail.com',
#    install_requires = install_reqs,
#    include_dirs = [numpy.get_include()],
    license = 'MIT license',
    packages = find_packages(),
    package_dir={'sgepy': 'sgepy'},
    url = 'https://github.com/leylabmpi/sgepy'
)




