#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import re
import gzip
import bz2
import argparse
import logging
import time
import multiprocessing as mp
import SGE

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)
class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter,
                      argparse.RawDescriptionHelpFormatter):
    pass

base_dir = os.path.abspath(os.path.dirname(__file__))
tmp_dir = os.path.join(os.path.split(base_dir)[0], 'tmp')

desc = 'Simple script for running tests'
epi = """DESCRIPTION:
Run >= 1 script runner test
"""
parser = argparse.ArgumentParser(description=desc, epilog=epi,
                                 formatter_class=CustomFormatter)
parser.add_argument('--test', type=str, nargs='+', default='lambda',
                    choices = ['lambda', 'kwargs', 'pool', 'all'],
                    help='Test(s) to perform')
parser.add_argument('--tmp-dir', type=str, default=tmp_dir,
                    help='Temporary file directory')
parser.add_argument('-n', '--n-jobs', type=int, default=2,
                    help='No. of parallel jobs')
parser.add_argument('--version', action='version', version='0.0.1')

def func1(x, y=1, z=2):
    time.sleep(x)
    return x * y * z

def main(args):
    if 'lambda' in args.test or 'all' in args.test:
        logging.info('-- lambda function test --')
        func = lambda x: [x**2 for x in range(5)]
        w = SGE.Worker(tmp_dir=args.tmp_dir, verbose=True)
        ret = w.run(func, 2)
        assert ret == [0, 1, 4, 9, 16], 'lambda test failed'
    if 'kwargs' in args.test or 'all' in args.test:
        logging.info('-- kwargs test --')
        kwargs = {'y' : 2, 'z' : 3}
        pkgs = ['time']
        w = SGE.Worker(tmp_dir=args.tmp_dir, kwargs=kwargs, pkgs=pkgs, verbose=True)
        ret = w.run(func1, 1)
        assert ret == 6, 'kwargs test failed'
    if 'pool' in args.test or 'all' in args.test:
        logging.info('-- pool test --')
        kwargs = {'y' : 2, 'z' : 2}
        pkgs = ['time']
        p = SGE.Pool(tmp_dir=args.tmp_dir, kwargs=kwargs, pkgs=pkgs, n_jobs=2, verbose=True)
        ret = p.map(func1, [1,5])
        assert ret == [4, 20], 'pool test failed'

    
if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
