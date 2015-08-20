#! /usr/bin/env python
# -*- coding: utf-8 -*-
'''setup for shove'''

import sys

from setuptools import setup, find_packages


def getversion(fname):
    '''Get __version__ without importing.'''
    with open(fname) as f:
        for line in f:
            if line.startswith('__version__'):
                return '%s.%s.%s' % eval(line[13:].rstrip())

requires = 'setuptools stuf>=0.9.14'
test_requires = 'nose coverage'

system = float('%d.%d' % sys.version_info[:2])

if system < 2.7:
    requires = 'ordereddict importlib futures ' + requires
    test_requires = 'unittest2 ' + test_requires
elif system == 2.7:
    requires = 'futures ' + requires

setup(
    name='shove',
    version=getversion('shove/__init__.py'),
    description='Generic dictionaryish object storage frontend',
    long_description=open('README.rst').read(),
    author='L. C. Rees',
    author_email='lcrees@gmail.com',
    url='https://bitbucket.org/lcrees/shove/',
    license='BSD',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requires.split(' '),
    test_suite='shove.test',
    tests_require=test_requires.split(' '),
    zip_safe=False,
    keywords='object storage persistence database dictionary',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: Implementation :: CPython',
        'Framework :: Setuptools Plugin',
    ],
    entry_points='''\
    [shove.stores]
    dbm=shove.store:DBMStore
    file=shove.store:FileStore
    lite=shove.store:SQLiteStore
    memory=shove.store:MemoryStore
    simple=shove.store:SimpleStore
    [shove.caches]
    file=shove.cache:FileCache
    filelru=shove.cache:FileLRUCache
    lite=shove.cache:SQLiteCache
    memlru=shove.cache:MemoryLRUCache
    memory=shove.cache:MemoryCache
    simple=shove.cache:SimpleCache
    simplelru=shove.cache:SimpleLRUCache
    ''',
)