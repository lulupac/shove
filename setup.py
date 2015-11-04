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

requires = 'futures setuptools stuf>=0.9.14'
test_requires = 'nose coverage'

if float('%d.%d' % sys.version_info[:2]) < 2.7:
    requires = 'ordereddict importlib ' + requires
    test_requires = 'unittest2 ' + test_requires

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
        'Environment :: Web Environment',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database :: Front-Ends',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: Implementation :: Jython',
    ],
    entry_points='''\
    [shove.stores]
    dbm=shove.store:DBMStore
    file=shove.store:FileStore
    sqlite=shove.store:SQLiteStore
    memory=shove.store:MemoryStore
    simple=shove.store:SimpleStore
    [shove.caches]
    file=shove.cache:FileCache
    filelru=shove.cache:FileLRUCache
    sqlite=shove.cache:SQLiteCache
    memlru=shove.cache:MemoryLRUCache
    memory=shove.cache:MemoryCache
    simple=shove.cache:SimpleCache
    simplelru=shove.cache:SimpleLRUCache
    ''',
)