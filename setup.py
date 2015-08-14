#! /usr/bin/env python
# -*- coding: utf-8 -*-
'''setup for shove'''

from os import getcwd
from os.path import join

from setuptools import setup, find_packages


def getversion(fname):
    '''Get the __version__ without importing.'''
    with open(fname) as f:
        for line in f:
            if line.startswith('__version__'):
                return '%s.%s.%s' % eval(line[13:].rstrip())

install_requires = list(l.strip() for l in open(
    join(getcwd(), 'reqs/requires.txt'),
))

setup(
    name='shove',
    version=getversion('shove/__init__.py'),
    description='Common object storage frontend',
    long_description=open('README.rst').read(),
    author='L. C. Rees',
    author_email='lcrees@gmail.com',
    url='https://bitbucket.org/lcrees/shove/',
    license='BSD',
    packages=find_packages(),
    test_suite='tests',
    install_requires=install_requires,
    zip_safe=False,
    keywords='object storage persistence database shelve',
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
    entry_points='''
    [shove.stores]
    dbm=shove.store:DBMStore
    file=shove.store:FileStore
    memory=shove.store:MemoryStore
    simple=shove.store:SimpleStore
    [shove.caches]
    file=shove.cache:FileCache
    filelru=shove.cache:FileLRUCache
    memlru=shove.cache:MemoryLRUCache
    memory=shove.cache:MemoryCache
    simple=shove.cache:SimpleCache
    simplelru=shove.cache:SimpleLRUCache
    ''',
)
