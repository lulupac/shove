# -*- coding: utf-8 -*-
'''shove hstore tests.'''

import os
import time
import signal
import shutil
from tempfile import mkdtemp

from stuf.six import unittest
from subprocess32 import Popen  # @UnresolvedImport

from tests.test_store import Store


def setUpModule():
    temp = mkdtemp()
    os.chdir(temp)
    os.environ['PGDATABASE'] = 'shove'
    os.environ['PGDATA'] = temp
    process = Popen(
        ['initdb', '-A', 'trust', '-E', 'utf-8', '-D', temp],
        stdout=open('/dev/null', 'w'),
        stderr=open('/dev/null', 'w'),
        shell=True,
    )
    process.wait()
    unittest.TestCase.process = Popen(
        ['postgres', '-D', temp, '-h', 'localhost', '-p', '5432', '-F'],
        stdout=open('/dev/null', 'w'),
        stderr=open('/dev/null', 'w'),
        shell=True,
    )
    time.sleep(15.0)
    process = Popen(
        ['createdb', '-h', 'localhost', '-p', '5432', 'shove'],
        stdout=open('/dev/null', 'w'),
        stderr=open('/dev/null', 'w'),
        shell=True,
    )
    process.wait()


def tearDownModule():
    unittest.TestCase.process.send_signal(signal.SIGQUIT)
    unittest.TestCase.process.wait()
    temp = os.environ['PGDATA']
    shutil.rmtree(temp)
    del os.environ['PGDATA']


class TestHStoreStore(Store, unittest.TestCase):

    initstring = 'hstore://localhost:5432/shove'
