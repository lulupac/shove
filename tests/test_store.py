# -*- coding: utf-8 -*-
'''shove store tests'''

from stuf.six import unittest, keys, values, items
from tests.mixins import Spawn


setUpModule = Spawn.setUpModule
tearDownModule = Spawn.tearDownModule


class Store(object):

    def setUp(self):
        from shove import Shove
        self.store = Shove(
            self.initstring, optimize=False, compress=True, sync=0,
        )

    def tearDown(self):
        self.store.close()

    def test__getitem__(self):
        self.store['max'] = 3
        self.store.sync()
        self.assertEqual(self.store['max'], 3)

    def test__setitem__(self):
        self.store['max'] = 3
        self.store['d'] = {'A': 1}, {'A': 1}
        self.store['d'] = {'AA': 1}, {'A': 1}
        self.store['d'] = {'AA': 1}, {'AA': 1}
        self.store.sync()
        self.assertEqual(self.store['max'], 3)

    def test__delitem__(self):
        self.store['max'] = 3
        self.store.sync()
        del self.store['max']
        self.store.sync()
        self.assertEqual('max' in self.store, False)

    def test_get(self):
        self.store['max'] = 3
        self.store.sync()
        self.assertEqual(self.store.get('min'), None)

    def test__cmp__(self):
        from shove import Shove
        tstore = Shove()
        self.store['max'] = 3
        self.store.sync()
        tstore['max'] = 3
        self.assertEqual(self.store, tstore)

    def test__len__(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store.sync()
        self.assertEqual(len(self.store), 2)

    def test_items(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store['pow'] = 7
        self.store.sync()
        slist = list(items(self.store))
        self.assertEqual(('min', 6) in slist, True)

    def test_keys(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store['pow'] = 7
        self.store.sync()
        slist = list(keys(self.store))
        self.assertEqual('min' in slist, True)

    def test_values(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store['pow'] = 7
        self.store.sync()
        slist = list(values(self.store))
        self.assertEqual(6 in slist, True)

    def test_pop(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store.sync()
        item = self.store.pop('min')
        self.store.sync()
        self.assertEqual(item, 6)

    def test_setdefault(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store.sync()
        self.assertEqual(self.store.setdefault('pow', 8), 8)
        self.store.sync()
        self.assertEqual(self.store['pow'], 8)

    def test_update(self):
        from shove import Shove
        tstore = Shove()
        tstore['max'] = 3
        tstore['min'] = 6
        tstore['pow'] = 7
        self.store['max'] = 2
        self.store['min'] = 3
        self.store['pow'] = 7
        self.store.update(tstore)
        self.store.sync()
        self.assertEqual(self.store['min'], 6)

    def test_clear(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store['pow'] = 7
        self.store.sync()
        self.store.clear()
        self.store.sync()
        self.assertEqual(len(self.store), 0)

    def test_popitem(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store['pow'] = 7
        self.store.sync()
        item = self.store.popitem()
        self.store.sync()
        self.assertEqual(len(item) + len(self.store), 4)

    def test_close(self):
        self.store.close()
        self.assertEqual(self.store._store, None)
        self.assertEqual(self.store._buffer, None)
        self.assertEqual(self.store._cache, None)


class TestSimpleStore(Store, unittest.TestCase):

    initstring = 'simple://'


class TestMemoryStore(Store, unittest.TestCase):

    initstring = 'memory://'


class TestFileStore(Store, unittest.TestCase):

    initstring = 'file://test'

    def tearDown(self):
        import shutil
        self.store.close()
        shutil.rmtree('test')


class TestDBMStore(Store, unittest.TestCase):

    initstring = 'dbm://test.dbm'

    def tearDown(self):
        import os
        self.store.close()
        try:
            os.remove('test.dbm')
        except OSError:
            os.remove('test.dbm.db')