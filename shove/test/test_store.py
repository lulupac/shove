# -*- coding: utf-8 -*-
'''shove store tests'''

from stuf.six import unittest, keys, values, items


class Store(object):

    def setUp(self):
        from shove import Shove
        self.store = Shove(
            self.initstring, sync=0
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


class PathStore(Store):

    def setUp(self):
        import os
        from tempfile import mkdtemp
        TMP = mkdtemp()
        os.environ['TEST_DIR'] = TMP
        os.chdir(TMP)
        super(PathStore, self).setUp()

    def tearDown(self):
        super(PathStore, self).tearDown()
        import os
        from shutil import rmtree
        rmtree(os.environ['TEST_DIR'])
        del os.environ['TEST_DIR']


class TestSimpleStore(Store, unittest.TestCase):

    initstring = 'simple://'


class TestMemoryStore(Store, unittest.TestCase):

    initstring = 'memory://'


class TestFileStore(PathStore, unittest.TestCase):

    initstring = 'file://test'


class TestDBMStore(PathStore, unittest.TestCase):

    initstring = 'dbm://test.dbm'


class TestSQLiteMemoryStore(Store, unittest.TestCase):

    initstring = 'lite://:memory:'


class TestSQLiteDiskStore(PathStore, unittest.TestCase):

    initstring = 'lite://test.db'