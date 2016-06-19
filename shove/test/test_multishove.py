# -*- coding: utf-8 -*-
'''multishove tests'''

from stuf.six import unittest


def setUpModule():
    import os
    from tempfile import mkdtemp
    TMP = mkdtemp()
    os.chdir(TMP)


class Multi(object):

    def test__getitem__(self):
        self.store['max'] = 3
        self.store.sync()
        self.assertEqual(self.store['max'], 3)
        self.store.clear()

    def test__setitem__(self):
        self.store['max'] = 3
        self.store.sync()
        self.assertEqual(self.store['max'], 3)
        self.store.clear()

    def test__delitem__(self):
        self.store['max'] = 3
        self.store.sync()
        del self.store['max']
        self.store.sync()
        self.assertEqual('max' in self.store, False)
        self.store.clear()

    def test_get(self):
        self.store['max'] = 3
        self.store.sync()
        self.assertEqual(self.store.get('min'), None)
        self.store.clear()

    def test__cmp__(self):
        from shove.core import MultiShove
        tstore = MultiShove()
        self.store['max'] = 3
        tstore['max'] = 3
        self.store.sync()
        tstore.sync()
        self.assertEqual(self.store, tstore)
        self.store.clear()

    def test__len__(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store['pow'] = 7
        self.store.sync()
        self.assertEqual(len(self.store), 15)
        self.store.clear()

    def test_clear(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store['pow'] = 7
        self.store.clear()
        self.assertEqual(len(self.store), 0)
        self.store.clear()

    def test_items(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store['pow'] = 7
        self.store.sync()
        slist = list(self.store.items())
        self.assertEqual(('min', 6) in slist, True)
        self.store.clear()

    def test_keys(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store['pow'] = 7
        self.store.sync()
        slist = list(self.store.keys())
        self.assertEqual('min' in slist, True)
        self.store.clear()

    def test_values(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store['pow'] = 7
        self.store.sync()
        slist = list(self.store.values())
        self.assertEqual(6 in slist, True)
        self.store.clear()

    def test_pop(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store.sync()
        item = self.store.pop('min')
        self.store.sync()
        self.assertEqual(item, 6)
        self.store.clear()

    def test_popitem(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store['pow'] = 7
        self.store.sync()
        item = self.store.popitem()
        self.store.sync()
        self.assertEqual(len(self.store), 10)
        self.store.clear()

    def test_setdefault(self):
        self.store['max'] = 3
        self.store['min'] = 6
        self.store['powl'] = 7
        self.store.sync()
        self.store.setdefault('pow', 8)
        self.store.sync()
        self.assertEqual(self.store['pow'], 8)
        self.store.clear()

    def test_update(self):
        from shove.core import MultiShove
        tstore = MultiShove()
        tstore['max'] = 3
        tstore['min'] = 6
        tstore['pow'] = 7
        self.store['max'] = 2
        self.store['min'] = 3
        self.store['pow'] = 7
        self.store.update(tstore)
        self.store.sync()
        self.assertEqual(self.store['min'], 6)
        self.store.clear()


class TestMultiShove(Multi, unittest.TestCase):

    stores = (
        'simple://', 'dbm://one.dbm', 'memory://', 'file://two', 'lite://:memory:',
    )

    def setUp(self):
        from shove.core import MultiShove
        self.store = MultiShove(*self.stores, sync=0)

    def tearDown(self):
        import os
        import shutil
        self.store.close()
        shutil.rmtree('two')
        try:
            os.remove('one.dbm')
        except OSError:
            try:
                os.remove('one.dbm.db')
            except OSError:
                pass


class TestThreadShove(unittest.TestCase):

    stores = (
        'simple://', 'memory://', 'file://six', 'lite://:memory:',
    )

    def setUp(self):
        from shove.core import ThreadShove
        self.store = ThreadShove(*self.stores, max_workers=3, sync=0)

    def tearDown(self):
        import shutil
        self.store.close()
        shutil.rmtree('six')


if __name__ == '__main__':
    unittest.main()
