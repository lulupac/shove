# -*- coding: utf-8 -*-

from stuf.six import PY3, unittest, keys, values, items
from testresources import ResourcedTestCase, TestResourceManager


class EventualStore(object):

    def setUp(self):
        from shove import Shove
        self.store = Shove(self.initstring, compress=True)

    def tearDown(self):
        self.store.close()

    def test__getitem__(self):
        self.store['max'] = 3
        self.store.sync()
        self.assertEqual(self.store['max'], 3)

    def test__setitem__(self):
        self.store['max'] = 3
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


class Store(EventualStore):

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


class TestHgStore(Store, unittest.TestCase):

    initstring = 'hg://test3'

    def tearDown(self):
        import shutil
        self.store.close()
        shutil.rmtree('test3')


class TestGitStore(Store, unittest.TestCase):

    initstring = 'git://test4'

    def tearDown(self):
        import shutil
        self.store.close()
        shutil.rmtree('test4')


class TestDBStore(Store, unittest.TestCase):

    initstring = 'sqlite://'


class MongoDBManager(TestResourceManager):

    setUpCost = 10
    tearDownCost = 5

    def make(self, dependency_resources):
        import os
        from tempfile import mkdtemp
        from fabric.api import local
        self.tmp = mkdtemp()
        os.chdir(self.tmp)
        os.mkdir('mongo')
        local('mongod --dbpath {0}'.format('./mongo/'))

    def clean(self, resource):
        import shutil
        from fabric.api import local
        local('killall mongod')
        shutil.rmtree(self.tmp)


class TestMongoDBStore(Store, unittest.TestCase):

    initstring = 'mongodb://127.0.0.1:27017/shove/shove'
    resources = (('server', MongoDBManager()),)

    def tearDown(self):
        self.store.clear()
        super(TestMongoDBStore, self).tearDown()

    def test_close(self):
        pass


@unittest.skip('reason')
class TestFTPStore(Store, unittest.TestCase):

    initstring = 'put ftp string here'

    def setUp(self):
        from shove import Shove
        self.store = Shove(self.ftpstring, compress=True)

    def tearDown(self):
        self.store.clear()
        self.store.close()


@unittest.skip('reason')
class TestS3Store(Store, unittest.TestCase):

    initstring = 's3 test string here'

    def tearDown(self):
        self.store.clear()
        self.store.close()


if not PY3:
    class TestZODBStore(Store, unittest.TestCase):

        initstring = 'zodb://test.db'

        def tearDown(self):
            self.store.close()
            import os
            os.remove('test.db')
            os.remove('test.db.index')
            os.remove('test.db.tmp')
            os.remove('test.db.lock')

    class TestDurusStore(Store, unittest.TestCase):

        initstring = 'durus://test.durus'

        def tearDown(self):
            import os
            self.store.close()
            os.remove('test.durus')

    class RedisManager(TestResourceManager):
        setUpCost = 10
        tearDownCost = 5

        def make(self, dependency_resources):
            import os
            from tempfile import mkdtemp
            from fabric.api import local
            self.tmp = mkdtemp()
            os.chdir(self.tmp)
            local('redis-server --daemonize=yes')

        def clean(self, resource):
            import os
            import shutil
            from fabric.api import local
            local('killall redis-server')
            os.chdir(self.tmp)
            shutil.rmtree(self.tmp)

    class TestRedisStore(Store, ResourcedTestCase):

        initstring = 'redis://localhost:6379/0'
        resources = (('server', RedisManager()),)

        def tearDown(self):
            self.store.clear()
            self.store.close()
            super(TestRedisStore, self).tearDown()

    @unittest.skip('reason')
    class TestBSDBStore(Store, unittest.TestCase):

        initstring = 'bsddb://test.db'

        def tearDown(self):
            import os
            self.store.close()
            os.remove('test.db')

    class TestCassandraStore(EventualStore, unittest.TestCase):

        def setUp(self):
            ResourcedTestCase.setUp()
            from shove import Shove
            from pycassa.system_manager import SystemManager  # @UnresolvedImport @IgnorePep8
            system_manager = SystemManager('localhost:9160')
            try:
                system_manager.create_column_family('Murk', 'shove')
            except:
                pass
            self.store = Shove('cassandra://localhost:9160/Murk/shove')

        def tearDown(self):
            self.store.clear()
            self.store.close()
            from pycassa.system_manager import SystemManager  # @UnresolvedImport @IgnorePep8
            system_manager = SystemManager('localhost:9160')
            system_manager.drop_column_family('Murk', 'shove')
            ResourcedTestCase.tearDown()

    class TestHDF5Store(Store, unittest.TestCase):

        initstring = 'hdf5://test.hdf5/test'

        def setUp(self):
            from shove import Shove
            self.store = Shove()

        def tearDown(self):
            import os
            self.store.close()
            try:
                os.remove('test.hdf5')
            except OSError:
                pass

    @unittest.skip('reason')
    class TestLevelDBStore(unittest.TestCase):

        initstring = 'leveldb://test'

        def tearDown(self):
            import shutil
            shutil.rmtree('test')


if __name__ == '__main__':
    unittest.main()
