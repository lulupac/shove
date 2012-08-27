# -*- coding: utf-8 -*-
'''shove cache tests'''

from stuf.six import PY3, unittest

from tests.mixins import Spawn


setUpModule = Spawn.setUpModule
tearDownModule = Spawn.tearDownModule


class NoTimeout(object):

    def setUp(self):
        from shove._imports import cache_backend
        self.cache = cache_backend(self.initstring)

    def tearDown(self):
        self.cache = None

    def test_getitem(self):
        self.cache['test'] = 'test'
        self.assertEqual(self.cache['test'], 'test')

    def test_setitem(self):
        self.cache['test'] = 'test'
        self.assertEqual(self.cache['test'], 'test')

    def test_delitem(self):
        self.cache['test'] = 'test'
        del self.cache['test']
        self.assertEqual('test' in self.cache, False)


class Cache(NoTimeout):

    def test_timeout(self):
        import time
        from shove._imports import cache_backend
        cache = cache_backend(self.initstring, timeout=1)
        cache['test'] = 'test'
        time.sleep(3)
        def tmp(): #@IgnorePep8
            cache['test']
        self.assertRaises(KeyError, tmp)


class CacheCull(Cache):

    def test_cull(self):
        from shove._imports import cache_backend
        cache = cache_backend(self.initstring, max_entries=1)
        cache['test1'] = 'test'
        cache['test2'] = 'test2'
        cache['test3'] = 'test3'
        self.assertEquals(len(cache), 1)


class TestSimpleCache(CacheCull, unittest.TestCase):

    initstring = 'simple://'


class TestSimpleLRUCache(NoTimeout, unittest.TestCase):

    initstring = 'simplelru://'


class TestMemoryCache(CacheCull, unittest.TestCase):

    initstring = 'memory://'


class TestMemoryLRUCache(NoTimeout, unittest.TestCase):

    initstring = 'memlru://'


class TestFileCache(CacheCull, unittest.TestCase):

    initstring = 'file://test'

    def tearDown(self):
        import shutil
        self.cache = None
        shutil.rmtree('test')


class TestFileLRUCache(NoTimeout, unittest.TestCase):

    initstring = 'filelru://test2'

    def tearDown(self):
        import shutil
        self.cache = None
        shutil.rmtree('test2')


class TestDBCache(CacheCull, unittest.TestCase):

    initstring = 'sqlite:///'


if not PY3:
    class TestMemcache(Cache, Spawn, unittest.TestCase):

        initstring = 'memcache://localhost:11211'
        cmd = ['memcached']

    class TestRedisCache(Cache, Spawn, unittest.TestCase):

        initstring = 'redis://localhost:6379/0'
        cmd = ['redis-server']


if __name__ == '__main__':
    unittest.main()
