# -*- coding: utf-8 -*-
'''shove core.'''
from __future__ import print_function

from operator import methodcaller
from collections import MutableMapping

from stuf.iterable import xpartmap
from concurrent.futures import ThreadPoolExecutor

from shove._imports import cache_backend, store_backend

__all__ = 'Shove MultiShove'.split()


class Shove(MutableMapping):

    '''Common object frontend class.'''

    def __init__(self, store='simple://', cache='null://', **kw):
        super(Shove, self).__init__()
        # load store backend
        self._store = store_backend(store, **kw)
        # load cache backend
        self._cache = cache_backend(cache, **kw)
        # buffer for lazier writing
        self._buffer = dict()
        # setting for syncing frequency
        self._sync = kw.get('sync', 2)

    def __getitem__(self, key):
        try:
            return self._cache[key]
        except KeyError:
            # synchronize cache with store
            self.sync()
            self._cache[key] = value = self._store[key]
            return value

    def __setitem__(self, key, value):
        self._cache[key] = self._buffer[key] = value
        # when buffer reaches self._limit, write buffer to store
        if len(self._buffer) >= self._sync:
            self.sync()

    def __delitem__(self, key):
        self.sync()
        try:
            del self._cache[key]
        except KeyError:
            pass
        del self._store[key]

    def __len__(self):
        self.sync()
        return len(self._store)

    def __contains__(self, key):
        self.sync()
        return key in self._store

    def __iter__(self):
        self.sync()
        return self._store.__iter__()

    def close(self):
        '''Finalizes and closes shove.'''
        # if close has been called, pass
        if self._store is not None:
            try:
                self.sync()
            except AttributeError:
                pass
            self._store.close()
        self._store = self._cache = self._buffer = None

    def sync(self):
        '''Writes buffer to store.'''
        self._store.update(self._buffer)
        self._buffer.clear()

    def clear(self):
        self._store.clear()
        self._buffer.clear()


def copy_dispatcher(stores):
    """
    Default dispatcher for MultiShoveStore. Copy item in all stores
    """
    def inner_dispatcher(key=None, value=None):
        return range(len(stores))
    return inner_dispatcher


class MultiShove(MutableMapping):
    """
    Common frontend to multiple object stores.

    Lulupac: Re-implementation with no cache by default and a user-defined (key, value) pairs
    distribution dispatcher.

    A dispatcher is instantiated on setup with the list of stores. It takes in arguments the key and value
    and returns a list of store indices in which the key, value pair is stored. dispatcher is only used in __setitem__,
    other methods look up the key in all stores.

    The default dispatcher copies item in all stores.
    """

    def __init__(self, *stores, **kw):
        # init superclass with first store
        super(MultiShove, self).__init__()
        if not stores:
            stores = ('simple://',)
        # load stores
        self._stores = list(store_backend(i, **kw) for i in stores)
        # load cache
        self._cache = cache_backend(kw.get('cache', 'null://'), **kw)
        # buffer for lazy writing
        self._buffer = dict()
        # setting for syncing frequency
        self._sync = kw.get('sync', 2)
        # dispatcher
        self._dispatcher = kw.get('dispatcher', copy_dispatcher)(self._stores)

    def __getitem__(self, key):
        try:
            return self._cache[key]
        except KeyError:
            # flush items in buffer to stores
            self.sync()
            for store in self._stores:
                try:
                    # synchronize cache and store
                    self._cache[key] = value = store[key]
                    return value
                except KeyError:
                    continue
            raise KeyError(key)

    def __setitem__(self, key, value):
        self._cache[key] = self._buffer[key] = value
        # when the buffer reaches self._limit, writes the buffer to the store
        if len(self._buffer) >= self._sync:
            self.sync()

    def __delitem__(self, key):
        # flush items in buffer to stores
        self.sync()
        try:
            del self._cache[key]
        except KeyError:
            pass
        deleted = False
        for store in self._stores:
            try:
                del store[key]
            except KeyError:
                continue
            deleted = True
        if not deleted:
            raise KeyError(key)

    def __contains__(self, key):
        self.sync()
        for store in self._stores:
            if key in store:
                return True
        return False

    def __iter__(self):
        self.sync()
        for store in self._stores:
            for key in store:
                yield key

    def __len__(self):
        self.sync()
        return sum(map(len, self._stores))

    def close(self):
        '''Finalizes and closes shove stores.'''
        self.sync()
        # close stores
        for store in self._stores:
            if hasattr(store, 'close'):
                store.close()
        self._cache = self._buffer = self._stores = None

    def sync(self):
        """
        Writes buffer to stores.
        """
        for key, value in self._buffer.items():
            for store in self._dispatcher(key, value):
                self._stores[store][key] = value
        self._buffer.clear()


# another example of dispatcher for MultiShove

def round_robin_dispatch(stores):
    """
    Disptach item among stores in round-robin fashion.
    """
    import itertools
    store_gen = itertools.cycle(range(len(stores)))

    def inner_dispatcher(key=None, value=None):
        return next(store_gen)
    return inner_dispatcher


class ThreadShove(MultiShove):

    '''Common frontend that syncs multiple object stores with threads.'''

    def __init__(self, *stores, **kw):
        # init superclass with first store
        super(ThreadShove, self).__init__(*stores, **kw)
        self._maxworkers = kw.get('max_workers', 2)

    def __delitem__(self, key):
        try:
            self.sync()
        except AttributeError:
            pass
        with ThreadPoolExecutor(max_workers=self._maxworkers) as executor:
            xpartmap(
                executor.submit,
                self._stores,
                methodcaller('__delitem__', key),
            )
        try:
            del self._cache[key]
        except KeyError:
            pass

    def sync(self):
        '''Writes buffer to store.'''
        with ThreadPoolExecutor(max_workers=self._maxworkers) as executor:
            xpartmap(
                executor.submit,
                self._stores,
                methodcaller('update', self._buffer),
            )
        self._buffer.clear()
