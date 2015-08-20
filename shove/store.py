# -*- coding: utf-8 -*-
'''shove store support.'''

from collections import MutableMapping
from copy import deepcopy
import shutil
from threading import Condition

from shove._compat import anydbm, synchronized
from shove.base import Mapping, FileBase, SQLiteBase, PathBase, CloseStore


__all__ = 'DBMStore FileStore MemoryStore SimpleStore SQLiteStore'.split()


class BaseStore(Mapping, MutableMapping, CloseStore):

    '''Base store.'''


class SimpleStore(BaseStore):

    '''
    Single-process in-memory store.

    The shove URI for a simple store is:

    simple://
    '''

    def __init__(self, engine, **kw):
        super(SimpleStore, self).__init__(engine, **kw)
        self._store = dict()


class MemoryStore(SimpleStore):

    '''
    Thread-safe in-memory store.

    The shove URI for a memory store is:

    memory://
    '''

    def __init__(self, engine, **kw):
        super(MemoryStore, self).__init__(engine, **kw)
        self._lock = Condition()

    @synchronized
    def __getitem__(self, key):
        return deepcopy(super(MemoryStore, self).__getitem__(key))

    __setitem__ = synchronized(SimpleStore.__setitem__)
    __delitem__ = synchronized(SimpleStore.__delitem__)


class ClientStore(PathBase, BaseStore):

    '''Base store where updates are automatically pickled/unpickled.'''

    def __getitem__(self, key):
        return self.loads(super(ClientStore, self).__getitem__(self.dumps(key)))

    def __setitem__(self, key, value):
        super(ClientStore, self).__setitem__(self.dumps(key), self.dumps(value))

    def __delitem__(self, key):
        super(ClientStore, self).__delitem__(self.dumps(key))


class SyncStore(ClientStore):

    '''Base store where updates have to be synced to disk.'''

    def __setitem__(self, key, value):
        super(SyncStore, self).__setitem__(key, value)
        try:
            self.sync()
        except AttributeError:
            pass

    def __delitem__(self, key):
        super(SyncStore, self).__delitem__(key)
        try:
            self.sync()
        except AttributeError:
            pass


class DBMStore(SyncStore):

    '''
    DBM Database Store.

    shove's URI for DBM stores follows the form:

    dbm://<path>

    Where <path> is a URL path to a DBM database. Alternatively, the native
    pathname to a DBM database can be passed as the 'engine' parameter.
    '''

    init = 'dbm://'

    def __init__(self, engine, **kw):
        super(DBMStore, self).__init__(engine, **kw)
        self._store = anydbm.open(self._engine, 'c')
        try:
            self.sync = self._store.sync
        except AttributeError:
            pass

    def __iter__(self):
        return iter(self.loads(i) for i in self._store.keys())


class FileStore(FileBase, BaseStore):

    '''
    Filesystem-based object store.

    shove's URI for filesystem-based stores follows the form:

    file://<path>

    Where the path is a URI path to a directory on a local filesystem.
    Alternatively, a native pathname to the directory can be passed as the
    'engine' argument.
    '''

    init = 'file://'

    def clear(self):
        '''Clear all objects from store.'''
        shutil.rmtree(self._dir)
        self._createdir()


class SQLiteStore(SQLiteBase, BaseStore):

    '''
    sqlite-based object store.

    shove's URI for sqlite stores follows the form:

    lite://<path>

    Where the path is a URI path to a file on a local filesystem or ":memory:".
    '''

    init = 'lite://'