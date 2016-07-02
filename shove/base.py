# -*- coding: utf-8 -*-
'''shove core.'''

from os import listdir, remove, makedirs
from os.path import exists, join
import sqlite3

from stuf.six import native, pickle

from shove._compat import url2pathname, quote_plus, unquote_plus


class Base(object):

    '''Base for shove.'''

    def __init__(self, engine, **kw):
        # encode/decode (compression, serialization, ...)
        self._encoder = kw.get('encoder', pickle.dumps)
        self._decoder = kw.get('decoder', pickle.loads)

    def __contains__(self, key):
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def dumps(self, value):
        '''Optionally encode object `value`.'''
        return self._encoder(value)

    def loads(self, value):
        '''Optionally decode object `value`.'''
        return self._decoder(value)


class CloseStore(object):

    '''Base store.'''

    def close(self):
        '''Closes internal store and clears object references.'''
        try:
            self._store.close()
        except AttributeError:
            pass
        self._store = None


class Mapping(Base):

    '''Base mapping for shove.'''

    def __getitem__(self, key):
        try:
            return self._store[key]
        except KeyError:
            raise KeyError(key)

    def __setitem__(self, key, value):
        self._store[key] = value

    def __delitem__(self, key):
        try:
            del self._store[key]
        except KeyError:
            raise KeyError(key)

    def __iter__(self):
        return iter(self._store)

    def __len__(self):
        return len(self._store)


class FileBase(Base):

    '''Base for file based storage.'''

    def __init__(self, engine, **kw):
        super(FileBase, self).__init__(engine, **kw)
        if engine.startswith(self.init):
            engine = url2pathname(engine.split('://')[1])
        self._dir = engine
        # Create directory
        if not exists(self._dir):
            self._createdir()

    def __getitem__(self, key):
        # (per Larry Meyn)
        try:
            with open(self._key_to_file(key), 'rb') as item:
                return self.loads(item.read())
        except (IOError, OSError):
            raise KeyError(key)

    def __setitem__(self, key, value):
        # (per Larry Meyn)
        try:
            with open(self._key_to_file(key), 'wb') as item:
                item.write(self.dumps(value))
        except (IOError, OSError):
            raise KeyError(key)

    def __delitem__(self, key):
        try:
            remove(self._key_to_file(key))
        except (IOError, OSError):
            raise KeyError(key)

    def __iter__(self, unquote_plus=unquote_plus):
        for name in listdir(self._dir):
            if not name.startswith('.'):
                yield unquote_plus(name)

    def __contains__(self, key):
        return exists(self._key_to_file(key))

    def __len__(self):
        return sum(1 for i in listdir(self._dir) if not i.startswith('.'))

    def _createdir(self):
        # creates the store directory
        try:
            makedirs(self._dir)
        except OSError:
            raise EnvironmentError(
                'cache directory "{0}" does not exist and could not be '
                'created'.format(self._dir)
            )

    def _key_to_file(self, key):
        # gives the filesystem path for a key
        return join(self._dir, quote_plus(key))


class PathBase(Base):

    '''Base store where updates can be committed to disk.'''

    def __init__(self, engine, **kw):
        super(PathBase, self).__init__(engine, **kw)
        if engine.startswith(self.init):
            self._engine = url2pathname(engine.split('://')[1])


class SQLiteBase(PathBase):

    '''Base for file based storage.'''

    def __init__(self, engine, **kw):
        super(SQLiteBase, self).__init__(engine, **kw)
        # make store table
        self._store = sqlite3.connect(self._engine)
        self._store.text_factory = native
        self._cursor = self._store.cursor()
        # create store table if it does not exist
        self._cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS shove (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            )
            '''
        )
        self._store.commit()

    def __getitem__(self, key):
        self._cursor.execute('SELECT value FROM shove WHERE key=?', (self.dumps(key),))
        row = self._cursor.fetchone()
        if row:
            return self.loads(row[0])
        raise KeyError(key)

    def __setitem__(self, k, v):
        self._cursor.execute(
            'INSERT OR REPLACE INTO shove VALUES (?, ?)',
            (self.dumps(k), self.dumps(v))
        )
        self._store.commit()

    def __delitem__(self, key):
        self._cursor.execute('DELETE FROM shove WHERE key=?', (self.dumps(key),))
        self._store.commit()

    def __iter__(self):
        for row in self._store.execute('SELECT key FROM shove'):
            yield self.loads(row[0])

    def __len__(self):
        return int(self._store.execute('SELECT COUNT(*) FROM shove').fetchone()[0])

    def clear(self):
        self._cursor.execute('DELETE FROM shove')
        self._store.commit()