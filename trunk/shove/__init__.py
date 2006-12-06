# Copyright (c) 2001-2006 Python Software Foundation
# Copyright (c) 2005, the Lawrence Journal-World
# Copyright (c) 2006 L. C. Rees
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice, 
#       this list of conditions and the following disclaimer.
#    
#    2. Redistributions in binary form must reproduce the above copyright 
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
#    3. Neither the name of Django nor the names of its contributors may be used
#       to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

'''Universal object storage.'''

import os
import zlib
import urllib
try:
    import cPickle as pickle
except ImportError:
    import pickle
try:
    # Import store and cache entry points if setuptools installed
    import pkg_resources
    stores = dict((_store.name, _store) for _store in
        pkg_resources.iter_entry_points('shove.stores'))
    caches = dict((_cache.name, _cache) for _cache in
        pkg_resources.iter_entry_points('shove.caches'))
    # Pass if nothing loaded
    if not stores and not caches: raise ImportError()
except ImportError:
    # Static store registry
    stores = dict(
        simple='shove.store.simple:SimpleStore',
        memory='shove.store.memory:MemoryStore',
        file='shove.store.file:FileStore',
        dbm='shove.store.dbm:DbmStore',
        bsddb='shove.store.bsdb:BsdStore',
        sqlite='shove.store.db:DbStore',
        postgres='shove.store.db:DbStore',
        mysql='shove.store.db:DbStore',
        oracle='shove.store.db:DbStore',
        firebird='shove.store.db:DbStore',
        mssql='shove.store.db:DbStore',
        svn='shove.store.svn:SvnStore',
        s3='shove.store.s3:S3Store',
        ftp='shove.store.ftp:FtpStore',
        zodb='shove.store.zodb:ZodbStore',
        durus='shove.store.durusdb:DurusStore')
    # Cache registry
    caches = dict(
        simple='shove.cache.simple:SimpleCache',
        memory='shove.cache.memory:MemoryCache',
        file='shove.cache.file:FileCache',
        mssql='shove.cache.db:DbCache',
        sqlite='shove.cache.db:DbCache',
        postgres='shove.cache.db:DbCache',
        firebird='shove.cache.db:DbCache',
        mysql='shove.cache.db:DbCache',
        oracle='shove.cache.db:DbCache',
        memcache='shove.cache.memcached:MemCached',
        bsddb='shove.cache.bsdb:BsdCache')

__all__ = ['Shove', 'BaseStore', 'Base', 'storage', 'cache']
    
def synchronized(func):
    '''Decorator to lock and unlock a method (Phillip J. Eby).

    @param func Method to decorate
    '''
    def wrapper(self, *__args, **__kw):
        self._lock.acquire()
        try:
            return func(self, *__args, **__kw)
        finally:
            self._lock.release()
    # Add same meta info
    wrapper.__name__ = func.__name__
    wrapper.__dict__ = func.__dict__
    wrapper.__doc__ = func.__doc__
    return wrapper

def getbackend(uri, engines, **kw):
    '''Loads the right backend based on a URI.

    @param uri Instance or name string
    @param engines A dictionary of scheme/class pairs
    @param kw Keywords'''
    if isinstance(uri, basestring):
        mod = engines[uri.split('://', 1)[0]]
        # Manual load if setuptools not present
        if isinstance(mod, basestring): 
            # Isolate classname from dot path
            module, klass = mod.split(':')
            # Load module
            mod = getattr(__import__(module, '', '', ['']), klass)
        # Load appropriate class from setuptools entry point
        else:
            mod = mod.load()
        # Return instance
        return mod(uri, **kw)
    # No-op for existing instances
    return uri


class Base(object):

    '''Base Mapping class.'''
    
    def __init__(self, engine, **kw):
        super(Base, self).__init__()
        self._compress = kw.get('compress', False)
    
    def __getitem__(self, key):
        raise NotImplementedError()

    def __setitem__(self, key, value):
        raise NotImplementedError()

    def __delitem__(self, key):
        raise NotImplementedError()

    def __contains__(self, key):
        try:
            value = self[key]
        except KeyError:
            return False
        return True

    def get(self, key, default=None):
        '''Fetch a given key from the mapping. If the key does not exist,
        return the default.

        @param key Keyword of item in mapping.
        @param default Default value (default: None)
        '''
        try:
            return self[key]
        except KeyError:
            return default
    
    def dumps(self, value):
        '''Optionally serializes and  compresses an object.
        
        value An object'''
        # Serialize everything but non-ascii strings
        if not isinstance(value, str): value = pickle.dumps(value)
        # Apply maximum compression
        if self._compress: value = zlib.compress(value, 9)
        return value
    
    def loads(self, value):
        '''Deserializes and optionally decompresses an object.

        value Object'''
        if self._compress:
            try:
                value = zlib.decompress(value)
            except zlib.error: pass
        # Load serialized objects
        try:
            value = pickle.loads(value)
        # Pass on strings
        except IndexError: pass
        return value


class BaseStore(Base):

    '''Base Store class (based on DictMixin).'''

    def __init__(self, engine, **kw):
        super(BaseStore, self).__init__(engine, **kw)
        self._store = None

    def __cmp__(self, other):
        if other is None: return False
        if isinstance(other, BaseStore):
            return cmp(dict(self.iteritems()), dict(other.iteritems()))

    def __del__(self):
        '''Handles object clean up if store is deleted and gced.'''
        # __init__ didn't succeed, so don't bother closing
        if not hasattr(self, '_store'): return
        self.close()

    def __iter__(self):
        for k in self.keys(): yield k

    def __len__(self):
        return len(self.keys())

    def __repr__(self):
        return repr(dict(self.iteritems()))

    def close(self):
        '''Closes internal store and clears object references.'''
        try:
            self._store.close()
        except AttributeError: pass
        self._store = None

    def clear(self):
        '''Removes all keys and values from a store.'''
        for key in self.keys(): del self[key]

    def items(self):
        '''Returns a list with all key/value pairs in the store.'''
        return list(self.iteritems())

    def iteritems(self):
        '''Lazily returns all key/value pairs in a store.'''
        for k in self: yield (k, self[k])

    def iterkeys(self):
        '''Lazy returns all keys in a store.'''
        return self.__iter__()

    def itervalues(self):
        '''Lazily returns all values in a store.'''
        for _, v in self.iteritems(): yield v

    def keys(self):
        '''Returns a list with all keys in a store.'''
        raise NotImplementedError()

    def pop(self, key, *args):
        '''Removes and returns a value from a store.

        @param args Default to return if key not present.'''
        if len(args) > 1:
            raise TypeError('pop expected at most 2 arguments, got '\
                + repr(1 + len(args)))
        try:
            value = self[key]
        # Return default if key not in store
        except KeyError:
            if args: return args[0]
        del self[key]
        return value

    def popitem(self):
        '''Removes and returns a key, value pair from a store.'''
        try:
            k, v = self.iteritems().next()
        except StopIteration:
            raise KeyError('Store is empty.')
        del self[k]
        return (k, v)

    def setdefault(self, key, default=None):
        '''Returns the value corresponding to an existing key or sets the
        to key to the default and returns the default.

        @param default Default value (default: None)
        '''
        try:
            return self[key]
        except KeyError:
            self[key] = default
        return default

    def update(self, other=None, **kw):
        '''Adds to or overwrites the values in this store with values from
        another store.

        other Another store
        kw Additional keys and values to store
        '''
        if other is None: pass
        elif hasattr(other, 'iteritems'):
            for k, v in other.iteritems(): self[k] = v
        elif hasattr(other, 'keys'):
            for k in other.keys(): self[k] = other[k]
        else:
            for k, v in other: self[k] = v
        if kw: self.update(kw)

    def values(self):
        '''Returns a list with all values in a store.'''
        return list(v for _, v in self.iteritems()) 
    
    
class Shove(BaseStore):

    '''Shove class.'''
    
    def __init__(self, store='simple://', cache='simple://', **kw):
        super(Shove, self).__init__(store, **kw)
        # Load store
        self._store = getbackend(store, stores, **kw)
        # Load cache
        self._cache = getbackend(cache, caches, **kw)
        # Buffer for lazy writing and setting for syncing frequency
        self._buffer, self._sync = dict(), kw.get('sync', 2)

    def __getitem__(self, key):
        '''Gets a item from shove.'''
        try:
            return self._cache[key]
        except KeyError:
            # Synchronize cache and store
            self.sync()
            value = self._store[key]
            self._cache[key] = value
            return value

    def __setitem__(self, key, value):
        '''Sets an item in shove.'''
        self._cache[key] = self._buffer[key] = value
        # When the buffer reaches self._limit, writes the buffer to the store
        if len(self._buffer) >= self._sync: self.sync()

    def __delitem__(self, key):
        '''Deletes an item from shove.'''
        try:
            del self._cache[key]
        except KeyError: pass
        self.sync()
        del self._store[key]

    def keys(self):
        '''Returns a list of keys in shove.'''
        self.sync()
        return self._store.keys()

    def sync(self):
        '''Writes buffer to store.'''
        for k, v in self._buffer.iteritems(): self._store[k] = v
        self._buffer.clear()
        
    def close(self):
        '''Finalizes and closes shove.'''
        # If close has been called, pass
        if self._store is not None:
            self.sync()
            self._store.close()
            self._store = self._cache = self._buffer = None


class FileBase(Base):

    '''Base class for file based storage.'''    

    def __init__(self, engine, **kw):
        super(FileBase, self).__init__(engine, **kw)
        if engine.startswith('file://'):
            engine = urllib.url2pathname(engine.split('://')[1])
        self._dir = engine
        # Create directory
        if not os.path.exists(self._dir): self._createdir()  

    def __delitem__(self, key):
        try:
            os.remove(self._key_to_file(key))
        except (IOError, OSError):
            raise KeyError('%s' % key)    

    def __contains__(self, key):
        return os.path.exists(self._key_to_file(key))
    
    def __len__(self):
        return len(os.listdir(self._dir))    

    def _createdir(self):
        '''Creates the cache directory.'''
        try:
            os.makedirs(self._dir)
        except OSError:
            raise EnvironmentError('Cache directory "%s" does not exist and ' \
                'could not be created' % self._dir)

    def _key_to_file(self, key):
        '''Gives the filesystem path for a key.'''
        return os.path.join(self._dir, urllib.quote_plus(key))