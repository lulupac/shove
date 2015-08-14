Common object storage frontend that supports
dictionary-style access, object serialization
and compression, and multiple storage and caching
backends.

Current supported storage backends are:

- Filesystem
- Memory

Currently supported caching backends are:

- Filesystem
- Memory

The simplest *shove* use case...

>>> from shove import Shove
>>> store = Shove()

...which creates an in-memory store and cache.

Use of other backends for storage and caching involves
passing an module URI or existing store or cache instance
to *shove* following the form:

>>> from shove import Shove
>>> <storename> = Shove(<store_uri>, <cache_uri>)

Each module-specific URI form is documented in its module. The
URI form follows the URI form used by SQLAlchemy:

    http://www.sqlalchemy.org/docs/core/engines.html

*shove* implements the Python dictionary/mapping API:

    http://docs.python.org/lib/typesmapping.html
