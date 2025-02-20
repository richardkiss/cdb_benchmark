# rocksdb.py
from ._rocksdb_cffi import ffi, lib
# import contextlib


class RocksDBError(Exception):
    pass


def _check_error(err):
    if err != ffi.NULL:
        try:
            error_message = ffi.string(err).decode("utf-8")
        finally:
            lib.rocksdb_free(ffi.cast("void*", err))
        raise RocksDBError(error_message)


class RocksDBIterator:
    def __init__(self, db):
        self._db = db
        self._iterator = lib.rocksdb_create_iterator(db._db, db._read_options)
        lib.rocksdb_iter_seek_to_first(self._iterator)

    def __iter__(self):
        return self

    def __next__(self):
        if not lib.rocksdb_iter_valid(self._iterator):
            lib.rocksdb_iter_destroy(self._iterator)
            raise StopIteration

        key_len = ffi.new("size_t*")
        value_len = ffi.new("size_t*")

        key = lib.rocksdb_iter_key(self._iterator, key_len)
        value = lib.rocksdb_iter_value(self._iterator, value_len)

        # Get the key and value as bytes
        key_bytes = ffi.buffer(key, key_len[0])[:]
        value_bytes = ffi.buffer(value, value_len[0])[:]

        # Move iterator to next item
        lib.rocksdb_iter_next(self._iterator)

        return key_bytes, value_bytes


class RocksDB:
    def __init__(self, path, create_if_missing=True):
        self._path = path
        self._create_if_missing = create_if_missing
        self._db = None
        self._options = None
        self._write_options = None
        self._read_options = None
        self.__enter__()

    def __del__(self):
        self.__exit__(None, None, None)

    def __enter__(self):
        # Create options
        self._options = lib.rocksdb_options_create()
        lib.rocksdb_options_set_create_if_missing(
            self._options, self._create_if_missing
        )

        # Open database
        error = ffi.new("char**")
        self._db = lib.rocksdb_open(
            self._options, str(self._path).encode("utf-8"), error
        )
        _check_error(error[0])

        # Create read/write options
        self._write_options = lib.rocksdb_writeoptions_create()
        self._read_options = lib.rocksdb_readoptions_create()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._db:
            lib.rocksdb_close(self._db)
            lib.rocksdb_options_destroy(self._options)
            lib.rocksdb_readoptions_destroy(self._read_options)
            lib.rocksdb_writeoptions_destroy(self._write_options)
            self._db = None

    def put(self, key, value):
        if not isinstance(key, (str, bytes)):
            raise TypeError("key must be str or bytes")
        if not isinstance(value, (str, bytes)):
            raise TypeError("value must be str or bytes")

        key = key.encode("utf-8") if isinstance(key, str) else key
        value = value.encode("utf-8") if isinstance(value, str) else value

        error = ffi.new("char**")
        lib.rocksdb_put(
            self._db, self._write_options, key, len(key), value, len(value), error
        )
        _check_error(error[0])

    def get(self, key):
        if not isinstance(key, (str, bytes)):
            raise TypeError("key must be str or bytes")

        key = key.encode("utf-8") if isinstance(key, str) else key

        error = ffi.new("char**")
        value_len = ffi.new("size_t*")
        value = lib.rocksdb_get(
            self._db, self._read_options, key, len(key), value_len, error
        )
        _check_error(error[0])

        if value == ffi.NULL:
            return None

        try:
            return ffi.buffer(value, value_len[0])[:]
        finally:
            lib.rocksdb_free(ffi.cast("void*", value))

    def delete(self, key):
        if not isinstance(key, (str, bytes)):
            raise TypeError("key must be str or bytes")

        key = key.encode("utf-8") if isinstance(key, str) else key

        error = ffi.new("char**")
        lib.rocksdb_delete(self._db, self._write_options, key, len(key), error)
        _check_error(error[0])

    def iterator(self):
        """Create an iterator over all key-value pairs in the database."""
        return RocksDBIterator(self)


# Example usage
if __name__ == "__main__":
    breakpoint()
    with RocksDB("test.db") as db:
        db.put("hello", "world")
        value = db.get("hello")
        print(value.decode("utf-8"))  # prints: world
        db.delete("hello")
