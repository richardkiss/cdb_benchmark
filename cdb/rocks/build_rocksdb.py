# build_rocksdb.py

import subprocess

from cffi import FFI

ffibuilder = FFI()

# C declarations
ffibuilder.cdef("""
    typedef struct rocksdb_t rocksdb_t;
    typedef struct rocksdb_options_t rocksdb_options_t;
    typedef struct rocksdb_writeoptions_t rocksdb_writeoptions_t;
    typedef struct rocksdb_readoptions_t rocksdb_readoptions_t;
    
    // Memory management
    void rocksdb_free(void* ptr);
    
    // Options
    rocksdb_options_t* rocksdb_options_create();
    void rocksdb_options_destroy(rocksdb_options_t*);
    void rocksdb_options_set_create_if_missing(rocksdb_options_t*, unsigned char);
    
    // Database operations
    rocksdb_t* rocksdb_open(const rocksdb_options_t* options,
                           const char* name, char** errptr);
    void rocksdb_close(rocksdb_t* db);
    
    // Read/Write options
    rocksdb_writeoptions_t* rocksdb_writeoptions_create();
    void rocksdb_writeoptions_destroy(rocksdb_writeoptions_t*);
    rocksdb_readoptions_t* rocksdb_readoptions_create();
    void rocksdb_readoptions_destroy(rocksdb_readoptions_t*);
    
    // Iterator
    typedef struct rocksdb_iterator_t rocksdb_iterator_t;
    rocksdb_iterator_t* rocksdb_create_iterator(rocksdb_t* db, const rocksdb_readoptions_t* options);
    void rocksdb_iter_destroy(rocksdb_iterator_t* iterator);
    unsigned char rocksdb_iter_valid(const rocksdb_iterator_t* iterator);
    void rocksdb_iter_seek_to_first(rocksdb_iterator_t* iterator);
    void rocksdb_iter_next(rocksdb_iterator_t* iterator);
    const char* rocksdb_iter_key(const rocksdb_iterator_t* iterator, size_t* keylen);
    const char* rocksdb_iter_value(const rocksdb_iterator_t* iterator, size_t* vallen);
    
    // Key/Value operations
    void rocksdb_put(rocksdb_t* db, const rocksdb_writeoptions_t* options,
                     const char* key, size_t keylen,
                     const char* val, size_t vallen,
                     char** errptr);
    char* rocksdb_get(rocksdb_t* db, const rocksdb_readoptions_t* options,
                      const char* key, size_t keylen,
                      size_t* vallen, char** errptr);
    void rocksdb_delete(rocksdb_t* db, const rocksdb_writeoptions_t* options,
                        const char* key, size_t keylen, char** errptr);
""")


def get_pkg_config_info():
    try:
        cflags = (
            subprocess.check_output(["pkg-config", "--cflags", "rocksdb"])
            .decode()
            .strip()
        )
        libs = (
            subprocess.check_output(["pkg-config", "--libs", "rocksdb"])
            .decode()
            .strip()
        )
        return cflags, libs
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None, None


cflags, libs = get_pkg_config_info()

# C source
ffibuilder.set_source(
    "_rocksdb_cffi",
    """
    #include <rocksdb/c.h>
    """,
    libraries=["rocksdb"],
    include_dirs=[
        "/usr/include",  # Debian default
        "/usr/local/include",  # Common Unix location
        "/opt/local/include",
    ],  # Other locations
    library_dirs=[
        "/usr/lib/x86_64-linux-gnu",  # Debian default
        "/usr/lib",
        "/usr/local/lib",
    ],
    extra_compile_args=cflags.split() if cflags else [],
    extra_link_args=libs.split() if libs else [],
)

if __name__ == "__main__":
    ffibuilder.compile(verbose=True)
