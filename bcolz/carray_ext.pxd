import numpy as np
cimport numpy as np
import cython

#-----------------------------------------------------------------

# numpy functions & objects
from definitions cimport import_array, ndarray, dtype, \
    malloc, realloc, free, memcpy, memset, strdup, strcmp, \
    npy_uint8, npy_uint32, npy_int32, npy_uint64, npy_int64, npy_float64, \
    PyString_AsString, PyString_GET_SIZE, \
    PyString_FromStringAndSize, \
    Py_BEGIN_ALLOW_THREADS, Py_END_ALLOW_THREADS, \
    PyArray_GETITEM, PyArray_SETITEM, \
    npy_intp, PyBuffer_FromMemory, Py_uintptr_t, Py_ssize_t

ctypedef ndarray ndarray_t

cdef class chunk:
    cdef char typekind, isconstant
    cdef int atomsize, itemsize, blocksize
    cdef int nbytes, cbytes, cdbytes
    cdef int true_count
    cdef char *data
    cdef object atom, constant, dobject

    cdef void _getitem(self, int start, int stop, char *dest)
    cdef compress_data(self, char *data, size_t itemsize, size_t nbytes,
                       object cparams)
    cdef compress_arrdata(self, ndarray array, int itemsize,
                          object cparams, object _memory)
    cpdef ndarray_t _to_ndarray(self)


cdef class chunks(object):
    cdef object _rootdir, _mode
    cdef object dtype, cparams, lastchunkarr
    cdef object chunk_cached
    cdef npy_intp nchunks, nchunk_cached, len

    cdef read_chunk(self, nchunk)
    cdef _save(self, nchunk, chunk_)

cdef class carray:
    cdef int itemsize, atomsize
    cdef int _chunksize, _chunklen, leftover
    cdef int nrowsinbuf, _row
    cdef int sss_mode, wheretrue_mode, where_mode
    cdef npy_intp startb, stopb
    cdef npy_intp start, stop, step, nextelement
    cdef npy_intp _nrow, nrowsread
    cdef npy_intp _nbytes, _cbytes
    cdef npy_intp nhits, limit, skip
    cdef npy_intp expectedlen
    cdef char *lastchunk
    cdef object lastchunkarr, where_arr, arr1
    cdef object _cparams, _dflt
    cdef object _dtype
    cdef object chunks
    cdef object _rootdir, datadir, metadir, _mode
    cdef object _attrs, iter_exhausted
    cdef ndarray iobuf, where_buf
    # For block cache
    cdef int idxcache
    cdef ndarray blockcache
    cdef char *datacache

    cdef void bool_update(self, boolarr, value)
    cdef int getitem_cache(self, npy_intp pos, char *dest)
    cdef reset_iter_sentinels(self)
    cdef int check_zeros(self, object barr)
    cdef _adapt_dtype(self, dtype_, shape)

