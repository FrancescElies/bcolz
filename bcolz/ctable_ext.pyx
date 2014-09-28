import numpy as np
cimport numpy as np
import cython
import bcolz
from cpython.ref cimport PyObject, Py_DECREF, Py_INCREF, Py_XDECREF
from cpython cimport (PyDict_New, PyDict_GetItem, PyDict_SetItem,
                      PyDict_Contains, PyDict_Keys,
                      PyTuple_SET_ITEM,
                      PyList_Check, PyList_GET_SIZE, PyFloat_Check,
                      PyString_Check,
                      PyBytes_Check,
                      PyTuple_SetItem,
                      PyTuple_New,
                      PyObject_SetAttrString)


@cython.boundscheck(False)
@cython.wraparound(False)
cpdef carray_is_in(carray col, set value_set, np.ndarray boolarr, bint reverse):
    """
    Update a boolean array with checks whether the values of a column (col) are in a set (value_set)
    Reverse means "not in" functionality

    For the 0d array work around, see https://github.com/Blosc/bcolz/issues/61

    :param col:
    :param value_set:
    :param boolarr:
    :param reverse:
    :return:
    """
    cdef Py_ssize_t i, col_len
    col_len = len(col)
    if not reverse:
        for i in range(col_len):
            if boolarr[i] == True:
                val = col[i]
                # numpy 0d array work around
                if type(val) == np.ndarray:
                    val = val[()]
                if val not in value_set:
                    boolarr[i] = False
    else:
        for i in range(col_len):
            if boolarr[i] == True:
                val = col[i]
                # numpy 0d array work around
                if type(val) == np.ndarray:
                    val = val[()]
                if val in value_set:
                    boolarr[i] = False


@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline object _dict_update(dict d, tuple key, tuple input_val):
    cdef PyObject *obj = PyDict_GetItem(d, key)
    cdef tuple current_val, new_val
    cdef Py_ssize_t i, N
    if obj is NULL:
        PyDict_SetItem(d, key, input_val)
    else:
        current_val = <object>obj
        N = PyList_GET_SIZE(input_val)
        new_val = PyTuple_New(N)
        for i in range(N):
            row_val = current_val[i] + input_val[i]
            Py_INCREF(row_val)
            PyTuple_SET_ITEM(new_val, i, row_val)
        PyDict_SetItem(d, key, new_val)


@cython.boundscheck(False)
@cython.wraparound(False)
cpdef groupby_cython(ctable_iter, list groupby_cols, list measure_cols):
    """
    Groups the measure_cols over the groupby_cols. Currently only sums are supported.
    NB: current Python standard hash *might* have collisions!

    :param groupby_cols: A list of groupby columns
    :param measure_cols: A list of measure columns (sum only atm)
    :return:
    """
    cdef int actual_len, i, j, col_nr, hash_max, \
        current_index, previous_index, \
        groupby_cols_len, measure_cols_len
    cdef str col
    cdef list outcols, total_matrix
    cdef object row
    cdef bint hash_found
    cdef np.ndarray hash_arr, output_arr
    cdef tuple groupby_tup, measure_tup
    cdef dict total_dict
    total_dict = {}
    hash_max = 0
    current_index = 0
    outcols = groupby_cols + measure_cols
    groupby_cols_len = len(groupby_cols)
    measure_cols_len = len(measure_cols)
    # process the rows
    for row in ctable_iter:
        # lookup values and create hash
        groupby_tup = row[0:groupby_cols_len]
        measure_tup = row[groupby_cols_len:]
        _dict_update(total_dict, groupby_tup, measure_tup)
    # now create the output table
    actual_len = len(total_dict)
    total_matrix = []
    for col in outcols:
        output_arr = np.zeros(actual_len, col_dtype_set[col])
        total_matrix.append(output_arr)
    # now fill the output table
    i = 0
    for groupby_tup, measure_tup in total_dict.iteritems():
        # save groupby
        for j in range(groupby_cols_len):
            total_matrix[j][i] = groupby_tup[j]
        # save measures
        for j in range(measure_cols_len):
            total_matrix[j + groupby_cols_len][i] = measure_tup[j]
        # prepare for next row
        i += 1
    # create the output table
    output_table = bcolz.ctable(columns=total_matrix, names=outcols)
    return output_table

