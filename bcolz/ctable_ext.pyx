%load_ext cythonmagic
    """
    Groups the measure_cols over the groupby_cols. Currently only sums are supported.
    NB: current Python standard hash *might* have collisions!

    :param groupby_cols: A list of groupby columns
    :param measure_cols: A list of measure columns (sum only atm)
    :return:
    """

%timeit df.groupby(['state'])['cost', 'cost2'].sum()
%timeit groupby_cython(fact_bcolz, ['state'], ['cost', 'cost2'])

%%cython
import numpy as np
cimport numpy as np
import cython
import bcolz
from cpython cimport (PyDict_New, PyDict_GetItem, PyDict_SetItem,
                      PyDict_Contains, PyDict_Keys,
                      Py_INCREF, PyTuple_SET_ITEM,
                      PyList_Check, PyFloat_Check,
                      PyString_Check,
                      PyBytes_Check,
                      PyTuple_SetItem,
                      PyTuple_New,
                      PyObject_SetAttrString)
@cython.boundscheck(False)
@cython.wraparound(False)
cpdef groupby_cython(input_ctable, list groupby_cols, list measure_cols):
    cdef int arr_len, i, j, col_nr, hash_max, \
        current_hash, previous_hash, \
        current_index, previous_index, \
        groupby_cols_len, measure_cols_len
    cdef str col
    # cdef tuple row <- namedtuple issue?
    cdef list outcols, new_matrix
    cdef object tup
    cdef bint hash_found
    cdef np.ndarray index_arr, hash_arr, output_arr
    # cdef bcolz.carray_ext.carray index_arr, hash_arr, current_col <- array issue
    arr_len = input_ctable.size
    # Make a new integer array: this will have a per-row index where the new value will go
    index_arr = np.zeros(arr_len, dtype=int)  # bcolz.carray()
    # Make a new type hash array: this will have a hash code of the group by columns
    hash_arr = np.zeros(arr_len, dtype=int)  # bcolz.carray()
    # Make an integer variable hash_max that starts at 0
    hash_max = 0
    previous_hash = 0
    current_index = 0
    groupby_cols_len = len(groupby_cols)
    measure_cols_len = len(measure_cols)
    # first create an overview of which row goes where (fill the index array)
    i = 0
    for row in input_ctable.iter(outcols=groupby_cols):
        tup = PyTuple_New(groupby_cols_len)
        for j in range(groupby_cols_len):
            val = row[j]
            PyTuple_SET_ITEM(tup, j, val)
            Py_INCREF(val)
        current_hash = hash(tup)
        # if the current_hash is unlike the previous hash, lookup the new index value
        if current_hash != previous_hash:
            # check if it already exists as a known value
            hash_found = False
            for j in range(hash_max):
                if current_hash == hash_arr[j]:
                    current_index = j
                    hash_found = True
                    break
            # if it does not exist yet, add it to the hash_arr
            if not hash_found:
                current_index = hash_max
                hash_arr[current_index] = current_hash
                hash_max += 1
        # save where the current row has to go
        index_arr[i] = current_index
        # get ready for the next row of the loop
        i += 1
        previous_hash = current_hash
    # now create the output data frame
    outcols = groupby_cols + measure_cols
    new_matrix = []
    # write the groupby columns
    for col in groupby_cols:
        output_arr = np.zeros(hash_max, input_ctable.dtype[col])
        previous_index = -1
        i = 0
        for val in input_ctable[col].iter():
            current_index = index_arr[i]
            if current_index != previous_index:
                output_arr[current_index] = val
            # get ready for the next row of the loop
            i += 1
            previous_index = current_index
        new_matrix.append(output_arr)
    # write the measure_columns
    for col in measure_cols:
        output_arr = np.zeros(hash_max, input_ctable.dtype[col])
        i = 0
        for val in input_ctable[col].iter():
            current_index = index_arr[i]
            output_arr[current_index] += val
            # get ready for the next row of the loop
            i += 1
        new_matrix.append(output_arr)
    # create the output table
    output_table = bcolz.ctable(columns=new_matrix, names=outcols)
    return output_table
