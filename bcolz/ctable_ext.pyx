%load_ext cythonmagic
    """
    Groups the measure_cols over the groupby_cols. Currently only sums are supported.
    NB: current Python standard hash *might* have collisions!

    :param groupby_cols: A list of groupby columns
    :param measure_cols: A list of measure columns (sum only atm)
    :return:
    """

%timeit groupby_carst(self, ['state'], ['cost', 'cost3'])
%timeit groupby_cython(self, ['state'], ['cost', 'cost3'])
bcolz.carray_ext.carray

%%cython
import numpy as np
cimport numpy as np
import cython
import bcolz
@cython.boundscheck(False)
@cython.wraparound(False)
cpdef groupby_cython(input_gen, list groupby_cols, list measure_cols):
    cdef int arr_len, i, j, col_nr, hash_max, current_hash, previous_hash, current_index, previous_index
    cdef str col
    # cdef tuple row <- namedtuple issue?
    cdef list outcols
    # cdef bcolz.carray_ext.carray index_arr, hash_arr, current_col <- array issue
    arr_len = input_gen.size
    # Make a new integer carry index_arr with the length of the ctable
    # -> this will have a per-row index where the new value will go
    index_arr = bcolz.carray(np.zeros(arr_len, dtype=int))
    # Make a new type x carry hash_arr with the length of the ctable
    # -> this will have a hash code of the group by columns;
    hash_arr = bcolz.carray(np.zeros(arr_len, dtype=int))
    # Make an integer variable hash_max that starts at 0
    hash_max = 0
    previous_hash = 0
    current_index = 0
    # first create an overview of which row goes where (fill the index array)
    i = 0
    for row in input_gen.iter(outcols=groupby_cols):
        current_hash = hash(row)
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
        previous_hash = current_hash
        i += 1
    # now create the output data frame
    outcols = groupby_cols + measure_cols
    new_matrix = [np.zeros(hash_max, input_gen.dtype[col]) for col in outcols]
    output_table = bcolz.ctable(columns=new_matrix, names=outcols)
    # and write away the new values by using the index array
    previous_index = -1
    groupby_len = len(groupby_cols)
    i = 0
    for row in input_gen.iter(outcols=outcols):
        current_index = index_arr[i]
        # save groupby cols
        # (a bit unefficient because it does not have to do that each time normally, but only the first time)
        if current_index != previous_index:
            col_nr = 0
            for col in groupby_cols:
                current_col = output_table.cols[col]
                current_col[current_index] = row[col_nr]
                col_nr += 1
        else:
            col_nr = groupby_len
        # do sum operation (only sum atm; have to add mean, median, min, max, etc. in future)
        for col in measure_cols:
            current_col = output_table.cols[col]
            current_col[current_index] += row[col_nr]
            col_nr += 1
        # get ready for the next row of the loop
        i += 1
        previous_index = current_index
    return output_table
