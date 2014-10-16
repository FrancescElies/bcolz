from collections import OrderedDict
from bcolz import carray
import numpy as np


def factorize_pure(carray_):
    count = 0
    lookup = OrderedDict()
    n = len(carray_)
    labels = carray([], dtype='uint64', expectedlen=n)

    for element in carray_[:]:
        try:
            idx = lookup[element]
        except KeyError:
            lookup[element] = idx = count
            count += 1
        labels.append(idx)

    return labels, lookup


def factorize_pure2(carray_):
    count = 0
    lookup = OrderedDict()
    n = len(carray_)
    labels = carray([], dtype='uint64', expectedlen=n)

    buffer = np.empty(len(carray_.chunks[0][:]), dtype='uint64')

    for chunk in carray_.chunks:

        for i, element in enumerate(chunk[:]):
            try:
                idx = lookup[element]
            except KeyError:
                lookup[element] = idx = count
                count += 1
            buffer[i] = idx
        labels.append(buffer)

    return labels, lookup

def groupsort_indexer(labels, reverse):
    ngroups = len(reverse)
    # count group sizes, location 0 for NA
    counts = np.zeros(ngroups + 1, dtype=np.int64)
    n = len(labels)
    for i in range(n):
        print labels[i] +1
        counts[labels[i] + 1] += 1

    # mark the start of each contiguous group of like-indexed data
    where = np.zeros(ngroups + 1, dtype=np.int64)
    for i in range(1, ngroups + 1):
        where[i] = where[i - 1] + counts[i - 1]

    # this is our indexer
    result = np.zeros(n, dtype=np.int64)
    for i in range(n):
        label = labels[i] + 1
        result[where[label]] = i
        where[label] += 1
    return result, counts

import numpy, random, bcolz, tempfile, shutil
N = int(1e2)
a = numpy.fromiter((random.choice(['NY', 'IL', 'OH', 'CA']) for _ in xrange(N)), dtype='S2')
rootdir = tempfile.mkdtemp(prefix='bcolz-factorize')
shutil.rmtree(rootdir)
c = bcolz.carray(a, bcolz.cparams(clevel=5, shuffle=False, cname='blosclz'), rootdir=rootdir)
labels, reverse = bcolz.carray_ext.factorize_cython(c)
print groupsort_indexer(labels, reverse)
shutil.rmtree(rootdir)