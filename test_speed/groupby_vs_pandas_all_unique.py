import bcolz
from bcolz import carray_ext
import shutil
import tempfile
import os
import pandas as pd
import numpy as np
import contextlib, time
import random, string
from nose.tools import assert_equal

INT32 = 2**31

# -- Context manager --
g_elapsed = 0
@contextlib.contextmanager
def ctime(label=""):
    "Counts the time spent in some context"
    global g_elapsed
    t = time.time()
    yield
    g_elapsed = time.time() - t
    print label, round(g_elapsed, 3), "sec\n"


# -- Common inputs for groupby --
# a1: dtype string
# a2: dtype float64
# a3: dtype int32
# a4: dtype int64
N = int(1e6)
groupby_cols = ['a1', 'a2', 'a3', 'a4']
agg_list = ['m1', 'm2', 'm3']

def gen_almost_unique_row():
    s = ''.join(random.choice(string.lowercase) for i in range(20))
    d = {
        'a1': s,
        'a2': random.random(),
        'a3': random.randint(-INT32, INT32 - 1),
        'a4': random.randint(-INT32, INT32 - 1),
        'm1': 1,
        'm2': 2,
        'm3': 3.2
    }
    yield d

random.seed(1)
projects = [gen_almost_unique_row().next() for _ in range(N)]



# -- Pandas --
df = pd.DataFrame(projects)
# force certain columns dtypes as int32
df.a3 = df.a3.astype(np.int32)
df.m1 = df.m1.astype(np.int32)

# print 'reference\n', df.groupby(groupby_cols, sort=True)[agg_list].sum()
with ctime("--> Pandas groupby"):
    result_pandas = \
        df.groupby(groupby_cols, as_index=False)['m1', 'm2', 'm3'].sum()

elapsed_pandas = g_elapsed

# -- Bcolz --
prefix = 'bcolz-'
rootdir = tempfile.mkdtemp(prefix=prefix)
os.rmdir(rootdir) # folder should be emtpy
fact_bcolz = bcolz.ctable.fromdataframe(df, rootdir=rootdir)
fact_bcolz.rootdir


# this caches the factorizations on-disk directly in the rootdir
with ctime("--> Bcolz caching"):
    fact_bcolz.cache_factor(groupby_cols, refresh=True)

with ctime("--> Bcolz groupby"):
    result_bcolz = fact_bcolz.groupby(groupby_cols, agg_list)
elapsed_bcolz = g_elapsed

print round(elapsed_bcolz / elapsed_pandas, 3), 'x times slower than pandas'

print('--> Check correctness of the result vs pandas, if nothing printed is fine')
assert_equal(result_bcolz.len, len(result_pandas))
for n, row in enumerate(result_bcolz):
    # results might not appear in the same order
    tmp = result_pandas.loc[(result_pandas.a1 == row.a1) &
                            (result_pandas.a2 == row.a2) &
                            (result_pandas.a3 == row.a3) &
                            (result_pandas.a4 == row.a4)]

    assert_equal(row.a1, tmp.a1.values[0])
    assert_equal(row.a2, tmp.a2.values[0])
    assert_equal(row.a3, tmp.a3.values[0])
    assert_equal(row.a4, tmp.a4.values[0])
    assert_equal(row.m1, tmp.m1.values[0])
    assert_equal(row.m2, tmp.m2.values[0])
    assert_equal(row.m3, tmp.m3.values[0])

# -- test speed in Ipython --
# %timeit fact_bcolz.groupby(['a1', 'a2', 'a3'], ['m1', 'm2'])
# %timeit df.groupby(['a1', 'a2', 'a3'], as_index=False)['m1', 'm2'].sum()


# import cytoolz
# cytoolz.groupby(lambda x: (x[0], x[1], x[2]), fact_bcolz.iter())
# %timeit cytoolz.groupby(lambda x: (x[0], x[1], x[2]), fact_bcolz.iter())

shutil.rmtree(rootdir)
