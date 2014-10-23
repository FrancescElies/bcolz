import bcolz
from bcolz import carray_ext
import shutil
import tempfile
import os
import pandas as pd
import numpy as np
import contextlib, time

g_elapsed = 0
@contextlib.contextmanager
def ctime(label=""):
    "Counts the time spent in some context"
    global g_elapsed
    t = time.time()
    yield
    g_elapsed = time.time() - t
    print '--> ', label, round(g_elapsed, 3), "sec\n"

projects = [
{'a1': 'build roads',  'a2': 'CA', 'a3': 2, 'm1':   1, 'm2':  2, 'm3':    3.2},
{'a1': 'fight crime',  'a2': 'IL', 'a3': 1, 'm1':   2, 'm2':  3, 'm3':    4.1},
{'a1': 'help farmers', 'a2': 'IL', 'a3': 1, 'm1':   4, 'm2':  5, 'm3':    6.2},
{'a1': 'help farmers', 'a2': 'CA', 'a3': 3, 'm1':   8, 'm2':  9, 'm3':   10.9},
{'a1': 'build roads',  'a2': 'CA', 'a3': 3, 'm1':  16, 'm2': 17, 'm3':   18.2},
{'a1': 'fight crime',  'a2': 'IL', 'a3': 1, 'm1':  32, 'm2': 33, 'm3':   34.3},
{'a1': 'help farmers', 'a2': 'IL', 'a3': 2, 'm1':  64, 'm2': 65, 'm3':   66.6},
{'a1': 'help farmers', 'a2': 'AR', 'a3': 3, 'm1': 128, 'm2': 129, 'm3': 130.9}
]

groupby_cols = ['a1', 'a2', 'a3']

df = pd.DataFrame(projects * int(1e6))
print 'reference\n', df.groupby(groupby_cols, sort=True).sum()

with ctime("pandas"):
    df.groupby(groupby_cols, sort=True).sum()
elapsed_pandas = g_elapsed
#-----------

prefix = 'bcolz-'
rootdir = tempfile.mkdtemp(prefix=prefix)
# folder should be emtpy
os.rmdir(rootdir)
fact_bcolz = bcolz.ctable.fromdataframe(df, rootdir=rootdir)
fact_bcolz.rootdir
self = fact_bcolz

# this caches the factorizations on-disk directly in the rootdir
fact_bcolz.cache_factor(groupby_cols, refresh=True) 
# does the first 3 parts of the groupby, see the code
print fact_bcolz.groupby(groupby_cols, {})



with ctime("bcolz"):
    fact_bcolz.groupby(groupby_cols, {})
elapsed_bcolz = g_elapsed

print elapsed_bcolz / elapsed_pandas, 'x times slower than pandas'

# %timeit df.groupby(groupby_cols, sort=True).sum()
# %timeit fact_bcolz.cache_factor(groupby_cols, refresh=True) 
# %timeit fact_bcolz.groupby(groupby_cols, {})

# import cytoolz
# cytoolz.groupby(lambda x: (x[0], x[1], x[2]), fact_bcolz.iter())
# %timeit cytoolz.groupby(lambda x: (x[0], x[1], x[2]), fact_bcolz.iter())
# shutil.rmtree(rootdir)


