import bcolz
from bcolz import carray_ext
import shutil
import tempfile
import os
import pandas as pd
import numpy as np
import contextlib, time
import random
import logging
logging.basicConfig(filename='example.log',level=logging.CRITICAL)

random.seed(1)

# -- Context manager --
g_elapsed = 0
@contextlib.contextmanager
def ctime(label=""):
    "Counts the time spent in some context"
    global g_elapsed
    t = time.time()
    yield
    g_elapsed = time.time() - t
    print '--> ', label, round(g_elapsed, 3), "sec\n"


# -- Common inputs for groupby --
# a1: dtype string
# a2: dtype float64
# a3: dtype int32
# a4: dtype int64
attr_values = {
    0 : ['AAA', 'BBB', 'CCC'],
    1 : [11.1, 22.2, 33.3],
    2 : [1, 2, 3],
    3 : [1000000000, 2000000000, 3000000000],
}

def gen_row(num_attr_cols=10, num_meas_cols=3):
    d = {}

    for n in range(num_attr_cols):
        d['a' + str(n)] = random.choice(attr_values[n % len(attr_values)])
    for n in range(num_meas_cols):
        d['m' + str(n)] = random.random()

    yield d

# -- size dataframe --
N_rows = int(1e4)
K_attr_cols = 2000  # min value 4

# -- groupby parameters --
groupby_cols = ['a' + str(n) for n in range(K_attr_cols / 2)]
agg_list = ['m0', 'm1', 'm2']

# -- Pandas --
projects = [ gen_row(num_attr_cols=K_attr_cols).next() for _ in range(N_rows) ]
df = pd.DataFrame(projects)
# force dtypes int32 for certain groupby columns
df.a3 = df.a3.astype(np.int32)
df.m1 = df.m1.astype(np.int32)

logging.info( '--> Pandas result reference' )
logging.info( df.groupby(groupby_cols, sort=True)[agg_list].sum() )
with ctime("Pandas groupby"):
    df.groupby(groupby_cols, sort=True).sum()

elapsed_pandas = g_elapsed

# -- Bcolz --
prefix = 'bcolz-'
rootdir = tempfile.mkdtemp(prefix=prefix)
os.rmdir(rootdir) # folder should be emtpy
fact_bcolz = bcolz.ctable.fromdataframe(df, rootdir=rootdir)
fact_bcolz.rootdir

# this caches the factorizations on-disk directly in the rootdir
with ctime("Bcolz caching"):
    fact_bcolz.cache_factor(groupby_cols, refresh=True)

# does the first 3 parts of the groupby, see the code
logging.info( '--> Bcolz' )
logging.info( fact_bcolz.groupby(groupby_cols, agg_list) )
with ctime("Bcolz groupby"):
    fact_bcolz.groupby(groupby_cols, agg_list)

elapsed_bcolz = g_elapsed

print round(elapsed_bcolz / elapsed_pandas, 3), 'x times slower than pandas'

# -- test speed in Ipython --
# %timeit fact_bcolz.groupby(['a1', 'a2', 'a3'], ['m1', 'm2'])
# %timeit df.groupby(['a1', 'a2', 'a3'], as_index=False)['m1', 'm2'].sum()


# import cytoolz
# cytoolz.groupby(lambda x: (x[0], x[1], x[2]), fact_bcolz.iter())
# %timeit cytoolz.groupby(lambda x: (x[0], x[1], x[2]), fact_bcolz.iter())

shutil.rmtree(rootdir)
