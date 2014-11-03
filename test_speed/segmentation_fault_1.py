import bcolz
import shutil
import tempfile
import os
import pandas as pd
import random, string

INT32 = 2**31

# -- Common inputs for groupby --
N = 100
groupby_cols = ['a1']
agg_list = ['m1', 'm2', 'm3']

def gen_almost_unique_row():
    s = ''.join(random.choice(string.lowercase) for i in range(20))
    d = {
        'a1': random.random(),
        'm1': 1,
        'm2': 2,
        'm3': 3.2
    }
    return d

random.seed(1)
projects = [gen_almost_unique_row() for _ in range(N)]

# -- Pandas --
df = pd.DataFrame(projects)

result_pandas = \
    df.groupby(groupby_cols, as_index=False)[agg_list].sum()


# -- Bcolz --
prefix = 'bcolz-'
rootdir = tempfile.mkdtemp(prefix=prefix)
os.rmdir(rootdir) # folder should be emtpy
fact_bcolz = bcolz.ctable.fromdataframe(df, rootdir=rootdir)
fact_bcolz.flush()

print 'rootdir', fact_bcolz.rootdir

print 'bcolz cache'
fact_bcolz.cache_factor(groupby_cols, refresh=True)

result_bcolz = fact_bcolz.groupby(groupby_cols, agg_list)
print result_bcolz

shutil.rmtree(rootdir)


# [100 rows x 4 columns]
# rootdir /tmp/bcolz-_ZjjBw
# bcolz cache
# `start`+`nitems` out of boundsException RuntimeError: RuntimeError('fatal error during Blosc decompression: -1',) in 'bcolz.carray_ext.chunk._getitem' ignored
# `start`+`nitems` out of boundsException RuntimeError: RuntimeError('fatal error during Blosc decompression: -1',) in 'bcolz.carray_ext.chunk._getitem' ignored
# `start`+`nitems` out of boundsException RuntimeError: RuntimeError('fatal error during Blosc decompression: -1',) in 'bcolz.carray_ext.chunk._getitem' ignored
# `start`+`nitems` out of boundsException RuntimeError: RuntimeError('fatal error during Blosc decompression: -1',) in 'bcolz.carray_ext.chunk._getitem' ignored
# [(0.06588126587027232, 100, 200, 319.9999999999994) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)
#  (0.0, 0, 0, 0.0) (0.0, 0, 0, 0.0)]