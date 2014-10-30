import bcolz
import shutil
import tempfile
import os
import pandas as pd
import random, string
import numpy as np
import itertools
import pandas as pd

#-----------------------------------------------
# -- Common inputs for groupby --
groupby_cols = ['f0']
agg_list = ['f1','f3']
#-----------------------------------------------

def gen_almost_unique_row(N):
    pool = itertools.cycle(['a','b'])
    pool_b = itertools.cycle([1, 2, 3])
    pool_c = itertools.cycle([1.1, 1.2])
    for _ in range(N):
        d = (
            pool.next(),
            pool_b.next(),
            pool_c.next(),
            1,
            1,
            1.1,
        )
        yield d

def run(N):
    print'N =', N
    random.seed(1)
    g = gen_almost_unique_row(N)

    # -- Bcolz --
    prefix = 'bcolz-'
    rootdir = tempfile.mkdtemp(prefix=prefix)
    os.rmdir(rootdir) # folder should be emtpy
    a = np.fromiter(g, dtype='S1,i8,f8,i4,i8,f8')
    fact_bcolz = bcolz.ctable(a, rootdir=rootdir)
    fact_bcolz.flush()
    print 'rootdir', fact_bcolz.rootdir

    print 'bcolz cache'
    fact_bcolz.cache_factor(groupby_cols, refresh=True)

    result_bcolz = fact_bcolz.groupby(groupby_cols, agg_list)
    print result_bcolz

    print('--> Result Pandas')
    df = pd.DataFrame(data=a)
    print df.groupby(groupby_cols, as_index=False)[agg_list].sum()

    shutil.rmtree(rootdir)
    print '---------------'

run(10)
run(1000)
run(8191)
run(8192)
run(8193)
run(10000)
run(1000000)
