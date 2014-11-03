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

def gen_almost_unique_row(N):
    pool = itertools.cycle(['a','b'])
    pool_b = itertools.cycle([1.1, 1.2])
    pool_c = itertools.cycle([1, 2, 3])
    pool_d = itertools.cycle([1, 2, 3])
    for _ in range(N):
        d = (
            pool.next(),
            pool_b.next(),
            pool_c.next(),
            pool_d.next(),
            random.random(),
            random.randint(-500, 500),
            random.randint(-500, 500),
        )
        yield d

def run(N, groupby_cols=None, agg_list=None):
    assert groupby_cols is not None
    assert  agg_list is not None

    print'N =', N
    random.seed(1)
    g = gen_almost_unique_row(N)

    # -- Bcolz --
    prefix = 'bcolz-'
    rootdir = tempfile.mkdtemp(prefix=prefix)
    os.rmdir(rootdir) # folder should be emtpy
    a = np.fromiter(g, dtype='S1,f8,i8,i4,f8,i8,i4')
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

if __name__ == '__main__':
    inputs = [10, 1000, 8191, 8192, 8193, 10000, 100000, 1000000,]
    for num_rows in inputs:
        run(num_rows,
            groupby_cols=['f0'],
            agg_list=['f1', 'f3'])
        run(num_rows,
            groupby_cols=['f0', 'f1'],
            agg_list=['f4', 'f5', 'f6'])
        run(num_rows,
            groupby_cols=['f0', 'f1', 'f2'],
            agg_list=['f4', 'f5', 'f6'])
        run(num_rows,
            groupby_cols=['f0', 'f1', 'f2', 'f3'],
            agg_list=['f4', 'f5', 'f6'])