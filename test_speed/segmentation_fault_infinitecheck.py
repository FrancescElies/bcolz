import bcolz
import shutil
import tempfile
import os
import pandas as pd
import random, string
import numpy as np
import itertools
import pandas as pd
from nose.tools import assert_equal

#-----------------------------------------------
# -- Common inputs for groupby --
groupby_cols = ['f0','f1']
agg_list = ['f2','f3','f4']
#-----------------------------------------------

def gen_almost_unique_row(N):
    pool = itertools.cycle(['a','b'])
    pool_b = itertools.cycle([1, 2, 3])
    for _ in range(N):
        d = (
            pool.next(),
            pool_b.next(),
            random.randint(-100000, 100000),
            random.randint(-100000, 100000),
            random.random(),
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
    a = np.fromiter(g, dtype='S1,i8,i4,i8,f8')
    fact_bcolz = bcolz.ctable(a, rootdir=rootdir)
    fact_bcolz.flush()
    print 'rootdir', fact_bcolz.rootdir

    print 'bcolz cache'
    fact_bcolz.cache_factor(groupby_cols, refresh=True)

    result_bcolz = fact_bcolz.groupby(groupby_cols, agg_list)
    print result_bcolz

    print('--> Result Pandas')
    df = pd.DataFrame(data=a)
    result_pandas = df.groupby(groupby_cols, as_index=False)[agg_list].sum()
    print result_pandas

    for n, row in enumerate(result_bcolz):
        # results might not appear in the same order
        tmp = result_pandas.loc[(result_pandas.f0 == row.f0) &
                                (result_pandas.f1 == row.f1) &
                                (result_pandas.f2 == row.f2) &
                                (result_pandas.f3 == row.f3) &
                                (result_pandas.f4 == row.f4)]

        assert_equal(row.f0, tmp.f0.values[0])
        assert_equal(row.f1, tmp.f1.values[0])
        assert_equal(row.f2, tmp.f2.values[0])
        assert_equal(row.f3, tmp.f3.values[0])
        assert_equal(row.f4, tmp.f4.values[0])

    shutil.rmtree(rootdir)
    print '--------------------'

if __name__ == '__main__':
    for N in range(int(1e8)):
        run(N)