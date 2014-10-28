import os
import tempfile
import bcolz
import pandas as pd
import contextlib
import time
from nose.tools import assert_equal, assert_almost_equals

@contextlib.contextmanager
def ctime(label=""):
    "Counts the time spent in some context"
    global g_elapsed
    t = time.time()
    yield
    g_elapsed = time.time() - t
    print label, round(g_elapsed, 3), "sec\n"


file_h5='/srv/hdf5/fc_nl_fact_ex_factory_actuals_2.h5'

hdf = pd.HDFStore(file_h5, mode='r')

fact_df = hdf['/store_0'][ [ 'a_n_11', 'a_n_101' , 'a_n_21' , 'a_n_31', 'm101101'] ]
prefix = 'bcolz-TestH5'
rootdir = tempfile.mkdtemp(prefix=prefix)
os.rmdir(rootdir)  # tests needs this cleared
print(rootdir)
fact_bcolz = bcolz.ctable.fromdataframe(fact_df, rootdir=rootdir)
fact_bcolz.flush()

with ctime("--> Pandas groupby"):
    result_pandas = \
        fact_df.groupby(['a_n_11', 'a_n_21'], as_index=False)['m101101'].sum()
elapsed_pandas = g_elapsed

with ctime("--> Bcolz caching"):
    fact_bcolz.cache_factor(['a_n_11', 'a_n_21'], refresh=True)

with ctime("--> Bcolz groupby"):
    result_bcolz = \
        fact_bcolz.groupby(['a_n_11', 'a_n_21'], ['m101101'])
elapsed_bcolz = g_elapsed

print round(elapsed_bcolz / elapsed_pandas, 3), 'x times slower than pandas'

print('--> Check correctness of the result vs pandas, if nothing printed is fine')
assert_equal(result_bcolz.len, len(result_pandas))
for n, row in enumerate(result_bcolz):
    # results might not appear in the same order
    tmp = result_pandas.loc[(result_pandas.a_n_11 == row.a_n_11) &
                            (result_pandas.a_n_21 == row.a_n_21)]

    assert_equal(row.a_n_11, tmp.a_n_11.values[0])
    assert_equal(row.a_n_21, tmp.a_n_21.values[0])
    assert_equal(row.m101101, tmp.m101101.values[0])

hdf.close()
# -------
# fact_bcolz.cache_factor([ 'a_n_11', 'a_n_101' , 'a_n_21' , 'a_n_31'])
#
# %prun -l 20 -s cumulative fact_bcolz.groupby(['a_n_11', 'a_n_21'], ['m101101'])
#
# x = bcolz.ctable(rootdir='pvm_fact_nielsen_data.bcolz', mode='r')
# x.groupby([ 'a_n_11'], ['m_n_1001'])
