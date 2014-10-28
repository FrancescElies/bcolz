import os
import tempfile
import bcolz
import pandas as pd
import contextlib
import time

@contextlib.contextmanager
def ctime(label=""):
    "Counts the time spent in some context"
    global g_elapsed
    t = time.time()
    yield
    g_elapsed = time.time() - t
    print '--> ', label, round(g_elapsed, 3), "sec\n"

file_h5='/srv/hdf5/pvm_fact_nielsen_data.h5'

hdf = pd.HDFStore(file_h5, mode='r')

fact_df = hdf['/store_0'][ [ 'a_n_11', 'a_n_21' , 'a_n_31', 'm_n_1001'] ]
prefix = 'bcolz-TestH5'
rootdir = tempfile.mkdtemp(prefix=prefix)
os.rmdir(rootdir)  # tests needs this cleared
print(rootdir)
fact_bcolz = bcolz.ctable.fromdataframe(fact_df, rootdir=rootdir)
fact_bcolz.flush()

with ctime("Pandas groupby"):
    fact_df.groupby(['a_n_11', 'a_n_21'], as_index=False)['m_n_1001'].sum()

with ctime("Bcolz groupby"):
    fact_bcolz.groupby(['a_n_11', 'a_n_21'], ['m_n_1001'])


fact_bcolz.cache_factor([ 'a_n_11', 'a_n_101' , 'a_n_21' , 'a_n_31'])

%prun -l 20 -s cumulative fact_bcolz.groupby(['a_n_11', 'a_n_21'], ['m101101'], method=2)

x = bcolz.ctable(rootdir='pvm_fact_nielsen_data.bcolz', mode='r')
x.groupby([ 'a_n_11'], ['m_n_1001'])
