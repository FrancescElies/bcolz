import os
import tempfile
import bcolz
import pandas as pd


file_h5='pvm_fact_nielsen_data.h5'

hdf = pd.HDFStore(file_h5, mode='r')

fact_df = hdf['/store_0'][ [ 'a_n_11', 'a_n_101' , 'a_n_21' , 'a_n_31', 'm__n_1001'] ]
prefix = 'bcolz-TestH5'
rootdir = tempfile.mkdtemp(prefix=prefix)
os.rmdir(rootdir)  # tests needs this cleared
print(rootdir)
fact_bcolz = bcolz.ctable.fromdataframe(fact_df, rootdir=rootdir)
fact_bcolz.flush()

%timeit fact_df.groupby(['a_n_11', 'a_n_21'], as_index=False)['m101101'].sum()
%timeit fact_bcolz.groupby(['a_n_11', 'a_n_21'], ['m101101'], method=2)

fact_bcolz.cache_factor([ 'a_n_11', 'a_n_101' , 'a_n_21' , 'a_n_31'])

%prun -l 20 -s cumulative fact_bcolz.groupby(['a_n_11', 'a_n_21'], ['m101101'], method=2)