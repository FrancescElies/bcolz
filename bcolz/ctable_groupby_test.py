# accounts = [
#     (1, 'Alice',   100, 'F', 'AMS',  1),  # id, name, balance, gender
#     (2, 'Bob',     200, 'M', 'AMS',  2),
#     (3, 'Charlie', 150, 'M', 'ROT',  4),
#     (4, 'Dennis',   50, 'M', 'AMS',  8),
#     (5, 'Edith',   300, 'F', 'AMS', 16),
# ]
#
# from toolz import groupby, valmap, compose
# from toolz.curried import get, pluck
#
# binop = lambda total, (id, name, bal, gend): total + bal
#
# gb = groupby(get(3), accounts)
# # {
# #  'F': [(1, 'Alice', 100, 'F'), (5, 'Edith', 300, 'F')],
# #  'M': [(2, 'Bob', 200, 'M'), (3, 'Charlie', 150, 'M'), (4, 'Dennis', 50, 'M')]
# # }
# valmap(compose(sum, pluck(2)), gb)
# # {'F': 400, 'M': 400}
#
# pipe(accounts, groupby(get(3)), valmap(compose(sum, pluck(2))))
#
# # -----
#
# import pandas as pd
# df = pd.DataFrame({'col1':['A','A','A','B','B','B'], 'col2':['C','D','D','D','C','C'], 'col3':[.1,.2,.4,.6,.8,1]})
# def func3(df):
#     dfout =  sum(df['col3']**2)
#     return  dfout
# t = df.groupby(['col1', 'col2']).apply(func3)
#
# # -----------------------
#
# from random import randint
# import numpy as np
# A = np.array([1.1, 1.1, 3.3, 3.3, 5.5, 6.6])
# B = np.array([111, 222, 222, 333, 333, 777])
# C = np.array([511, 522, 522, 533, 533, 577])
#
# df = pd.DataFrame(zip(A, B, C), columns=['A', 'B', 'C'])
# df.set_index(['A', 'B'], inplace=True)
#
# result_df = df.loc[(df.index.get_level_values('A') > 1.7) & (df.index.get_level_values('B') < 666)]

# ------

import bcolz
import tempfile
import os
import pandas as pd

projects = [
    {'name': 'build roads',  'state': 'CA', 'cost': 1000000, 'cost2': 1000000},
    {'name': 'fight crime',  'state': 'IL', 'cost': 100000,  'cost2': 100000},
    {'name': 'help farmers', 'state': 'IL', 'cost': 2000000, 'cost2': 2000000},
    {'name': 'help farmers', 'state': 'CA', 'cost': 200000,  'cost2': 200000},
    {'name': 'build roads',  'state': 'CA', 'cost': 9000000, 'cost2': 199900},
    {'name': 'fight crime',  'state': 'IL', 'cost': 900000,  'cost2': 19990},
    {'name': 'help farmers', 'state': 'IL', 'cost': 9000000, 'cost2': 299900},
    {'name': 'help farmers', 'state': 'AR', 'cost': 900000,  'cost2': 29990}
]

df_tmp = pd.DataFrame(projects)
df = [df_tmp for i in range(100000)]
df = pd.concat(df, ignore_index=True)
print df

prefix = 'bcolz-TestH5'
rootdir = tempfile.mkdtemp(prefix=prefix)
os.rmdir(rootdir)  # tests needs this cleared
print(rootdir)
fact_bcolz = bcolz.ctable.fromdataframe(df, rootdir=rootdir)
print '--'
print fact_bcolz.groupby(['state','name'], ['cost', 'cost2'])
print '--'
# fact_bcolz.groupby(['state'],{'sum_costs':['sum', 'cost', 'cost2']})

# %timeit groupby_cython2(fact_bcolz, ['state'], ['cost', 'cost2'])
