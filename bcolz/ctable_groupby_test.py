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

from prettyprint import pp
import bcolz
import tempfile
import os
import pandas as pd

projects = [
    {'name': 'build roads',  'state': 'CA', 'cost':   1, 'cost2':  2, 'cost3':    3},
    {'name': 'fight crime',  'state': 'IL', 'cost':   2, 'cost2':  3, 'cost3':    4},
    {'name': 'help farmers', 'state': 'IL', 'cost':   4, 'cost2':  5, 'cost3':    6},
    {'name': 'help farmers', 'state': 'CA', 'cost':   8, 'cost2':  9, 'cost3':   10},
    {'name': 'build roads',  'state': 'CA', 'cost':  16, 'cost2': 17, 'cost3':   18},
    {'name': 'fight crime',  'state': 'IL', 'cost':  32, 'cost2': 33, 'cost3':   34},
    {'name': 'help farmers', 'state': 'IL', 'cost':  64, 'cost2': 65, 'cost3':   66},
    {'name': 'help farmers', 'state': 'AR', 'cost': 128, 'cost2': 129, 'cost3': 130}
]

df = pd.DataFrame(columns=projects[0].keys())
for i, item in enumerate(projects):
    df.loc[i] = item.values()

print df
prefix = 'bcolz-TestH5'
rootdir = tempfile.mkdtemp(prefix=prefix)
os.rmdir(rootdir)  # tests needs this cleared
fact_bcolz = bcolz.ctable.fromdataframe(df, rootdir=rootdir)
print '--'
res1 = fact_bcolz.groupby(
    ['state'],
    [{'sum_costs':{'sum': ['cost', 'cost2']}}]
)
pp(res1)
print '--'

res2 = fact_bcolz.groupby(
    ['state', 'name'],
    [
        {'sum_cost1':{'sum': ['cost']}},
        {'sum_cost2':{'sum': ['cost', 'cost2']}},
        {'sum_costs':{'sum': ['cost', 'cost2', 'cost3']}}
    ]
)

pp(res2)
