from contextlib import contextmanager
import numpy as np
import time
import bcolz as bz
from bcolz import carray
from bcolz.carray_ext import test_v1, test_v2, \
    test_v3, test_v4, test_v5, test_v6, test_v7, test_v8

@contextmanager
def ctime(message=None):
    assert message is not None
    "Counts the time spent in some context"
    t = time.time()
    yield
    print message + ":\t", \
          round(time.time() - t, 5), "sec"

n = 2000000 + 3333

c = bz.fromiter((x for x in xrange(n)), count=n, dtype='int64')
print c


# with ctime('test_v1'):
#     r1 = test_v1(c)
# del r1

with ctime('test_v2'):
    r2 = test_v2(c)
del r2

# with ctime('test_v3'):
#     r3 = test_v3(c)
# del r3

# with ctime('test_v4'):
#     r4 = test_v4(c)
# del r4

# with ctime('test_v5'):
#     r5 = test_v5(c)
# del r5

with ctime('test_v6'):
    r6 = test_v6(c)
del r6

with ctime('test_v7'):
    r7 = test_v7(c, num_threads=4)
del r7


# assert np.array_equal(r1, r2)
# assert np.array_equal(r1, r3)
# assert np.array_equal(r1, r4)
# assert np.array_equal(r1, r5)
# assert np.array_equal(r1, r6)
# assert np.array_equal(r1, r7)
