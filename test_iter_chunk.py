from contextlib import contextmanager
import numpy as np
import time
import bcolz as bz
from bcolz import carray
from bcolz.carray_ext import test_v1, test_v2, \
    test_v3, test_v4, test_v5, test_v6, test_v7, test_v8, test_v9

@contextmanager
def ctime(message=None):
    assert message is not None
    "Counts the time spent in some context"
    t = time.time()
    yield
    print message + ":\t", \
          round(time.time() - t, 5), "sec"

n = 105

c = bz.fromiter((x for x in xrange(n)), count=n, dtype='int64', chunklen=10)
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
    r7 = test_v7(c, n_threads=4)

# with ctime('test_v8'):
#     r8 = test_v8(c)


print test_v9(c)
# assert np.array_equal(r1, r2)
