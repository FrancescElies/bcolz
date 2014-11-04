rm bcolz/carray_ext.c
rm bcolz/carray_ext.so 
rm bcolz/ctable_ext.c
rm bcolz/ctable_ext.so
python setup.py build_ext --inplace 
python setup.py install
