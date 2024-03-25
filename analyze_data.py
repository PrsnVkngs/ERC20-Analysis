import cudf

from functions import make_efficient

v1: cudf.DataFrame = cudf.read_csv('data/subset_data_10.csv')

print(v1.head())

print(v1.dtypes)

print('-------------before---------------------\n')
print(v1.memory_usage(deep=True))

v1 = make_efficient(v1)

print('-------------after---------------------\n')
print(v1.memory_usage(deep=True))

print(v1.head())
