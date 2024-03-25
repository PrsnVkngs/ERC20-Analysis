import cudf

from functions import make_efficient

N = 10
chunk = 1000000

vA: cudf.DataFrame = make_efficient(cudf.read_csv('data/token_transfers_all_encoded.csv'))

vA['date'] = cudf.to_datetime(vA['time_stamp'], unit='s')

print(vA.time_stamp)

vA['date'] = vA['date'].dt.strftime('%Y-%m-%d')

# do the group by once and sample multiple times

def sample_by_day(df):
    return df.groupby('date').apply(lambda x: x.sample(min(N, len(x))))


subset_data = sample_by_day(vA)  # Use a copy to avoid modifying the original
print(len(subset_data))

subset_data.to_csv(f'data/subset_data_{N}.csv', index=False, chunksize=chunk)
