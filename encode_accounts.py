import cudf

from functions import make_efficient

chunk = 1000000

v1: cudf.DataFrame = make_efficient(cudf.read_csv('data/token_transfers_1_encoded.csv'))
v2: cudf.DataFrame = make_efficient(cudf.read_csv('data/token_transfers_2_encoded.csv'))
v3: cudf.DataFrame = make_efficient(cudf.read_csv('data/token_transfers_3_encoded.csv'))

vA: cudf.DataFrame = cudf.concat([v1, v2, v3], ignore_index=True)

print(len(vA))

address_set = set(vA['to_address'].unique().to_pandas().tolist())
address_set.update(vA['from_address'].unique().to_pandas().tolist())

unique_address_map: dict[str: int] = {value: item for item, value in enumerate(address_set)}

print(len(unique_address_map))


# Encoding (using the new mapping)
def encode_address(address: str) -> int:
    return unique_address_map[address] if address in unique_address_map else -1


def process_in_chunks(series, chunksize, func):
    """Processes a cuDF Series in chunks and applies a function.

  Args:
      series: The cuDF Series to process.
      chunksize: The size of each chunk to process.
      func: The function to apply to each chunk.

  Returns:
      A new cuDF Series with the results of applying the function to each chunk.
  """
    results = []
    for i in range(0, len(series), chunksize):
        chunk_s = series.iloc[i:i + chunksize]
        results.append(chunk_s.astype('string').map(unique_address_map))
    return cudf.concat(results)


print('encoding whole data set')
# Apply the encoding to your original columns
vA['from_address'] = process_in_chunks(vA['from_address'], chunk, encode_address)
vA['to_address'] = process_in_chunks(vA['to_address'], chunk, encode_address)

print('encoding first file')
v1['from_address'] = process_in_chunks(v1['from_address'], chunk, encode_address)
v1['to_address'] = process_in_chunks(v1['to_address'], chunk, encode_address)

print('encoding second file')
v2['from_address'] = process_in_chunks(v2['from_address'], chunk, encode_address)
v2['to_address'] = process_in_chunks(v2['to_address'], chunk, encode_address)

print('encoding third file')
v3['from_address'] = process_in_chunks(v3['from_address'], chunk, encode_address)
v3['to_address'] = process_in_chunks(v3['to_address'], chunk, encode_address)

vA.to_csv('data/token_transfers_all_encoded.csv', index=False, chunksize=chunk)

v1.to_csv('data/token_transfers_1a_encoded.csv', index=False, chunksize=chunk)
v2.to_csv('data/token_transfers_2a_encoded.csv', index=False, chunksize=chunk)
v3.to_csv('data/token_transfers_3a_encoded.csv', index=False, chunksize=chunk)
