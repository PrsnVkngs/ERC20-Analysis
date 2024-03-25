import cudf

contract_address_map = {
    '0xdac17f958d2ee523a2206206994597c13d831ec7': 'usdt',
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'usdc',
    '0x6b175474e89094c44da98b954eedeac495271d0f': 'dai',
    '0xd2877702675e6ceb975b4a1dff9fb7baf4c91ea9': 'luna',
    '0xa47c8bf37f92abed4a126bda807a7b7498661acd': 'ustc',
    '0x8e870d67f660d95d5be530380d0ec0bd388289e1': 'usdp'
}

chunk = 500000

v1: cudf.DataFrame = cudf.read_csv('data/token_transfers_V1.0.0.csv')

# v1['contract_address'] = v1['contract_address'].map(contract_address_map)
# v1['contract_address'] = v1['contract_address'].astype(str)

# unique_addresses = v1['contract_address'].unique()
address_to_int_map = {addr: i for i, addr in enumerate(contract_address_map.keys())}
print(address_to_int_map)

# Replace the contract addresses in the DataFrame with integers
v1['contract_address'] = v1['contract_address'].map(address_to_int_map).astype('int8')

# print(v1['contract_address'])

v1.to_csv('data/token_transfers_1_encoded.csv', index=False, chunksize=chunk)
