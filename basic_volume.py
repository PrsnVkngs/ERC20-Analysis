import cudf
import cugraph as cugr

import networkx as nx
import matplotlib.pyplot as plt

import sys

contract_address_map = {
    '0xdac17f958d2ee523a2206206994597c13d831ec7': 'usdt',
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'usdc',
    '0x6b175474e89094c44da98b954eedeac495271d0f': 'dai',
    '0xd2877702675e6ceb975b4a1dff9fb7baf4c91ea9': 'luna',
    '0xa47c8bf37f92abed4a126bda807a7b7498661acd': 'ustc',
    '0x8e870d67f660d95d5be530380d0ec0bd388289e1': 'usdp'
}

v1: cudf.DataFrame = cudf.read_csv('data/token_transfers.csv')

# v1['contract_address'] = v1['contract_address'].map(contract_address_map)
# v1['contract_address'] = v1['contract_address'].astype(str)

# unique_addresses = v1['contract_address'].unique()
address_to_int_map = {addr: i for i, addr in enumerate(contract_address_map.keys())}
print(address_to_int_map)

# Replace the contract addresses in the DataFrame with integers
v1['contract_address'] = v1['contract_address'].map(address_to_int_map).astype('int32')

# v1.to_csv('data/token_transfers_encoded.csv', index=False)

# print(v1.contract_address.value_counts())
print(v1['contract_address'].describe())

G_v1 = cugr.MultiGraph(directed=True)
G_v1.from_cudf_edgelist(
    v1,
    source='from_address',
    destination='to_address',
    edge_attr=['value', 'time_stamp', 'contract_address']
)

# print("Done creating MultiGraph, sampling the df.")
# Assuming v1 is already your cuDF DataFrame loaded from 'data/token_transfers.csv'
# Convert cuDF DataFrame to Pandas DataFrame for visualization

v1_betweenness = cugr.betweenness_centrality(G_v1)
v1_betweenness.to_csv('data/betweenness_centrality_1.csv')

sys.exit(0) # skip the rest of the code for now don't need to do this
v1_pd = v1.to_pandas().sample(frac=0.0001, random_state=42)
print("Finished sampling, making the graph")

# Create a NetworkX graph from the Pandas DataFrame
G_nx = nx.from_pandas_edgelist(
    v1_pd,
    source='from_address',
    target='to_address',
    edge_attr=['value', 'time_stamp', 'contract_address'],
    create_using=nx.MultiDiGraph()
)

print("Done creating MultiGraph, making the visualization.")
# Basic visualization with NetworkX and Matplotlib
plt.figure(figsize=(100, 80))
nx.draw_networkx(G_nx, with_labels=False, node_size=15, edge_color='r', alpha=0.3)
plt.show()
