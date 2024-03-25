import cudf
import cugraph as cugr

import networkx as nx
from matplotlib import pyplot as plt

from functions import make_efficient

int_cols = ['block_number', 'transaction_index', 'from_address', 'to_address', 'time_stamp', 'contract_address']

# v1 = make_efficient(cudf.read_csv('data/subset_data_10.csv').drop('date', axis=1))
v1: cudf.DataFrame = cudf.read_csv('data/subset_data_10.csv').drop('date', axis=1)
print(v1.dtypes)

# cast all integer columns to int32
for col in int_cols:
    v1[col] = v1[col].astype('int32')

print(v1.dtypes)

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
v1_betweenness.to_csv('data/betweenness_centrality_10s.csv')

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
plt.figure(figsize=(20, 20))
nx.draw_networkx(G_nx, with_labels=False, node_size=15, edge_color='r', alpha=0.3)

plt.savefig('figures/graph_10s.png')
