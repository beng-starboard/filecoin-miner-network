from prefect import flow, task
import pandas as pd
import numpy as np
import networkx as nx
import itertools
from tqdm import tqdm
from scipy import stats
from geolite2 import geolite2 

def countries_in_subgraph(G, subgraph_idx):
    '''
    Given a NetworkX subgraph, generates the list of countries seen. 

    Parameters
    ----------
    G (nx.Graph): the original graph
    subgraph_idx (int): index number of the graph component

    Returns
    -------
    num_nodes (int): number of nodes in the subgraph
    all_countries (list): list of all countries seen in the subgraph
    '''
    subgraphs = [nx.subgraph(G,c) for c in nx.connected_components(G)]
    k = subgraphs[subgraph_idx]
    # k = subgraph_list[subgraph_idx]
    all_countries = []
    num_nodes = len(k.nodes)
    for node in k.nodes(data=True):
        try:
            if str(node[1]['country']) != 'nan':
                all_countries.append(node[1]['country'])
        except:
            pass
    return num_nodes, all_countries

@task
def generate_df_graph_from_file(file_dir, num_components=1000):

    '''
    Generates a NetworkX graph and a dataframe, both with location data, from a given CSV file.
    The network is in general sparse and not fully connected. Therefore this function will by 
    default only choose the top num_components subgraphs (in terms of connections) and thus drop
    all small subgraphs. 

    Parameters
    ----------
    file_dir (str): location of the raw datafile
    num_components (int): the number of connected components to keep. 

    Returns
    -------
    data (pd.DataFrame): dataframe of the raw file
    G (nx.Graph): NetworkX graph
    '''

    geo = geolite2.reader()

    def extract_country(x):
        out = np.nan
        try:
            out = geo.get(x)['country']['iso_code']
        except:
            pass
        return out

    # 0b. Load worker relationship data
    data = pd.read_csv(file_dir, index_col=[0])
    dsplt = data['multi_addresses'].str.split('/', expand=True)
    dsplt = dsplt[dsplt.columns[2]]
    data['ip'] = dsplt
    del dsplt

    # 0c. Extract country
    data['country'] = data['ip'].apply(extract_country)

    # 1. Load network
    # 1a. Add edges
    G = nx.Graph()
    for m,c in zip(data.miner_id, data.country):
        G.add_node(m, country=c)
    G.add_edges_from([mi, wi] for mi, wi in data[['miner_id', 'worker_id']].drop_duplicates().values)
    G.add_edges_from([mi, oi] for mi, oi in data[['miner_id', 'owner_id']].drop_duplicates().values)

    # 1b. Remove small components
    small_components = sorted(nx.connected_components(G), key=len)[:-num_components]
    G.remove_nodes_from(itertools.chain.from_iterable(small_components))

    return data, G


@task
def generate_geographic_lookup_df(G):

    '''
    Generates a lookup table connecting every miner to a known and an implied location based on
    network connectivity. 
    '''

    subgraphs = [nx.subgraph(G,c) for c in nx.connected_components(G)]
    number_of_subgraphs = len(subgraphs)
    n_node_list = []
    mode_country_list = []
    n_geolocated_list = []

    t = tqdm(range(number_of_subgraphs))
    count = 0
    for n in t:
        t.set_description('Generating subgraph %s'%count)
        count += 1
        n_nodes, all_c = countries_in_subgraph(G, n)
        n_node_list.append(n_nodes)
        if len(all_c) > 0:
            mode_country_list.append(stats.mode(all_c).mode[0])
        else: 
            mode_country_list.append('None')
        n_geolocated_list.append(len(all_c))

    geo_df = pd.DataFrame(np.array([n_node_list, mode_country_list, n_geolocated_list]).T, 
                        columns=['num_nodes', 'mode_country', 'num_geolocated_nodes'])    
    geo_df.num_geolocated_nodes = pd.to_numeric(geo_df.num_geolocated_nodes)
    geo_df.num_nodes = pd.to_numeric(geo_df.num_nodes)

    return geo_df

@flow
def workflow(filename, n_components=800):
    df, G = generate_df_graph_from_file(filename)
    geo_df = generate_geographic_lookup_df(G)
    geo_df.to_csv('data/output.csv')

workflow('data/worker_relationship_jun_20.csv')
