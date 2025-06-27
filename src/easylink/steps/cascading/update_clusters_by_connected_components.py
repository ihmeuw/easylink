# STEP_NAME: updating_clusters

# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml networkx

# PIPELINE_SCHEMA: main

import logging
import os
from pathlib import Path

import networkx as nx
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[logging.StreamHandler()],
)


def load_file(file_path, file_format=None):
    logging.info(f"Loading file {file_path} with format {file_format}")
    if file_format is None:
        file_format = file_path.split(".")[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    raise ValueError(f"Unknown file format {file_format}")


# LOAD INPUTS and SAVE OUTPUTS

# NEW_CLUSTERS_FILE_PATH is a path to a single file
new_clusters_filepath = os.environ["NEW_CLUSTERS_FILE_PATH"]

# KNOWN_CLUSTERS_AND_MAYBE_INPUT_DATASETS_FILE_PATHS is a list of file paths.
# There is one item in it that is a file with "clusters" in the filename.
# That's the only item we're interested in here.
# The other items may be there if this is coming from the user's input,
# due to our workaround for only having one slot of user input.
known_clusters_filepaths = [
    path
    for path in os.environ["KNOWN_CLUSTERS_AND_MAYBE_INPUT_DATASETS_FILE_PATHS"].split(",")
    if "clusters" in Path(path).stem
]
if len(known_clusters_filepaths) > 1:
    raise ValueError("Multiple known clusters files found")
if len(known_clusters_filepaths) == 0:
    raise ValueError("No known clusters file found")

known_clusters_filepath = known_clusters_filepaths[0]
known_clusters_df = load_file(known_clusters_filepath)

# OUTPUT_PATHS is a path to a single file (clusters.parquet)
results_filepath = os.environ["OUTPUT_PATHS"]
Path(results_filepath).parent.mkdir(exist_ok=True, parents=True)

new_clusters_df = load_file(new_clusters_filepath)


def merge_clusters(known_clusters_df, new_clusters_df):
    # Combine both dataframes
    combined_df = pd.concat(
        [
            # Ensure cluster names are unique
            known_clusters_df.assign(
                **{"Cluster ID": lambda df: "known__" + df["Cluster ID"].astype(str)}
            ),
            new_clusters_df.assign(
                **{"Cluster ID": lambda df: "new__" + df["Cluster ID"].astype(str)}
            ),
        ],
        ignore_index=True,
    )
    combined_df["Input Record Key"] = (
        combined_df["Input Record Dataset"]
        + "-__-"
        + combined_df["Input Record ID"].astype(int).astype(str)
    )

    # Group by Cluster ID to get connected records
    cluster_groups = combined_df.groupby("Cluster ID")["Input Record Key"].apply(list)

    # Build a graph of all connections implied by cluster IDs
    G = nx.Graph()
    for group in cluster_groups:
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                G.add_edge(group[i], group[j])

    # Add isolated nodes (records with unique clusters)
    all_keys = set(combined_df["Input Record Key"])
    G.add_nodes_from(all_keys)

    # Compute connected components
    components = list(nx.connected_components(G))

    # Assign new cluster IDs
    merged_data = []
    for cluster_id, records in enumerate(components, start=1):
        for record_key in records:
            merged_data.append((record_key, cluster_id))

    # Build the final DataFrame
    merged_df = pd.DataFrame(merged_data, columns=["Input Record Key", "Cluster ID"])

    merged_df[["Input Record Dataset", "Input Record ID"]] = (
        merged_df["Input Record Key"].str.split("-__-", n=1, expand=True)
        if not merged_df.empty
        else pd.DataFrame(columns=["Input Record Dataset", "Input Record ID"])
    )

    merged_df["Input Record ID"] = merged_df["Input Record ID"].astype(int)

    return merged_df[["Input Record Dataset", "Input Record ID", "Cluster ID"]]


output_df = merge_clusters(known_clusters_df, new_clusters_df)

logging.info(
    f"Writing output for dataset from input {new_clusters_filepath} to {results_filepath}"
)
output_df.to_parquet(results_filepath)
