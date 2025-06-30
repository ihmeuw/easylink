# STEP_NAME: links_to_clusters
# REQUIREMENTS: pandas pyarrow networkx

import os
from pathlib import Path

import networkx as nx
import pandas as pd

links = pd.read_parquet(os.environ["LINKS_FILE_PATH"])
output_path = Path(os.environ["OUTPUT_PATHS"])

no_duplicates_dataset = os.environ["NO_DUPLICATES_DATASET"]
break_ties_method = os.getenv("BREAK_TIES_METHOD", "drop")

left_no_duplicates_dataset = links["Left Record Dataset"] == no_duplicates_dataset
right_no_duplicates_dataset = links["Right Record Dataset"] == no_duplicates_dataset

if (left_no_duplicates_dataset & right_no_duplicates_dataset).any():
    raise ValueError(
        f"Provided links include links within the no_duplicates_dataset ({no_duplicates_dataset})"
    )

if not (left_no_duplicates_dataset | right_no_duplicates_dataset).all():
    raise ValueError(
        f"Provided links include links that don't involve the no_duplicates_dataset ({no_duplicates_dataset})"
    )

# Get the no-duplicates dataset all on the right
id_cols = [
    "Left Record Dataset",
    "Left Record ID",
    "Right Record Dataset",
    "Right Record ID",
]
switched_id_cols = [
    "Right Record Dataset",
    "Right Record ID",
    "Left Record Dataset",
    "Left Record ID",
]
links.loc[left_no_duplicates_dataset, id_cols] = links.loc[
    left_no_duplicates_dataset, switched_id_cols
].to_numpy()
links[["Left Record ID", "Right Record ID"]] = links[
    ["Left Record ID", "Right Record ID"]
].astype(int)

links["Left Record Key"] = (
    links["Left Record Dataset"] + "-__-" + links["Left Record ID"].astype(int).astype(str)
)
links["Right Record Key"] = (
    links["Right Record Dataset"] + "-__-" + links["Right Record ID"].astype(int).astype(str)
)

links_to_accept = (
    links[links["Probability"] >= float(os.environ["THRESHOLD_MATCH_PROBABILITY"])]
    # Pre-emptively break probability ties by right record key for the highest_id method
    .sort_values(["Probability", "Right Record Key"], ascending=False)
    # No duplicates in the *right* means only one link per *left* record
    .groupby(["Left Record Key"]).first()
)

if break_ties_method == "drop":
    num_tied = (
        links_to_accept.merge(links, on=["Left Record Key", "Probability"])
        .groupby(["Left Record Key"])
        .size()
    )
    print("Ties:")
    print(num_tied)
    print(num_tied.describe())
    links_to_accept = links_to_accept[num_tied == 1]
elif break_ties_method == "highest_id":
    # Done above pre-emptively
    pass
else:
    raise ValueError(f"Unknown break_ties_method {break_ties_method}")

# NOTE: We only include nodes involved in an accepted link in our cluster.
# If a node isn't involved in an accepted link, that could just represent
# that we haven't evaluated the right pairs involving it, not confidence that
# it is a singleton.
G = nx.from_pandas_edgelist(
    links_to_accept.reset_index()[["Left Record Key", "Right Record Key"]].rename(
        columns={"Left Record Key": "source", "Right Record Key": "target"}
    )
)

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

merged_df[["Input Record Dataset", "Input Record ID", "Cluster ID"]].to_parquet(output_path)
