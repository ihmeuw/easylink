# STEP_NAME: clusters_to_links

# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

# PIPELINE_SCHEMA: main

import logging
import os

import pandas as pd
from itertools import combinations

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[logging.StreamHandler()],
)
diagnostics = {}


def load_file(file_path, file_format=None):
    if file_format is None:
        file_format = file_path.split(".")[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    raise ValueError()


def clusters_to_links(clusters_df):
    # Group by Cluster ID and collect Record IDs for each cluster
    grouped = clusters_df.groupby("Cluster ID")["Input Record ID"].apply(list)

    # Generate all unique pairs of Record IDs within each cluster
    links = []
    for record_ids in grouped:
        links.extend(combinations(sorted(record_ids), 2))

    # Create a DataFrame for the links
    links_df = pd.DataFrame(links, columns=["Left Record ID", "Right Record ID"])
    links_df["Probability"] = 1.0
    return links_df


# LOAD INPUTS and SAVE OUTPUTS

clusters_var = os.environ["KNOWN_CLUSTERS_FILE_PATHS"]
# don't need to load ids_to_remove since its empty for dummy impl
results_dir = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

logging.info(f"Loading files for {clusters_var}")

datasets = []
file_paths = os.environ[clusters_var].split(",")
for path in file_paths:
    clusters_df = load_file(path)
    links_df = clusters_to_links(clusters_df)
    output_path = f"{results_dir}{os.path.basename(path)}.parquet"
    logging.info(f"Writing output for dataset from input {path} to {output_path}")
    links_df.to_parquet(output_path)

diagnostics[f"num_files_{clusters_var.lower()}"] = len(file_paths)
