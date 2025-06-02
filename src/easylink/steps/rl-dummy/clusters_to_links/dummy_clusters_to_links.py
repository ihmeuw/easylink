# STEP_NAME: clusters_to_links

# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

# PIPELINE_SCHEMA: main

import logging
import os
from itertools import combinations

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
    raise ValueError()


# code example from pipeline schema docs
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

# KNOWN_CLUSTERS_AND_MAYBE_INPUT_DATASETS_FILE_PATHS is a list of filepaths with one
# known_clusters.parquet filepath, that may include input data filepaths due to workaround
clusters_filepaths = os.environ["KNOWN_CLUSTERS_AND_MAYBE_INPUT_DATASETS_FILE_PATHS"].split(
    ","
)
clusters_filepath = ""
for path in clusters_filepaths:
    if "known_clusters.parquet" in path:
        clusters_filepath = path
        break
if clusters_filepath == "":
    raise ValueError()

# DUMMY_CONTAINER_OUTPUT_PATHS is a path to a single file (results.parquet)
results_filepath = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

clusters_df = load_file(clusters_filepath)
links_df = clusters_to_links(clusters_df)
logging.info(
    f"Writing output for dataset from input {clusters_filepath} to {results_filepath}"
)
links_df.to_parquet(results_filepath)
