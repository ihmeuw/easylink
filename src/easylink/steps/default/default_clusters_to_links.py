# STEP_NAME: clusters_to_links

# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

# PIPELINE_SCHEMA: main

import logging
import os
from itertools import combinations
from pathlib import Path

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

# KNOWN_CLUSTERS_AND_MAYBE_INPUT_DATASETS_FILE_PATHS is a list of file paths.
# There is one item in it that is a file with "clusters" in the filename.
# That's the only item we're interested in here.
# The other items may be there if this is coming from the user's input,
# due to our workaround for only having one slot of user input.
clusters_filepaths = [
    path
    for path in
    os.environ["KNOWN_CLUSTERS_AND_MAYBE_INPUT_DATASETS_FILE_PATHS"].split(
        ","
    )
    if "clusters" in Path(path).stem
]
if len(clusters_filepaths) > 1:
    raise ValueError("Multiple known clusters files found")
if len(clusters_filepaths) == 0:
    raise ValueError("No known clusters file found")

clusters_filepath = clusters_filepaths[0]

# DUMMY_CONTAINER_OUTPUT_PATHS is a path to a single file (results.parquet)
results_filepath = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

clusters_df = load_file(clusters_filepath)
links_df = clusters_to_links(clusters_df)
logging.info(
    f"Writing output for dataset from input {clusters_filepath} to {results_filepath}"
)
links_df.to_parquet(results_filepath)
