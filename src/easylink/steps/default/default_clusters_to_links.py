# STEP_NAME: clusters_to_links

# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

# PIPELINE_SCHEMA: main

import logging
import os
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
    # Merge the dataframe with itself on Cluster ID to get all pairs within each cluster
    merged = clusters_df.merge(
        clusters_df,
        on="Cluster ID",
        suffixes=("_left", "_right"),
    )

    # Compare tuples row-wise to keep only unique pairs (left < right)
    mask = (merged["Input Record Dataset_left"] < merged["Input Record Dataset_right"]) | (
        (merged["Input Record Dataset_left"] == merged["Input Record Dataset_right"])
        & (merged["Input Record ID_left"] < merged["Input Record ID_right"])
    )
    filtered = merged[mask]

    # Build the output DataFrame
    links_df = filtered[
        [
            "Input Record Dataset_left",
            "Input Record ID_left",
            "Input Record Dataset_right",
            "Input Record ID_right",
        ]
    ].copy()
    links_df.columns = [
        "Left Record Dataset",
        "Left Record ID",
        "Right Record Dataset",
        "Right Record ID",
    ]
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
    for path in os.environ["KNOWN_CLUSTERS_AND_MAYBE_INPUT_DATASETS_FILE_PATHS"].split(",")
    if "clusters" in Path(path).stem
]
if len(clusters_filepaths) > 1:
    raise ValueError("Multiple known clusters files found")
if len(clusters_filepaths) == 0:
    raise ValueError("No known clusters file found")

clusters_filepath = clusters_filepaths[0]

# OUTPUT_PATHS is a path to a single file (results.parquet)
results_filepath = os.environ["OUTPUT_PATHS"]

clusters_df = load_file(clusters_filepath)
links_df = clusters_to_links(clusters_df)
logging.info(
    f"Writing output for dataset from input {clusters_filepath} to {results_filepath}"
)
links_df.to_parquet(results_filepath)
