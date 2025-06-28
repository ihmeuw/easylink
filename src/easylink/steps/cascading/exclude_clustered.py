# STEP_NAME: determining_exclusions

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


# LOAD INPUTS

# INPUT_DATASETS_AND_INPUT_KNOWN_CLUSTERS_FILE_PATHS is list of filepaths which includes
# the known_clusters filepath due to workaround
dataset_paths = os.environ["INPUT_DATASETS_AND_INPUT_KNOWN_CLUSTERS_FILE_PATHS"].split(",")
dataset_paths = [path for path in dataset_paths if "clusters" not in Path(path).stem]

# for workaround, choose path based on INPUT_DATASET configuration
splitter_choice = os.environ["INPUT_DATASET"]
dataset_path = None
for path in dataset_paths:
    if splitter_choice == Path(path).stem:
        dataset_path = path
        break
if dataset_path is None:
    raise ValueError(f"No dataset matching {splitter_choice} found")

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

# Exclude records that have been clustered
clusters_df = load_file(clusters_filepath)
# NOTE: We defined "clustered" for these purposes as clustered *with* anything else.
# Simply putting a record into its own cluster does not indicate to us that it has
# been sufficiently clustered to ignore.
cluster_sizes = clusters_df.groupby("Cluster ID").size()
clusters_df["size"] = cluster_sizes.loc[clusters_df["Cluster ID"]].values
clusters_df = clusters_df[clusters_df["size"] > 1]

dataset_df = load_file(dataset_path)
clustered_record_ids = set(dataset_df["Record ID"].unique()) & set(
    clusters_df[clusters_df["Input Record Dataset"] == splitter_choice][
        "Input Record ID"
    ].unique()
)

IDS_TO_REMOVE = pd.DataFrame({"Input Record ID": list(clustered_record_ids)})

# OUTPUT_PATHS is a single path to a file (results.parquet)
results_filepath = os.environ["OUTPUT_PATHS"]

logging.info(f"Writing output for dataset from input {dataset_path} to {results_filepath}")
IDS_TO_REMOVE.to_parquet(results_filepath)
