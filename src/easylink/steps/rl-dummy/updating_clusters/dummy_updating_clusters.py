# STEP_NAME: updating_clusters

# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

# PIPELINE_SCHEMA: main

import logging
import os

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[logging.StreamHandler()],
)


def load_file(file_path, file_format=None):
    if file_format is None:
        file_format = file_path.split(".")[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    raise ValueError()


# LOAD INPUTS and SAVE OUTPUTS

# NEW_CLUSTERS_FILE_PATH is a path to a single file
links_dir = os.environ["NEW_CLUSTERS_FILE_PATH"]
# KNOWN_CLUSTERS_AND_MAYBE_INPUT_DATASETS_FILE_PATHS is a list of filepaths with one
# known_clusters.parquet filepath, that may include input data filepaths due to workaround
clusters_filepaths = os.environ[
    "KNOWN_CLUSTERS_AND_MAYBE_INPUT_DATASETS_FILE_PATHS"
].split(",")
clusters_filepath = ""
for path in clusters_filepaths:
    if "known_clusters.parquet" in path:
        clusters_filepath = path
        break
if clusters_filepath == "":
    raise ValueError()
# DUMMY_CONTAINER_OUTPUT_PATHS is a path to a single directory
results_dir = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]


output_path = f"{results_dir}{os.path.basename(clusters_filepath)}.parquet"
logging.info(
    f"Writing output for dataset from input {clusters_filepath} to {output_path}"
)
clusters_filepath.to_parquet(output_path)
