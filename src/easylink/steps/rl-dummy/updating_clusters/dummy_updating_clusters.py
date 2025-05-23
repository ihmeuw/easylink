# STEP_NAME: updating_clusters

# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

# PIPELINE_SCHEMA: main

import logging
import os

import pandas as pd

from pathlib import Path

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


# LOAD INPUTS and SAVE OUTPUTS

# for dummy we will load only the new clusters and save them as-is

# NEW_CLUSTERS_FILE_PATH is a path to a single file
new_clusters_filepath = os.environ["NEW_CLUSTERS_FILE_PATH"]
# DUMMY_CONTAINER_OUTPUT_PATHS is a path to a single file (results.parquet)
results_filepath = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]
Path(results_filepath).parent.mkdir(exist_ok=True, parents=True)

clusters_df = load_file(new_clusters_filepath)


logging.info(
    f"Writing output for dataset from input {new_clusters_filepath} to {results_filepath}"
)
clusters_df.to_parquet(results_filepath)
