# STEP_NAME: canonicalizing_and_downstream_analysis

# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

# PIPELINE_SCHEMA: main

import logging
import os
from itertools import chain, combinations
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


# LOAD INPUTS and SAVE OUTPUTS

# For dummy we will load the clusters and output (only) them as-is

# CLUSTERS_FILE_PATH is a path to a single file
clusters_path = os.environ["CLUSTERS_FILE_PATH"]
# OUTPUT_PATHS is a path to a single file (results.parquet)
results_filepath = os.environ["OUTPUT_PATHS"]

clusters_df = load_file(clusters_path)

logging.info(f"Writing output for dataset from input {clusters_path} to {results_filepath}")
clusters_df.to_parquet(results_filepath)
