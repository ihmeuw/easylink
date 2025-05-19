# STEP_NAME: canonicalizing_and_downstream_analysis

# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

# PIPELINE_SCHEMA: main

import logging
import os
from itertools import combinations, chain

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

# For dummy we will load the clusters and output them as-is

# CLUSTERS_FILE_PATH is a path to a single file
clusters_path = os.environ["CLUSTERS_FILE_PATH"]
# DUMMY_CONTAINER_OUTPUT_PATHS is a path to a single directory
results_dir = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

clusters_df = load_file(clusters_path)


output_path = f"{results_dir}{os.path.basename(clusters_path)}.parquet"
logging.info(f"Writing output for dataset from input {clusters_path} to {output_path}")
clusters_df.to_parquet(output_path)
