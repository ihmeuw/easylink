# STEP_NAME: eliminating_records

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

# LOAD INPUTS

# INPUT_DATASETS_FILE_PATHS is list of filepaths which includes the known_clusters
# filepath due to workaround
dataset_paths = os.environ["INPUT_DATASETS_FILE_PATHS"].split(",")
dataset_paths = [path for path in dataset_paths if "known_clusters.parquet" not in path]

# don't need to actually this dataset or the clusters dataset,
# we will just save an empty ids_to_remove dataframe to the same
# filename as the dataset


# SAVE OUTPUTS

IDS_TO_REMOVE = pd.DataFrame(columns=["record_ids"])

# DUMMY_CONTAINER_OUTPUT_PATHS is a single path to a directory
results_dir = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

for dataset_path in dataset_paths:
    output_path = f"{results_dir}{os.path.basename(dataset_path)}.parquet"
    logging.info(
        f"Writing output for dataset from input {dataset_path} to {output_path}"
    )
    IDS_TO_REMOVE.to_parquet(output_path)
