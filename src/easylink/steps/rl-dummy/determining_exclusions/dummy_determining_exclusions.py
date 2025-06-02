# STEP_NAME: determining_exclusions

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

# INPUT_DATASETS_AND_INPUT_KNOWN_CLUSTERS_FILE_PATHS is list of filepaths which includes
# the known_clusters filepath due to workaround
dataset_paths = os.environ["INPUT_DATASETS_AND_INPUT_KNOWN_CLUSTERS_FILE_PATHS"].split(
    ","
)
dataset_paths = [path for path in dataset_paths if "known_clusters.parquet" not in path]

# for workaround, choose path based on INPUT_DATASET configuration
splitter_choice = os.environ["INPUT_DATASET"]
dataset_path = ""
for path in dataset_paths:
    if splitter_choice == os.path.basename(path):
        dataset_path = path
        break
if dataset_path == "":
    raise ValueError()

# don't need to actually this dataset or the clusters dataset,
# we will just save an empty ids_to_remove dataframe to the same
# filename as the dataset


# SAVE OUTPUTS

IDS_TO_REMOVE = pd.DataFrame(columns=["Record ID"])

# DUMMY_CONTAINER_OUTPUT_PATHS is a single path to a file (results.parquet)
results_filepath = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

logging.info(
    f"Writing output for dataset from input {dataset_path} to {results_filepath}"
)
IDS_TO_REMOVE.to_parquet(results_filepath)
