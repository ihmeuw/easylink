# STEP_NAME: removing_records

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
    raise ValueError()


# LOAD INPUTS and SAVE OUTPUTS

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

# IDS_TO_REMOVE_FILE_PATH is a single filepath (Cloneable section)
ids_filepath = os.environ["IDS_TO_REMOVE_FILE_PATH"]
# DUMMY_CONTAINER_OUTPUT_PATHS is a single path to a directory ('dataset')
results_dir = Path(os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"])
results_dir.mkdir(exist_ok=True, parents=True)

dataset = load_file(dataset_path)
ids_to_remove = load_file(ids_filepath)

dataset = dataset[~dataset["Record ID"].isin(ids_to_remove)]

output_path = results_dir / Path(os.path.basename(dataset_path))
logging.info(f"Writing output for dataset from input {dataset_path} to {output_path}")
dataset.to_parquet(output_path)
