# STEP_NAME: pre-processing
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

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

# DATASET_FILE_PATHS is list of filepaths
dataset_paths = os.environ["DATASET_FILE_PATHS"].split(",")

# for workaround, choose path based on INPUT_DATASETS_SPLITTER_CHOICE configuration
splitter_choice = os.environ["INPUT_DATASETS_SPLITTER_CHOICE"]
dataset_path = ""
for path in dataset_paths:
    if splitter_choice == os.path.basename(path):
        dataset_path = path
        break
if dataset_path == "":
    raise ValueError()

# DUMMY_CONTAINER_OUTPUT_PATHS is a single filepath
output_path = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

dataset = load_file(dataset_path)

# NOTE: No actual pre-processing here, we save as-is.

dataset.to_parquet(output_path)
