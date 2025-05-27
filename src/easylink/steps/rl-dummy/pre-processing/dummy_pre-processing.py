# STEP_NAME: pre-processing
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

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

# DATASET_FILE_PATHS is list of filepaths
dataset_paths = os.environ["DATASET_FILE_PATHS"].split(",")
logging.info(f"dataset_paths={dataset_paths}")

# for workaround, choose path based on INPUT_DATASETS_SPLITTER_CHOICE configuration
splitter_choice = os.environ["INPUT_DATASETS_SPLITTER_CHOICE"]
logging.info(f"splitter_choice={splitter_choice}")
dataset_path = ""
for path in dataset_paths:
    path_to_check = Path(path) / Path(splitter_choice)
    logging.info(f"Checking if path '{path_to_check}' exists")
    if os.path.exists(path_to_check):
        dataset_path = str(path_to_check)
        break
if dataset_path == "":
    raise ValueError()

# DUMMY_CONTAINER_OUTPUT_PATHS is a single path to a directory ('dataset')
results_dir = Path(os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"])
results_dir.mkdir(exist_ok=True, parents=True)

output_path = results_dir / Path(os.path.basename(dataset_path))

dataset = load_file(dataset_path)

# NOTE: No actual pre-processing here, we save as-is.

logging.info(f"Writing output for dataset from input {dataset_path} to {output_path}")
dataset.to_parquet(output_path)
