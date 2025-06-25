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
    raise ValueError(f"Unknown file format {file_format}")


# LOAD INPUTS and SAVE OUTPUTS

# DATASET_DIR_PATHS is list of directories, each containing one file
dataset_paths = os.environ["DATASET_DIR_PATHS"].split(",")
logging.info(f"{dataset_paths=}")

# for workaround, choose path based on INPUT_DATASET configuration
splitter_choice = os.environ["INPUT_DATASET"]
logging.info(f"splitter_choice={splitter_choice}")
dataset_path = None
for path in dataset_paths:
    path = Path(path)
    # NOTE: We iterate the dir here, but it should only have one non-hidden
    # file in it. We don't validate that here as it is checked in the validator.
    for path_to_check in path.iterdir():
        if path_to_check.stem == splitter_choice:
            dataset_path = str(path_to_check)
            break

if dataset_path is None:
    raise ValueError(f"No dataset matching {splitter_choice} found")

# OUTPUT_PATHS is a single path to a directory ('dataset')
results_dir = Path(os.environ["OUTPUT_PATHS"])
results_dir.mkdir(exist_ok=True, parents=True)

output_path = results_dir / Path(dataset_path).name

dataset = load_file(dataset_path)

# NOTE: No actual pre-processing here, we save as-is.

logging.info(f"Writing output for dataset from input {dataset_path} to {output_path}")
dataset.to_parquet(output_path)
