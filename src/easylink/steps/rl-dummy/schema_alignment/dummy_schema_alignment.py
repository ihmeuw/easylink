# STEP_NAME: schema_alignment
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

# DATASETS_FILE_PATHS is list of filepaths
dataset_paths = os.environ["DATASETS_FILE_PATHS"].split(",")
datasets = {
    Path(dataset_path).stem: load_file(dataset_path) for dataset_path in dataset_paths
}

records = pd.concat(
    [
        df.assign(
            dataset=dataset,
        )
        for dataset, df in datasets.items()
    ],
    ignore_index=True,
    sort=False,
)

# DUMMY_CONTAINER_OUTPUT_PATHS is a single filepath
output_path = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

records.to_parquet(output_path)
