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
    raise ValueError(f"Unknown file format {file_format}")


# LOAD INPUTS and SAVE OUTPUTS

# DATASETS_DIR_PATHS is list of directories
dataset_dirs = os.environ["DATASETS_DIR_PATHS"].split(",")

datasets = {}

for dir in dataset_dirs:
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file.startswith("."):
                continue
            datasets[Path(file).stem] = load_file(os.path.join(root, file))

records = pd.concat(
    [df.assign(**{"Input Record Dataset": dataset}) for dataset, df in datasets.items()],
    ignore_index=True,
    sort=False,
)

records = records.rename(columns={"Record ID": "Input Record ID"})

# DUMMY_CONTAINER_OUTPUT_PATHS is a single filepath
output_path = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]
Path(output_path).parent.mkdir(exist_ok=True, parents=True)

logging.info(f"Writing output to {output_path}")
records.to_parquet(output_path)
