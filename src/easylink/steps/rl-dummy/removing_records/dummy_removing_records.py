# STEP_NAME: removing_records

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
diagnostics = {}


def load_file(file_path, file_format=None):
    if file_format is None:
        file_format = file_path.split(".")[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    raise ValueError()


# LOAD INPUTS

datasets_var = os.environ["INPUT_DATASETS_FILE_PATHS"]

logging.info(f"Loading files for {datasets_var}")

datasets = []
file_paths = os.environ[datasets_var].split(",")
for path in file_paths:
    datasets.append(load_file(path))

diagnostics[f"num_files_{datasets_var.lower()}"] = len(file_paths)

# don't need to load ids_to_remove since its empty for dummy impl


# SAVE OUTPUTS

results_dir = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

for i, file_path in enumerate(file_paths):
    output_path = f"{results_dir}{os.path.basename(file_path)}.parquet"
    logging.info(f"Writing output for dataset from input {file_path} to {output_path}")
    datasets[i].to_parquet(output_path)
