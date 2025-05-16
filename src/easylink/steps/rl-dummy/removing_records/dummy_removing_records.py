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


def load_file(file_path, file_format=None):
    if file_format is None:
        file_format = file_path.split(".")[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    raise ValueError()


# LOAD INPUTS and SAVE OUTPUTS

# INPUT_DATASETS_FILE_PATHS is comma-separated list of filepaths
datasets_filepaths = os.environ["INPUT_DATASETS_FILE_PATHS"].split(",")
# IDS_TO_REMOVE_FILE_PATHS is a single path to a directory
ids_dir = os.environ["IDS_TO_REMOVE_FILE_PATHS"]
# DUMMY_CONTAINER_OUTPUT_PATHS is a single path to a directory
results_dir = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

logging.info(f"Loading files for {datasets_filepaths}")

datasets = []
dataset_filepaths = os.environ[datasets_filepaths]
for dataset_filepath in dataset_filepaths:
    df_dataset = load_file(dataset_filepath)
    df_ids = load_file()
    df_dataset = df_dataset[~df_dataset["Record ID"].isin(ids_var)]
    output_path = f"{results_dir}{os.path.basename(dataset_filepath)}.parquet"
    logging.info(
        f"Writing output for dataset from input {dataset_filepath} to {output_path}"
    )
    df_dataset.to_parquet(output_path)
