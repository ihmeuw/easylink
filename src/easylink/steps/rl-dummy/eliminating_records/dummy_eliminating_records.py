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

# INPUT_DATASETS_FILE_PATHS is comma-separated list of filepaths
file_paths = os.environ["INPUT_DATASETS_FILE_PATHS"].split(",")

# don't need to actually load inputs, just need the filepaths to 
# properly name the outputs when saving


# SAVE OUTPUTS

# save an empty ids_to_remove dataframe for each input dataset
# (rather than one for all datasets, due to parallel sections)

IDS_TO_REMOVE = pd.DataFrame(columns=["record_ids"])

# DUMMY_CONTAINER_OUTPUT_PATHS is a single path to a directory
results_dir = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

for file_path in file_paths:
    output_path = f"{results_dir}{os.path.basename(file_path)}.parquet"
    logging.info(f"Writing output for dataset from input {file_path} to {output_path}")
    IDS_TO_REMOVE.to_parquet(output_path)
