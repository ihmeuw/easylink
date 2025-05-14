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

datasets_var = os.environ["INPUT_DATASETS_FILE_PATHS"]

logging.info(f"Loading files for {datasets_var}")

file_paths = os.environ[datasets_var].split(",")

# don't need to actually load inputs, just need the filepaths for the outputs


# SAVE OUTPUTS

# save empty dataframes for datasets_ids_to_remove

ids_to_remove = pd.DataFrame(columns=["record_ids"])

results_dir = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

for file_path in file_paths:
    output_path = f"{results_dir}{os.path.basename(file_path)}.parquet"
    logging.info(f"Writing output for dataset from input {file_path} to {output_path}")
    ids_to_remove.to_parquet(output_path)
