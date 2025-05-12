import glob
import logging
import os

import pandas as pd
import yaml

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
    if file_format == "csv":
        return pd.read_csv(file_path)
    raise ValueError()


INPUT_ENV_VARS = os.getenv(
    "INPUT_DATASETS_FILE_PATHS",
    "KNOWN_CLUSTERS_FILE_PATHS",
).split(",")

diagnostics = {}

for env_var in INPUT_ENV_VARS:
    if env_var not in os.environ:
        logging.error(f"Missing required environment variable {env_var}")
        raise ValueError()

# LOAD INPUTS

# Load datasets
datasets_var = INPUT_ENV_VARS[0]

datasets = []

logging.info(f"Loading files for {datasets_var}")
file_paths = os.environ[datasets_var].split(",")
diagnostics[f"num_files_{datasets_var.lower()}"] = len(file_paths)
for path in file_paths:
    logging.info(f"Loading input from {path}")
    df = pd.concat([df, load_file(path)], ignore_index=True).fillna(0)
    datasets.append(df)

logging.info(f"Total number of input datasets is {len(datasets)}")

clusters_var = INPUT_ENV_VARS[1]

clusters = []

logging.info(f"Loading files for {clusters_var}")
file_paths = os.environ[clusters_var].split(",")
diagnostics[f"num_files_{clusters_var.lower()}"] = len(file_paths)
for path in file_paths:
    logging.info(f"Loading input from {path}")
    df = pd.concat([df, load_file(path)], ignore_index=True).fillna(0)
    clusters.append(df)

logging.info(f"Total number of input datasets is {len(clusters)}")

# VALIDATE INPUTS

# check for "record_id" column and unique values
for i, df in enumerate(datasets):
    if not df.record_id:
        logging.error(f"Missing column 'record_id' in input {file_paths[i]}")
        raise ValueError()
    if df.record_id.nunique() != len(df.record_id):
        logging.error(f"'record_id' values not unique for input {file_paths[i]}")
        raise ValueError()

# check known clusters is empty for default implementation
for i, df in enumerate(datasets):
    if len(df) > 0:
        logging.error(f"Non-empty known clusters data for input {file_paths[i]}")
        raise ValueError()

# PROCESSING

# for dummy implementation, skip all processing and just return an empty list of
# IDs to remove
datasets_ids_to_remove = []
for dataset in datasets:
    datasets_ids_to_remove.append(pd.DataFrame(columns=["record_ids"]))

# SAVE OUTPUTS

# save datasets_ids_to_remove

results_dir = "datasets/"

for i, df in enumerate(datasets_ids_to_remove):
    output_path = f"{results_dir}{i}.parquet"
    logging.info(
        f"Writing output for dataset from input {file_paths[i]} to {output_path}"
    )
    df.to_parquet(output_path)
