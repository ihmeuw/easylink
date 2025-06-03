# STEP_NAME: updating_clusters

# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

# PIPELINE_SCHEMA: main

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

# NEW_CLUSTERS_FILE_PATH is a path to a single file
new_clusters_filepath = os.environ["NEW_CLUSTERS_FILE_PATH"]

# KNOWN_CLUSTERS_AND_MAYBE_INPUT_DATASETS_FILE_PATHS is a list of file paths.
# There is one item in it that is a file with "clusters" in the filename.
# That's the only item we're interested in here.
# The other items may be there if this is coming from the user's input,
# due to our workaround for only having one slot of user input.
known_clusters_filepaths = [
    path
    for path in os.environ["KNOWN_CLUSTERS_AND_MAYBE_INPUT_DATASETS_FILE_PATHS"].split(",")
    if "clusters" in Path(path).stem
]
if len(known_clusters_filepaths) > 1:
    raise ValueError("Multiple known clusters files found")
if len(known_clusters_filepaths) == 0:
    raise ValueError("No known clusters file found")

known_clusters_filepath = known_clusters_filepaths[0]
known_clusters_df = load_file(known_clusters_filepath)

if len(known_clusters_df) > 0:
    raise ValueError(
        "Default implementation of updating_clusters passed a non-empty set of known clusters"
    )

# DUMMY_CONTAINER_OUTPUT_PATHS is a path to a single file (clusters.parquet)
results_filepath = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]
Path(results_filepath).parent.mkdir(exist_ok=True, parents=True)

clusters_df = load_file(new_clusters_filepath)


logging.info(
    f"Writing output for dataset from input {new_clusters_filepath} to {results_filepath}"
)
clusters_df.to_parquet(results_filepath)
