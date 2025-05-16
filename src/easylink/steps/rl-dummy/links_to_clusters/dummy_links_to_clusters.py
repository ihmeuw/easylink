# STEP_NAME: links_to_clusters

# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

# PIPELINE_SCHEMA: main

import logging
import os
from itertools import combinations, chain

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


# this code does not preserve the link relationships in the input,
# it just assigns some links to some clusters for the dummy implementation
# splink has connected components implementation for actually doing this
def links_to_clusters(links_df):
    all_records = list(zip(links_df["Left Record ID"], links_df["Right Record ID"]))
    l = list(range(len(all_records + 1) / 2))
    fake_clusters = list(chain.from_iterable(zip(l, l)))[: len(all_records)]

    clusters_df = pd.DataFrame(fake_clusters, columns=["Input Record ID", "Cluster ID"])
    return clusters_df


# LOAD INPUTS and SAVE OUTPUTS

# LINKS_FILE_PATHS is a path to a single directory
links_dir = os.environ["LINKS_FILE_PATHS"]
# DUMMY_CONTAINER_OUTPUT_PATHS is a path to a single directory
results_dir = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

for root, dirs, files in os.walk(links_dir):
    for file in files:
        links_filepath = os.path.join(root, file)
        links_df = load_file(links_filepath)
        clusters_df = links_to_clusters(links_df)
        output_path = f"{results_dir}{os.path.basename(links_filepath)}.parquet"
        logging.info(
            f"Writing output for dataset from input {links_filepath} to {output_path}"
        )
        clusters_df.to_parquet(output_path)
