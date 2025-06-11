# STEP_NAME: links_to_clusters
# REQUIREMENTS: pandas pyarrow splink==4.0.7

import os
from pathlib import Path

import numpy as np
import pandas as pd

# Adapted from example on https://moj-analytical-services.github.io/splink/api_docs/clustering.html
from splink import DuckDBAPI
from splink.clustering import cluster_pairwise_predictions_at_threshold

links = pd.read_parquet(os.environ["LINKS_FILE_PATH"]).rename(
    columns={
        "Probability": "match_probability",
    }
)

# Create unique record keys by concatenating Input Record Dataset and Record ID for both left and right
links["Left Record Key"] = (
    links["Left Record Dataset"].astype(str) + "-__-" + links["Left Record ID"].astype(str)
)
links["Right Record Key"] = (
    links["Right Record Dataset"].astype(str) + "-__-" + links["Right Record ID"].astype(str)
)

dummy_records_df = pd.DataFrame(
    {
        "Record Key": np.unique(
            list(links["Left Record Key"]) + list(links["Right Record Key"])
        )
    }
)
output_path = Path(os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"])

db_api = DuckDBAPI()

cc = (
    cluster_pairwise_predictions_at_threshold(
        dummy_records_df,
        links,
        node_id_column_name='"Record Key"',
        edge_id_column_name_left='"Left Record Key"',
        edge_id_column_name_right='"Right Record Key"',
        db_api=db_api,
        threshold_match_probability=float(os.environ["THRESHOLD_MATCH_PROBABILITY"]),
    )
    .as_pandas_dataframe()
    .rename(columns={"cluster_id": "Cluster ID"})
)

# Split "Record Key" back into "Input Record Dataset" and "Input Record ID"
cc[["Input Record Dataset", "Input Record ID"]] = (
    cc["Record Key"].astype(str).str.split("-__-", n=1, expand=True)
)
cc = cc.drop(columns=["Record Key"])
cc["Input Record ID"] = cc["Input Record ID"].astype(int)
cc = cc[["Input Record Dataset", "Input Record ID", "Cluster ID"]]

print(cc)

cc.to_parquet(output_path)
