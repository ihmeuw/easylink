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
dummy_records_df = pd.DataFrame(
    {
        "Input Record ID": np.unique(
            list(links["Left Record ID"]) + list(links["Right Record ID"])
        )
    }
)
output_path = Path(os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"])

db_api = DuckDBAPI()

cc = (
    cluster_pairwise_predictions_at_threshold(
        dummy_records_df,
        links,
        node_id_column_name='"Input Record ID"',
        edge_id_column_name_left='"Left Record ID"',
        edge_id_column_name_right='"Right Record ID"',
        db_api=db_api,
        threshold_match_probability=float(os.environ["THRESHOLD_MATCH_PROBABILITY"]),
    )
    .as_pandas_dataframe()
    .rename(columns={"cluster_id": "Cluster ID"})
)

print(cc)

cc.to_parquet(output_path)
