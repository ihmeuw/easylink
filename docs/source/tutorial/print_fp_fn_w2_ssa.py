import argparse
from pathlib import Path

import pandas as pd


def load_file(file_path, file_format=None):
    if file_format is None:
        file_format = file_path.split(".")[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    raise ValueError(f"Unknown file format {file_format}")


parser = argparse.ArgumentParser()
parser.add_argument("results_dir", type=Path)

p = parser.parse_args()
if not p.results_dir.exists():
    print(f"No argument for results directory path")

results_dir = p.results_dir

import glob

import yaml

# Read input data YAML file -- note that this assumes it starts with input_data,
# though that isn't a formal EasyLink requirement!
input_data_yaml_path = glob.glob(str(results_dir / "input_data_*.yaml"))
assert len(input_data_yaml_path) == 1
with open(input_data_yaml_path[0], "r") as stream:
    input_data_yaml = yaml.safe_load(stream)

input_data_files = {
    k: Path(p).resolve()
    for k, p in input_data_yaml.items()
    if Path(p).stem != "known_clusters"
}

records = pd.concat(
    [
        pd.read_parquet(p).assign(**{"Input Record Dataset": p.stem})
        for k, p in input_data_files.items()
    ],
    ignore_index=True,
    sort=False,
).rename(columns={"Record ID": "Input Record ID"})

clusters_df = load_file(str(Path(results_dir / "result.parquet")))

# code example from pipeline schema docs
def clusters_to_links(clusters_df):
    # Merge the dataframe with itself on Cluster ID to get all pairs within each cluster
    merged = clusters_df.merge(
        clusters_df,
        on="Cluster ID",
        suffixes=("_left", "_right"),
    )

    # Compare tuples row-wise to keep only unique pairs (left < right)
    mask = (merged["Input Record Dataset_left"] < merged["Input Record Dataset_right"]) | (
        (merged["Input Record Dataset_left"] == merged["Input Record Dataset_right"])
        & (merged["Input Record ID_left"] < merged["Input Record ID_right"])
    )
    filtered = merged[mask]

    # Build the output DataFrame
    links_df = filtered[
        [
            "Input Record Dataset_left",
            "Input Record ID_left",
            "Input Record Dataset_right",
            "Input Record ID_right",
        ]
    ].copy()
    links_df.columns = [
        "Left Record Dataset",
        "Left Record ID",
        "Right Record Dataset",
        "Right Record ID",
    ]
    links_df["Probability"] = 1.0
    return links_df


predictions_df = clusters_to_links(clusters_df)

# concatenate Record Dataset and Record ID columns for merge
def unique_id_column(df, record_name="Input"):
    return (
        df[f"{record_name} Record Dataset"].astype(str)
        + "_"
        + df[f"{record_name} Record ID"].astype(str)
    )


records["unique_id"] = unique_id_column(records)
predictions_df["unique_id_l"] = unique_id_column(predictions_df, record_name="Left")
predictions_df["unique_id_r"] = unique_id_column(predictions_df, record_name="Right")

links = (
    records.add_suffix("_l")
    .merge(
        records.add_suffix("_r"),
        left_on="simulant_id_l",
        right_on="simulant_id_r",
        how="left",
    )
    .pipe(
        lambda df: df[
            (df.unique_id_l != df.unique_id_r)
            & (df["Input Record Dataset_l"] < df["Input Record Dataset_r"])
            | (
                (df["Input Record Dataset_l"] == df["Input Record Dataset_r"])
                & (df["Input Record ID_l"] < df["Input Record ID_r"])
            )
        ]
    )
)
pd.set_option("future.no_silent_downcasting", True)
links = links.merge(
    predictions_df[["unique_id_l", "unique_id_r"]].assign(matched=True),
    on=["unique_id_l", "unique_id_r"],
    how="left",
)
links["matched"] = links["matched"].fillna(False).astype(bool)

predictions_df = predictions_df.merge(
    records.add_suffix("_l"), on="unique_id_l", how="left"
).merge(records.add_suffix("_r"), on="unique_id_r", how="left")

matched_nonlinks = predictions_df[
    predictions_df["simulant_id_l"] != predictions_df["simulant_id_r"]
]

pd.set_option("display.max_columns", None)
false_positives = matched_nonlinks
false_negatives = links[~links.matched]
print(f"{len(links)} true links")
print(f"{len(false_positives)=}; {len(false_negatives)=}")
