import argparse
from pathlib import Path

import pandas as pd


def load_file(file_path, file_format=None):
    print(f"Loading file {file_path} with format {file_format}")
    if file_format is None:
        file_format = file_path.split(".")[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    raise ValueError(f"Unknown file format {file_format}")


parser = argparse.ArgumentParser()
parser.add_argument("results_dir", type=Path)
parser.add_argument("threshold", type=float)

p = parser.parse_args()
if not p.results_dir.exists():
    print(f"No argument for results directory path")

if not p.threshold:
    print(f"No argument for threshold")

results_dir = p.results_dir
threshold = p.threshold

records = load_file(
    str(Path(results_dir / "intermediate/default_schema_alignment/result.parquet"))
)

predictions_df = load_file(
    str(Path(results_dir / "intermediate/splink_evaluating_pairs/result.parquet"))
)

# concatenate Record Dataset and Record ID columns for merge
records["unique_id"] = (
    records["Input Record Dataset"].astype(str) + "_" + records["Input Record ID"].astype(str)
)
predictions_df["unique_id_l"] = (
    predictions_df["Left Record Dataset"].astype(str)
    + "_"
    + predictions_df["Left Record ID"].astype(str)
)
predictions_df["unique_id_r"] = (
    predictions_df["Right Record Dataset"].astype(str)
    + "_"
    + predictions_df["Right Record ID"].astype(str)
)

predictions_df = predictions_df.merge(
    records.add_suffix("_l"), on="unique_id_l", how="left"
).merge(records.add_suffix("_r"), on="unique_id_r", how="left")

# sort links by lowest match_probability to see if we missed any
links = predictions_df[
    predictions_df["simulant_id_l"] == predictions_df["simulant_id_r"]
].sort_values("Probability")
# sort nonlinks by highest match_probability to see if we matched any
nonlinks = predictions_df[
    predictions_df["simulant_id_l"] != predictions_df["simulant_id_r"]
].sort_values("Probability", ascending=False)

cols_to_print = [
    "ssn_l",
    "ssn_r",
    "first_name_l",
    "first_name_r",
    "middle_initial_l",
    "middle_initial_r",
    "last_name_l",
    "last_name_r",
]
pd.set_option("display.max_columns", None)
false_positives = nonlinks[nonlinks["Probability"] >= threshold]
false_negatives = links[links["Probability"] < threshold]
print(f"{len(links)} true links")
print(f"For threshold {threshold}, {len(false_positives)=}; {len(false_negatives)=}")
