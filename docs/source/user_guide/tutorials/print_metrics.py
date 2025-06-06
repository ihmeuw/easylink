import pandas as pd
import pdb

import argparse
from pathlib import Path


def load_file(file_path, file_format=None):
    print(f"Loading file {file_path} with format {file_format}")
    if file_format is None:
        file_format = file_path.split(".")[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    raise ValueError(f"Unknown file format {file_format}")


parser = argparse.ArgumentParser()
parser.add_argument("inputs_file_path", type=Path)
parser.add_argument("results_dir", type=Path)

p = parser.parse_args()
if not p.inputs_file_path.exists():
    print(f"No argument for inputs configuration file (YAML) file path")
if not p.results_dir.exists():
    print(f"No argument for results directory path")

inputs_file_path = p.inputs_file_path
results_dir = p.results_dir

input_dfs = []
with open(inputs_file_path, "r") as file:
    for line in file:
        if "clusters" in line:
            continue
        input_dfs.append(load_file(str(line).split(":", 1)[1].strip()))

predictions_df = load_file(
    str(Path(results_dir / "intermediate/splink_evaluating_pairs/result.parquet"))
)

records = pd.concat(input_dfs)

num_cols_before_merge = len(predictions_df.columns)
predictions_df = pd.merge(
    predictions_df, records, left_on="Left Record ID", right_on="Record ID", how="left"
)
predictions_df = predictions_df.rename(
    columns=dict(
        zip(
            predictions_df.columns[num_cols_before_merge:],
            predictions_df.columns[num_cols_before_merge:] + "_l",
        )
    )
)

num_cols_before_merge = len(predictions_df.columns)
predictions_df = pd.merge(
    predictions_df, records, left_on="Right Record ID", right_on="Record ID", how="left"
)
predictions_df = predictions_df.rename(
    columns=dict(
        zip(
            predictions_df.columns[num_cols_before_merge:],
            predictions_df.columns[num_cols_before_merge:] + "_r",
        )
    )
)

# sort links by lowest match_probability to see if we missed any
links = predictions_df[
    predictions_df["simulant_id_l"] == predictions_df["simulant_id_r"]
].sort_values("Probability")
# sort nonlinks by highest match_probability to see if we matched any
nonlinks = predictions_df[
    predictions_df["simulant_id_l"] != predictions_df["simulant_id_r"]
].sort_values("Probability", ascending=False)

print(links)
print(nonlinks)
