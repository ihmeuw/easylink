import pandas as pd
import pdb

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


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

THRESHOLD = 0.9

false_positives = len(nonlinks["Probability"] >= THRESHOLD)
false_negatives = len(links["Probability"] < THRESHOLD)
print(f"For threshold {THRESHOLD}, {false_positives=}; {false_negatives=}")

print(links[0:false_positives])
print(nonlinks[0:false_negatives])


clusters_df = load_file(str(Path(results_dir / "result.parquet")))
print(clusters_df["Cluster ID"].value_counts())

data = []
pdb.set_trace()
num_w2s = records[records["Record ID"].str.contains("w2")]["Record ID"].nunique()
for prob in np.sort(predictions_df["Probability"].unique()):
    # change when separate dataset column is ready
    matches_w2_to_ssa = predictions_df[
        (predictions_df["Probability"] >= prob)
        & (
            (
                predictions_df["Left Record ID"].str.contains("w2")
                & ~predictions_df["Right Record ID"].str.contains("w2")
            )
            | (
                predictions_df["Right Record ID"].str.contains("w2")
                & ~predictions_df["Left Record ID"].str.contains("w2")
            )
        )
    ]
    num_w2s_matched = len(
        (
            matches_w2_to_ssa["Left Record ID"]
            if "w2" in matches_w2_to_ssa["Left Record ID"]
            else matches_w2_to_ssa["Right Record ID"]
        ).unique()
    )
    prop_w2_ssa_matches_with_duplicate_w2s = (
        len(matches_w2_to_ssa) - num_w2s_matched
    ) / len(matches_w2_to_ssa)
    data.append(
        [
            prob,
            num_w2s_matched / num_w2s,
            prop_w2_ssa_matches_with_duplicate_w2s,
        ]
    )

df = pd.DataFrame(
    data,
    columns=["Probability", "W2 Match Rate", "Duplicate W2 Rate among Matches"],
)
_, ax = plt.subplots()
df.plot(x="Probability", y="W2 Match Rate", kind="line", ax=ax)
df.plot(x="Probability", y="Duplicate W2 Rate among Matches", kind="line", ax=ax)
plt.savefig(str(Path(results_dir / "matches_and_duplicates_by_prob.png")))
