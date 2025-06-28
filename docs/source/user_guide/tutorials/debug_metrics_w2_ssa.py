# NOTE: This file was used for debugging and exploring pipeline options for the tutorial.
# It is kept for historical purposes and future development only; it is not included in the tutorial itself.

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
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
print("\n---------False Positives----------")
print(false_positives[cols_to_print])
print("\n---------False Negatives----------")
print(false_negatives[cols_to_print])

print("\n False Positives with same ssn:")
print(false_positives[false_positives["ssn_l"] == false_positives["ssn_r"]][cols_to_print])


clusters_df = load_file(str(Path(results_dir / "result.parquet")))
counts = clusters_df["Cluster ID"].value_counts()
print("Clusters of size > 2:")
print(counts[counts > 2])
print(
    f"{len(counts[counts == 2])} clusters of size 2; {len(counts[counts == 1])} clusters of size 1"
)


data = []
num_w2s = records[records["Input Record Dataset"].str.contains("w2")]["unique_id"].nunique()
probabilities = np.sort(np.round(predictions_df["Probability"], decimals=3).unique())
for prob in probabilities[probabilities < 1]:  # drop probability==1 if it exists
    matches_w2_to_ssa = predictions_df[
        (predictions_df["Probability"] >= prob)
        & (
            predictions_df["Left Record Dataset"].str.contains("w2")
            != predictions_df["Right Record Dataset"].str.contains("w2")
        )
    ]
    num_w2s_matched = (
        matches_w2_to_ssa["unique_id_l"]
        if "w2" in matches_w2_to_ssa["unique_id_l"]
        else matches_w2_to_ssa["unique_id_r"]
    ).nunique()
    prop_w2_ssa_matches_with_unique_w2s = num_w2s_matched / len(matches_w2_to_ssa)
    data.append(
        [
            prob,
            num_w2s_matched / num_w2s,
            prop_w2_ssa_matches_with_unique_w2s,
        ]
    )

df = pd.DataFrame(
    data,
    columns=["Probability Threshold", "W2 Match Rate", "Unique W2 Rate among Matches"],
)
_, ax = plt.subplots()
ax.set_ylim(0.75, 1)
df.plot(x="Probability Threshold", y="W2 Match Rate", kind="line", ax=ax, marker="x")
df.plot(
    x="Probability Threshold",
    y="Unique W2 Rate among Matches",
    kind="line",
    ax=ax,
    marker="x",
)
plt.savefig(str(Path(results_dir / "matches_and_duplicates_by_prob.png")))
print("Plot data:")
print(df)
