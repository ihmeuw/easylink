# NOTE: This file was used for debugging and exploring pipeline options for the tutorial.
# It is kept for historical purposes and future development only; it is not included in the tutorial itself.

import argparse
from pathlib import Path

import pandas as pd
import yaml

KEY_COLS = ["ssn", "first_name", "middle_initial", "last_name"]


def load_file(file_path, file_format=None):
    if file_format is None:
        file_format = file_path.suffix[1:]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    raise ValueError(f"Unsupported file format: {file_format}")


def clusters_to_links(clusters_df):
    merged = clusters_df.merge(clusters_df, on="Cluster ID", suffixes=("_left", "_right"))
    mask = (merged["Input Record Dataset_left"] < merged["Input Record Dataset_right"]) | (
        (merged["Input Record Dataset_left"] == merged["Input Record Dataset_right"])
        & (merged["Input Record ID_left"] < merged["Input Record ID_right"])
    )
    filtered = merged[mask]
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


def unique_id_column(df, side="Left"):
    return (
        df[f"{side} Record Dataset"].astype(str) + "_" + df[f"{side} Record ID"].astype(str)
    )


def compute_fp_fn(results_dir: Path):
    with open(results_dir / "input_data_demo.yaml", "r") as stream:
        input_data_yaml = yaml.safe_load(stream)

    input_data_files = {
        k: Path(v).resolve()
        for k, v in input_data_yaml.items()
        if Path(v).stem != "known_clusters"
    }
    records = pd.concat(
        [
            pd.read_parquet(p).assign(**{"Input Record Dataset": p.stem})
            for k, p in input_data_files.items()
        ],
        ignore_index=True,
        sort=False,
    ).rename(columns={"Record ID": "Input Record ID"})

    records["unique_id"] = (
        records["Input Record Dataset"].astype(str)
        + "_"
        + records["Input Record ID"].astype(str)
    )

    clusters_df = load_file(results_dir / "result.parquet")
    pred_links = clusters_to_links(clusters_df)

    pred_links["unique_id_l"] = unique_id_column(pred_links, "Left")
    pred_links["unique_id_r"] = unique_id_column(pred_links, "Right")

    pred_links = pred_links.merge(
        records.add_suffix("_l"), on="unique_id_l", how="left"
    ).merge(records.add_suffix("_r"), on="unique_id_r", how="left")

    # Compute False Positives
    false_positives = pred_links[pred_links["simulant_id_l"] != pred_links["simulant_id_r"]]

    # Compute all true links
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
                (df["unique_id_l"] != df["unique_id_r"])
                & (
                    (df["Input Record Dataset_l"] < df["Input Record Dataset_r"])
                    | (
                        (df["Input Record Dataset_l"] == df["Input Record Dataset_r"])
                        & (df["Input Record ID_l"] < df["Input Record ID_r"])
                    )
                )
            ]
        )
    )

    links["unique_id_l"] = links["unique_id_l"]
    links["unique_id_r"] = links["unique_id_r"]

    match_keys = ["unique_id_l", "unique_id_r"]
    predictions_set = set(pred_links[match_keys].itertuples(index=False, name=None))

    links["matched"] = links.apply(
        lambda row: (row["unique_id_l"], row["unique_id_r"]) in predictions_set, axis=1
    )
    false_negatives = links[~links.matched]

    return false_positives, false_negatives


def show_deltas(old_df, new_df, label):
    old_pairs = set(old_df[["unique_id_l", "unique_id_r"]].itertuples(index=False, name=None))
    new_pairs = set(new_df[["unique_id_l", "unique_id_r"]].itertuples(index=False, name=None))

    eliminated = old_pairs - new_pairs
    new = new_pairs - old_pairs

    print(f"\n{label.upper()}:")
    print(f"  Eliminated: {len(eliminated)}")
    print(f"  New: {len(new)}")

    def get_details(pairs, df, kind):
        return (
            df.set_index(["unique_id_l", "unique_id_r"])
            .loc[list(pairs)]
            .reset_index()[
                [
                    "unique_id_l",
                    "unique_id_r",
                    *[f"{col}_l" for col in KEY_COLS],
                    *[f"{col}_r" for col in KEY_COLS],
                ]
            ]
            .assign(type=kind)
            .sort_values(["unique_id_l", "unique_id_r"])
        )

    delta_df = pd.DataFrame()
    if eliminated:
        delta_df = pd.concat([delta_df, get_details(eliminated, old_df, "eliminated")])
    if new:
        delta_df = pd.concat([delta_df, get_details(new, new_df, "new")])

    print(delta_df.to_string(index=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--old-results-dir", type=Path, required=True)
    parser.add_argument("--new-results-dir", type=Path, required=True)
    args = parser.parse_args()

    old_fp, old_fn = compute_fp_fn(args.old_results_dir)
    new_fp, new_fn = compute_fp_fn(args.new_results_dir)

    show_deltas(old_fp, new_fp, "false positives")
    show_deltas(old_fn, new_fn, "false negatives")


if __name__ == "__main__":
    main()
