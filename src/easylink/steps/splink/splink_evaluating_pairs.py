# STEP_NAME: evaluating_pairs
# REQUIREMENTS: pandas pyarrow splink==4.0.7

import os
from pathlib import Path

import pandas as pd
import splink
import splink.comparison_library as cl
from splink import Linker, SettingsCreator

blocks_dir = Path(os.environ["BLOCKS_DIR_PATH"])
diagnostics_dir = Path(os.environ["DIAGNOSTICS_DIRECTORY"])
output_path = Path(os.environ["OUTPUT_PATHS"])
Path(output_path).parent.mkdir(exist_ok=True, parents=True)
link_only = os.getenv("LINK_ONLY", "false").lower() in ("true", "yes", "1")

all_predictions = []

for block_dir in blocks_dir.iterdir():
    if str(block_dir.stem).startswith("."):
        continue
    encoded_comparisons = os.environ["COMPARISONS"].split(",")

    comparisons = []
    for encoded_comparison in encoded_comparisons:
        column, method = encoded_comparison.split(":")
        if method == "exact":
            comparisons.append(cl.ExactMatch(column))
        elif method == "name":
            comparisons.append(cl.NameComparison(column))
        elif method == "dob":
            comparisons.append(cl.DateOfBirthComparison(column))
        elif method == "levenshtein":
            comparisons.append(cl.LevenshteinAtThresholds(column))
        else:
            raise ValueError(f"Unknown comparison method {method}")
    # TODO: check both datasets contain all the columns

    # Create the Splink linker in dedupe mode
    settings = SettingsCreator(
        link_type="link_only" if link_only else "link_and_dedupe",
        blocking_rules_to_generate_predictions=[],
        comparisons=comparisons,
        probability_two_random_records_match=float(
            os.environ["PROBABILITY_TWO_RANDOM_RECORDS_MATCH"]
        ),
        retain_intermediate_calculation_columns=True,
    )

    grouped = (
        pd.read_parquet(block_dir / "records.parquet")
        .rename(columns={"Input Record ID": "unique_id"})
        .groupby("Input Record Dataset")
    )

    from splink import DuckDBAPI

    db_api = DuckDBAPI()
    linker = Linker(
        [df for _, df in grouped],
        settings,
        db_api=db_api,
        input_table_aliases=[name for name, _ in grouped],
    )

    linker.training.estimate_u_using_random_sampling(max_pairs=5e6, seed=1234)

    blocking_rules_for_training = os.environ["BLOCKING_RULES_FOR_TRAINING"].split(",")

    for blocking_rule_for_training in blocking_rules_for_training:
        linker.training.estimate_parameters_using_expectation_maximisation(
            blocking_rule_for_training
        )

    chart_path = diagnostics_dir / f"match_weights_chart_{block_dir}.html"
    chart_path.parent.mkdir(exist_ok=True, parents=True)
    linker.visualisations.match_weights_chart().save(chart_path)

    # Copied/adapted from https://github.com/moj-analytical-services/splink/blob/3eb1921eaff6b8471d3ebacd3238eb514f62c844/splink/internals/linker_components/inference.py#L264-L293
    from splink.internals.pipeline import CTEPipeline
    from splink.internals.vertically_concatenate import compute_df_concat_with_tf

    pipeline = CTEPipeline()

    # In duckdb, calls to random() in a CTE pipeline cause problems:
    # https://gist.github.com/RobinL/d329e7004998503ce91b68479aa41139
    pairs = (
        pd.read_parquet(block_dir / "pairs.parquet")
        .assign(
            join_key_l=lambda df: df["Left Record Dataset"]
            + "-__-"
            + df["Left Record ID"].astype(int).astype(str),
            join_key_r=lambda df: df["Right Record Dataset"]
            + "-__-"
            + df["Right Record ID"].astype(int).astype(str),
        )
        .drop(
            columns=[
                "Left Record Dataset",
                "Left Record ID",
                "Right Record Dataset",
                "Right Record ID",
            ]
        )
        .assign(match_key=0)
    )  # What is this?
    db_api._table_registration(pairs, "__splink__blocked_id_pairs")
    df_concat_with_tf = compute_df_concat_with_tf(linker, pipeline)
    pipeline = CTEPipeline(
        [
            db_api.table_to_splink_dataframe(
                "__splink__blocked_id_pairs", "__splink__blocked_id_pairs"
            ),
            df_concat_with_tf,
        ]
    )

    from splink.internals.comparison_vector_values import (
        compute_comparison_vector_values_from_id_pairs_sqls,
    )

    sqls = compute_comparison_vector_values_from_id_pairs_sqls(
        linker._settings_obj._columns_to_select_for_blocking,
        linker._settings_obj._columns_to_select_for_comparison_vector_values,
        input_tablename_l="__splink__df_concat_with_tf",
        input_tablename_r="__splink__df_concat_with_tf",
        source_dataset_input_column=linker._settings_obj.column_info_settings.source_dataset_input_column,
        unique_id_input_column=linker._settings_obj.column_info_settings.unique_id_input_column,
    )
    pipeline.enqueue_list_of_sqls(sqls)

    from splink.internals.predict import (
        predict_from_comparison_vectors_sqls_using_settings,
    )

    sqls = predict_from_comparison_vectors_sqls_using_settings(
        linker._settings_obj,
        float(os.getenv("THRESHOLD_MATCH_PROBABILITY", 0)),
        threshold_match_weight=None,
        sql_infinity_expression=linker._infinity_expression,
    )
    pipeline.enqueue_list_of_sqls(sqls)

    predictions = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    linker._predict_warning()

    all_predictions.append(predictions.as_pandas_dataframe())

comparisons_path = diagnostics_dir / f"comparisons_chart_{block_dir}.html"
comparisons_path.parent.mkdir(exist_ok=True, parents=True)
linker.visualisations.comparison_viewer_dashboard(
    predictions, comparisons_path, overwrite=True
)

all_predictions = pd.concat(all_predictions, ignore_index=True)[
    [
        "source_dataset_l",
        "unique_id_l",
        "source_dataset_r",
        "unique_id_r",
        "match_probability",
    ]
].rename(
    columns={
        "source_dataset_l": "Left Record Dataset",
        "unique_id_l": "Left Record ID",
        "source_dataset_r": "Right Record Dataset",
        "unique_id_r": "Right Record ID",
        "match_probability": "Probability",
    }
)
print(all_predictions)
all_predictions.to_parquet(output_path)
