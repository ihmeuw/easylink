# STEP_NAME: blocking_and_filtering
# REQUIREMENTS: pandas pyarrow splink==4.0.7 vl-convert-python

import os

import pandas as pd

records = pd.read_parquet(os.environ["RECORDS_FILE_PATH"])

# OUTPUT_PATHS is a single path to a directory ('dataset')
results_dir = os.environ["OUTPUT_PATHS"]

import splink

blocking_rules = os.environ["BLOCKING_RULES"].split(",")

link_only = os.getenv("LINK_ONLY", "false").lower() in ("true", "yes", "1")

from splink import DuckDBAPI, Linker, SettingsCreator

# Create the Splink linker in dedupe mode
settings = SettingsCreator(
    link_type="link_only" if link_only else "link_and_dedupe",
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=[],
)
from splink import DuckDBAPI
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

grouped = records.rename(columns={"Input Record ID": "unique_id"}).groupby(
    "Input Record Dataset"
)

db_api = DuckDBAPI()
linker = Linker(
    [df for _, df in grouped],
    settings,
    db_api=db_api,
    input_table_aliases=[name for name, _ in grouped],
)

# Copied/adapted from https://github.com/moj-analytical-services/splink/blob/3eb1921eaff6b8471d3ebacd3238eb514f62c844/splink/internals/linker_components/inference.py#L86-L131
from splink.internals.pipeline import CTEPipeline
from splink.internals.vertically_concatenate import compute_df_concat_with_tf

pipeline = CTEPipeline()

# In duckdb, calls to random() in a CTE pipeline cause problems:
# https://gist.github.com/RobinL/d329e7004998503ce91b68479aa41139
df_concat_with_tf = compute_df_concat_with_tf(linker, pipeline)
pipeline = CTEPipeline([df_concat_with_tf])

blocking_input_tablename_l = "__splink__df_concat_with_tf"
blocking_input_tablename_r = "__splink__df_concat_with_tf"

link_type = linker._settings_obj._link_type


# If exploded blocking rules exist, we need to materialise
# the tables of ID pairs
from splink.internals.blocking import materialise_exploded_id_tables

exploding_br_with_id_tables = materialise_exploded_id_tables(
    link_type=link_type,
    blocking_rules=linker._settings_obj._blocking_rules_to_generate_predictions,
    db_api=linker._db_api,
    splink_df_dict=linker._input_tables_dict,
    source_dataset_input_column=linker._settings_obj.column_info_settings.source_dataset_input_column,
    unique_id_input_column=linker._settings_obj.column_info_settings.unique_id_input_column,
)

from splink.internals.blocking import block_using_rules_sqls

sqls = block_using_rules_sqls(
    input_tablename_l=blocking_input_tablename_l,
    input_tablename_r=blocking_input_tablename_r,
    blocking_rules=linker._settings_obj._blocking_rules_to_generate_predictions,
    link_type=link_type,
    source_dataset_input_column=linker._settings_obj.column_info_settings.source_dataset_input_column,
    unique_id_input_column=linker._settings_obj.column_info_settings.unique_id_input_column,
)

pipeline.enqueue_list_of_sqls(sqls)

blocked_pairs = (
    linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    .as_pandas_dataframe()
    .drop(columns=["match_key"])
)

blocked_pairs[["Left Record Dataset", "Left Record ID"]] = (
    blocked_pairs.pop("join_key_l").str.split("-__-", n=1, expand=True)
    if not blocked_pairs.empty
    else pd.DataFrame(columns=["Left Record Dataset", "Left Record ID"])
)

blocked_pairs[["Right Record Dataset", "Right Record ID"]] = (
    blocked_pairs.pop("join_key_r").str.split("-__-", n=1, expand=True)
    if not blocked_pairs.empty
    else pd.DataFrame(columns=["Right Record Dataset", "Right Record ID"])
)

blocked_pairs[["Left Record ID", "Right Record ID"]] = blocked_pairs[
    ["Left Record ID", "Right Record ID"]
].astype(int)

# Now ensure correct ordering
wrong_order_dataset = (
    blocked_pairs["Left Record Dataset"] > blocked_pairs["Right Record Dataset"]
)
id_cols = [
    "Left Record Dataset",
    "Left Record ID",
    "Right Record Dataset",
    "Right Record ID",
]
switched_id_cols = [
    "Right Record Dataset",
    "Right Record ID",
    "Left Record Dataset",
    "Left Record ID",
]
blocked_pairs.loc[wrong_order_dataset, id_cols] = blocked_pairs.loc[
    wrong_order_dataset, switched_id_cols
].values

wrong_order_ids = (
    blocked_pairs["Left Record Dataset"] == blocked_pairs["Right Record Dataset"]
) & (blocked_pairs["Left Record ID"] > blocked_pairs["Right Record ID"])
blocked_pairs.loc[wrong_order_ids, id_cols] = blocked_pairs.loc[
    wrong_order_ids, switched_id_cols
].values
blocked_pairs[["Left Record ID", "Right Record ID"]] = blocked_pairs[
    ["Left Record ID", "Right Record ID"]
].astype(int)

print(blocked_pairs)

from pathlib import Path

output_path = Path(results_dir) / "block_0"
output_path.mkdir(exist_ok=True, parents=True)

records.to_parquet(output_path / "records.parquet", index=False)
blocked_pairs.to_parquet(output_path / "pairs.parquet", index=False)

records["unique_id"] = (
    str(records["Input Record Dataset"]) + "_" + str(records["Input Record ID"])
)
db_api = DuckDBAPI()
diagnostics_dir = Path(os.environ["DIAGNOSTICS_DIRECTORY"])
chart_path = diagnostics_dir / f"blocking_cumulative_comparisons_chart_block_0.png"
cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    table_or_tables=records,
    blocking_rules=blocking_rules,
    db_api=db_api,
    link_type=link_type,
    unique_id_column_name="unique_id",
    source_dataset_column_name="Input Record Dataset",
).save(chart_path)
