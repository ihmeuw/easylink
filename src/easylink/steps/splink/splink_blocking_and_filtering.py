# STEP_NAME: blocking_and_filtering
# REQUIREMENTS: pandas pyarrow splink==4.0.7

import os

import pandas as pd

import pdb

records = pd.read_parquet(os.environ["RECORDS_FILE_PATH"])

# DUMMY_CONTAINER_OUTPUT_PATHS is a single path to a directory ('dataset')
results_dir = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

import splink

blocking_rules = os.environ["BLOCKING_RULES"].split(",")

link_only = os.getenv("LINK_ONLY", "false").lower() in ("true", "yes", "1")

from splink import Linker, SettingsCreator, DuckDBAPI

# Create the Splink linker in dedupe mode
settings = SettingsCreator(
    link_type="link_only" if link_only else "dedupe_only",
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=[],
)
from splink import DuckDBAPI
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

if link_only:
    df_list = [
        df
        for _, df in records.rename(columns={"Input Record ID": "unique_id"}).groupby(
            "dataset"
        )
    ]
else:
    df_list = [records.rename(columns={"Input Record ID": "unique_id"})]

db_api = DuckDBAPI()
linker = Linker(df_list, settings, db_api=db_api)

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
    .rename(
        columns={
            "join_key_l": "Left Record ID",
            "join_key_r": "Right Record ID",
        }
    )
    .drop(columns=["match_key"])
)

print(blocked_pairs)

from pathlib import Path

output_path = Path(results_dir) / "block_0"
output_path.mkdir(exist_ok=True, parents=True)

records.to_parquet(output_path / "records.parquet", index=False)
blocked_pairs.to_parquet(output_path / "pairs.parquet", index=False)

# workaround until dataset column is ready - only works for specific dataset names
if all(str(id).count("_") >= 3 for id in records["Input Record ID"]):
    records["source_dataset"] = records["Input Record ID"].str.rsplit(
        "_", n=1, expand=True
    )[0]
    db_api = DuckDBAPI()
    diagnostics_dir = Path(os.environ["DUMMY_CONTAINER_DIAGNOSTICS_DIRECTORY"])
    chart_path = diagnostics_dir / f"blocking_cumulative_comparisons_chart_block_0.png"
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
        table_or_tables=records,
        blocking_rules=blocking_rules,
        db_api=db_api,
        link_type="link_only",
        unique_id_column_name="Input Record ID",
        source_dataset_column_name="source_dataset",  # TBD change to dataset column when that's ready
    ).save(chart_path)
