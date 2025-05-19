# STEP_NAME: blocking_and_filtering
# REQUIREMENTS: pandas pyarrow splink==4.0.7

import os

import pandas as pd

records = pd.read_parquet(os.environ["RECORDS_FILE_PATH"])

output_path = os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"]

import splink

blocking_rules = os.environ["BLOCKING_RULES"].split(",")

from splink import Linker, SettingsCreator

# Create the Splink linker in dedupe mode
settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=[],
)
from splink import DuckDBAPI

db_api = DuckDBAPI()
linker = Linker(records.rename(
    columns={"Input Record ID": "unique_id"}
), settings, db_api=db_api)

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

output_path = Path(output_path) / "block_0"
output_path.mkdir(exist_ok=True, parents=True)

records.to_parquet(output_path / "records.parquet", index=False)
blocked_pairs.to_parquet(output_path / "pairs.parquet", index=False)
