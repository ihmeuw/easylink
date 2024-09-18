from pathlib import Path

import pandas as pd
from pyarrow import parquet as pq


def validate_input_file_dummy(filepath: str) -> None:
    extension = Path(filepath).suffix
    if extension == ".parquet":
        output_columns = set(pq.ParquetFile(filepath).schema.names)
    elif extension == ".csv":
        output_columns = set(pd.read_csv(filepath).columns)
    else:
        raise NotImplementedError(
            f"Data file type {extension} is not supported. Convert to Parquet or CSV instead"
        )

    required_columns = {"foo", "bar", "counter"}
    missing_columns = required_columns - output_columns
    if missing_columns:
        raise LookupError(
            f"Data file {filepath} is missing required column(s) {missing_columns}"
        )


DEMO_SAMPLE_PATH = (
    Path(__file__).parent.parent.parent.parent
    / "sample_data/pvs_like_case_study/simulated_census_2030.parquet"
)


def demo_validator(filepath: str) -> None:
    if Path(filepath).name == "simulated_geobase_reference_file.parquet":
        return

    sample_df = pd.read_parquet(DEMO_SAMPLE_PATH)
    extension = Path(filepath).suffix
    if not extension == ".parquet":
        raise NotImplementedError(
            f"Data file type {extension} is not supported. Convert to Parquet instead"
        )
    df_to_validate = pd.read_parquet(filepath)

    val_cols = set(df_to_validate.columns)
    expected_cols = set(sample_df.columns)
    if val_cols.symmetric_difference(expected_cols) - {"pik"}:
        err_str = f"Data file {filepath}"
        excess_cols = val_cols.difference(expected_cols) - {"pik"}
        missing_cols = expected_cols.difference(val_cols)
        if excess_cols:
            err_str += f" has excess columns {excess_cols}"
            if missing_cols:
                err_str += " and"
        if missing_cols:
            err_str += f" is missing columns {missing_cols}"

        raise LookupError(err_str)

    if len(df_to_validate.index) != len(sample_df.index):
        raise ValueError(
            f"Data file {filepath} has {len(df_to_validate.index)} rows but {len(sample_df.index)} are expected."
        )
