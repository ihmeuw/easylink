"""
=========================
Data Validation Utilities
=========================

This module contains utility functions for validating datasets, e.g. the validation
function(s) for processed data being passed out of one pipeline step and into the next.

"""

from pathlib import Path

import pandas as pd
from pyarrow import parquet as pq


def validate_input_file_dummy(filepath: str) -> None:
    """Validates an input file to a dummy :class:`~easylink.step.Step`.

    This function is intended to be used as the :attr:`~easylink.graph_components.InputSlot.validator`
    for _all_ input data at every step in the dummy/:mod:`easylink.pipeline_schema_constants.development`
    pipeline schema. It simply checks for supported file types as well as the presence
    of required columns.

    Parameters
    ----------
    filepath
        The path to the input data file to be validated.

    Raises
    ------
    NotImplementedError
        If the file type is not supported.
    LookupError
        If the file is missing required columns.
    """
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
