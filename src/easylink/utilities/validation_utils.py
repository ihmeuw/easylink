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


def _read_file(filepath: str) -> pd.DataFrame:
    """Reads a file.

    Parameters
    ----------
    filepath : str
        The path to the file to read.

    Returns
    -------
    pandas.DataFrame
        The loaded DataFrame.

    Raises
    ------
    NotImplementedError
        If the file type is not supported.
    """
    extension = Path(filepath).suffix
    if extension == ".parquet":
        return pd.read_parquet(filepath)
    elif extension == ".csv":
        return pd.read_csv(filepath)
    else:
        raise NotImplementedError(
            f"Data file type {extension} is not supported. Convert to Parquet or CSV instead."
        )


def _validate_required_columns(filepath: str, required_columns: set) -> None:
    extension = Path(filepath).suffix
    if extension == ".parquet":
        output_columns = set(pq.ParquetFile(filepath).schema.names)
    elif extension == ".csv":
        output_columns = set(pd.read_csv(filepath, nrows=5).columns)
    else:
        raise NotImplementedError(
            f"Data file type {extension} is not supported. Convert to Parquet or CSV instead"
        )

    missing_columns = required_columns - output_columns
    if missing_columns:
        raise LookupError(
            f"Data file {filepath} is missing required column(s) {missing_columns}"
        )


def _validate_unique_column(df: pd.DataFrame, column_name: str, filepath: str) -> None:
    """Validates that a column in a DataFrame has unique values.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to validate.
    column_name : str
        The name of the column to check.
    filepath : str
        The path to the file being validated.

    Raises
    ------
    ValueError
        If the column contains duplicate values.
    """
    if not df[column_name].is_unique:
        raise ValueError(
            f"Data file {filepath} contains duplicate values in the '{column_name}' column."
        )


def validate_input_file_dummy(filepath: str) -> None:
    """Validates an input file to a dummy :class:`~easylink.step.Step`.

    The file must contain the columns: "foo", "bar", and "counter".

    Parameters
    ----------
    filepath : str
        The path to the input file.

    Raises
    ------
    LookupError
        If the file is missing required columns.
    """
    _validate_required_columns(filepath, required_columns={"foo", "bar", "counter"})


def validate_input_dataset(filepath: str) -> None:
    """Validates an input dataset file.

    - Must be in a tabular format and contain a "Record ID" column.
    - The "Record ID" column must have unique values.

    Parameters
    ----------
    filepath : str
        The path to the input dataset file.

    Raises
    ------
    LookupError
        If the file is missing the required "Record ID" column.
    ValueError
        If the "Record ID" column is not unique in the file.
    """
    _validate_required_columns(filepath, {"Record ID"})
    df = _read_file(filepath)
    _validate_unique_column(df, "Record ID", filepath)


def validate_datasets_directory(filepath: str) -> None:
    """Validates a directory of input dataset files.

    - Each file in the directory must be in a tabular format and contain a "Record ID" column.
    - The "Record ID" column must have unique values.

    Parameters
    ----------
    filepath : str
        The path to the directory containing input dataset files.

    Raises
    ------
    NotADirectoryError
        If the provided path is not a directory.
    LookupError
        If any file is missing the required "Record ID" column.
    ValueError
        If the "Record ID" column is not unique in any file.
    """
    input_path = Path(filepath)
    if not input_path.is_dir():
        raise NotADirectoryError(f"The path {filepath} is not a directory.")

    for file in input_path.iterdir():
        if not file.is_file():
            raise ValueError(f"The path {file} is not a file.")
        validate_input_dataset(file.name)


def validate_clusters(filepath: str) -> None:
    """Validates a file containing cluster information.

    - The file must contain two columns: "Input Record ID" and "Cluster ID".
    - "Input Record ID" must have unique values.

    Parameters
    ----------
    filepath : str
        The path to the file containing cluster data.

    Raises
    ------
    LookupError
        If the file is missing required columns.
    ValueError
        If the "Input Record ID" column is not unique.
    """
    _validate_required_columns(filepath, {"Input Record ID", "Cluster ID"})
    df = _read_file(filepath)
    _validate_unique_column(df, "Input Record ID", filepath)


def validate_links(filepath: str) -> None:
    """Validates a file containing link information.

    - The file must contain three columns: "Left Record ID", "Right Record ID", and "Probability".
    - "Left Record ID" and "Right Record ID" must not be equal in any row.
    - Rows must be unique.
    - "Left Record ID" must be alphabetically before "Right Record ID".
    - "Probability" values must be between 0 and 1 (inclusive).

    Parameters
    ----------
    filepath : str
        The path to the file containing link data.

    Raises
    ------
    LookupError
        If the file is missing required columns.
    ValueError
        If:
        - "Left Record ID" equals "Right Record ID" in any row.
        - Duplicate rows exist with the same "Left Record ID" and "Right Record ID".
        - "Left Record ID" is not alphabetically before "Right Record ID".
        - Values in the "Probability" column are not between 0 and 1 (inclusive).
    """
    _validate_required_columns(filepath, {"Left Record ID", "Right Record ID", "Probability"})
    df = _read_file(filepath)

    if (df["Left Record ID"] == df["Right Record ID"]).any():
        raise ValueError(
            f"Data file {filepath} contains rows where 'Left Record ID' is equal to 'Right Record ID'."
        )

    if (
        not df[["Left Record ID", "Right Record ID"]].drop_duplicates().shape[0]
        == df.shape[0]
    ):
        raise ValueError(
            f"Data file {filepath} contains duplicate rows with the same 'Left Record ID' and 'Right Record ID'."
        )

    if not all(df["Left Record ID"] < df["Right Record ID"]):
        raise ValueError(
            f"Data file {filepath} contains rows where 'Left Record ID' is not alphabetically before 'Right Record ID'."
        )

    if not df["Probability"].between(0, 1).all():
        raise ValueError(
            f"Data file {filepath} contains values in the 'Probability' column that are not between 0 and 1 (inclusive)."
        )


def validate_ids_to_remove(filepath: str) -> None:
    """Validates a file containing IDs to remove.

    - The file must contain a single column: "Record ID".
    - "Record ID" must have unique values.

    Parameters
    ----------
    filepath : str
        The path to the file containing IDs to remove.

    Raises
    ------
    LookupError
        If the file is missing the "Record ID" column.
    ValueError
        If the "Record ID" column is not unique.
    """
    _validate_required_columns(filepath, {"Record ID"})
    df = _read_file(filepath)
    _validate_unique_column(df, "Record ID", filepath)


def validate_dir(filepath: str) -> None:
    input_path = Path(filepath)
    if not input_path.is_dir():
        raise NotADirectoryError(f"The path {filepath} is not a directory.")


def dont_validate(filepath: str) -> None:
    """Placeholder function that performs no validation.

    Parameters
    ----------
    filepath : str
        The path to the file (not used).
    """
    pass
