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


def validate_input_dataset_or_known_clusters(filepath: str) -> None:
    filepath = Path(filepath)
    if "clusters" in filepath.stem:
        validate_clusters(filepath)
    else:
        validate_dataset(filepath)


def validate_dataset(filepath: str) -> None:
    """Validates an dataset file.

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
        validate_dataset(file.name)


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


def validate_records(filepath: str) -> None:
    """Validates a file containing records.

    - A file in a tabular format.
    - The file may have any number of columns.
    - One column must be called “Input Record ID” and it must have unique values.
    """
    _validate_required_columns(filepath, {"Input Record ID"})
    df = _read_file(filepath)
    _validate_unique_column(df, "Input Record ID", filepath)


def validate_blocks(filepath: str) -> None:
    """
    Validates a directory containing blocks.

    Each block subdirectory must contain exactly two files: a records file and a pairs file, both in tabular format.

    Validation checks include:
    - The parent directory must exist and be a directory.
    - Each block subdirectory must contain exactly one records file (filename contains "records") and one pairs file (filename contains "pairs").
    - The records file must have a column "Input Record ID" with unique values.
    - The pairs file must have columns "Left Record ID" and "Right Record ID".
    - All values in "Left Record ID" and "Right Record ID" must exist in the "Input Record ID" column of the corresponding records file.
    - No row in the pairs file may have "Left Record ID" equal to "Right Record ID".
    - All rows in the pairs file must be unique with respect to ("Left Record ID", "Right Record ID").
    - In each row, "Left Record ID" must be alphabetically less than "Right Record ID".
    - No extra files are allowed in block subdirectories.

    Parameters
    ----------
    filepath : str
        Path to the directory containing block subdirectories.

    Raises
    ------
    NotADirectoryError
        If the provided path is not a directory.
    FileNotFoundError
        If a required records or pairs file is missing in any block.
    LookupError
        If required columns are missing in records or pairs files.
    ValueError
        If:
            - "Input Record ID" is not unique in the records file.
            - "Left Record ID" or "Right Record ID" in the pairs file do not exist in the records file.
            - "Left Record ID" equals "Right Record ID" in any row of the pairs file.
            - Duplicate rows exist in the pairs file.
            - "Left Record ID" is not alphabetically before "Right Record ID" in any row.
            - Extra files are present in a block subdirectory.
    """
    input_path = Path(filepath)

    if not input_path.is_dir():
        raise NotADirectoryError(f"The path {filepath} is not a directory.")

    for block_dir in filter(lambda d: d.is_dir(), input_path.iterdir()):
        files = {file.stem: file for file in block_dir.iterdir() if file.is_file()}
        records_file = next((f for name, f in files.items() if "records" in name), None)
        pairs_file = next((f for name, f in files.items() if "pairs" in name), None)

        if len(files) > 2:
            raise ValueError(f"Extra file(s) found in block directory {block_dir}.")

        if not records_file or not pairs_file:
            raise FileNotFoundError(
                f"Block directory {block_dir} must contain both a records file and a pairs file."
            )

        # Validate records file
        _validate_required_columns(records_file, {"Input Record ID"})
        records_df = _read_file(records_file)
        _validate_unique_column(records_df, "Input Record ID", records_file)
        record_ids = set(records_df["Input Record ID"])

        # Validate pairs file
        _validate_required_columns(pairs_file, {"Left Record ID", "Right Record ID"})
        pairs_df = _read_file(pairs_file)

        # Check that all IDs in pairs exist in records
        missing_left = set(pairs_df["Left Record ID"]) - record_ids
        missing_right = set(pairs_df["Right Record ID"]) - record_ids
        if missing_left or missing_right:
            raise ValueError(
                f"In block {block_dir}, pairs file {pairs_file} contains IDs not found in records file {records_file}: "
                f"Missing Left Record IDs: {missing_left}, Missing Right Record IDs: {missing_right}"
            )

        # Check Left != Right
        if (pairs_df["Left Record ID"] == pairs_df["Right Record ID"]).any():
            raise ValueError(
                f"In block {block_dir}, pairs file {pairs_file} contains rows where 'Left Record ID' equals 'Right Record ID'."
            )

        # Check for duplicate rows
        if pairs_df.duplicated(subset=["Left Record ID", "Right Record ID"]).any():
            raise ValueError(
                f"In block {block_dir}, pairs file {pairs_file} contains duplicate rows."
            )

        # Check alphabetical order
        if not (pairs_df["Left Record ID"] < pairs_df["Right Record ID"]).all():
            raise ValueError(
                f"In block {block_dir}, pairs file {pairs_file} contains rows where 'Left Record ID' is not alphabetically before 'Right Record ID'."
            )


def validate_dir(filepath: str) -> None:
    input_path = Path(filepath)
    if not input_path.is_dir():
        raise NotADirectoryError(f"The path {filepath} is not a directory.")


def validate_dataset_dir(filepath: str) -> None:
    input_path = Path(filepath)
    if not input_path.is_dir():
        raise NotADirectoryError(f"The path {filepath} is not a directory.")

    file_paths = [f for f in input_path.iterdir() if not str(f.stem).startswith(".")]
    if len(file_paths) > 1:
        raise ValueError(f"The directory {input_path} contains more than one file.")
    if len(file_paths) == 0:
        raise FileNotFoundError(f"The directory {input_path} does not contain any files.")

    file_path = file_paths[0]
    validate_dataset(file_path)


def dont_validate(filepath: str) -> None:
    """Placeholder function that performs no validation.

    Parameters
    ----------
    filepath : str
        The path to the file (not used).
    """
    pass
