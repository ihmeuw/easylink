"""
=========================
Data Validation Utilities
=========================

This module contains utility functions for validating datasets, e.g. the validation
function(s) for processed data being passed out of one pipeline step and into the next.

"""

from pathlib import Path

import pandas as pd
from pandas.api.types import is_integer_dtype
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


def _validate_required_columns(filepath: str, required_columns: set[str]) -> None:
    """
    Validates that the file at `filepath` contains all columns in `required_columns`.

    Parameters
    ----------
    filepath : str
        The path to the file to validate.
    required_columns : set[str]
        The set of required column names.

    Raises
    ------
    NotImplementedError
        If the file type is not supported.
    LookupError
        If any required columns are missing.
    """
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


def _validate_unique_column_set(df: pd.DataFrame, columns: set[str], filepath: str) -> None:
    """
    Validates that the combination of columns in `columns` is unique in the DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to validate.
    columns : set[str]
        The set of column names to check for uniqueness as a group.
    filepath : str
        The path to the file being validated.

    Raises
    ------
    ValueError
        If duplicate rows exist for the given columns.
    """
    if len(df[list(columns)].drop_duplicates()) < len(df):
        raise ValueError(
            f"Data file {filepath} contains duplicate rows with the same values for {columns}."
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
    """
    Validates a dataset or clusters file based on its filename.

    Parameters
    ----------
    filepath : str
        The path to the input file.

    Raises
    ------
    LookupError, ValueError
        If the file fails validation as a dataset or clusters file.
    """
    filepath = Path(filepath)
    if "clusters" in filepath.stem:
        validate_clusters(filepath)
    else:
        validate_dataset(filepath)


def validate_dataset(filepath: str) -> None:
    """Validates a dataset file.

    - Must be in a tabular format and contain a "Record ID" column.
    - The "Record ID" column must have unique integer values.

    Parameters
    ----------
    filepath : str
        The path to the input dataset file.

    Raises
    ------
    LookupError
        If the file is missing the required "Record ID" column.
    ValueError
        If the "Record ID" column is not unique or not integer dtype.
    """
    _validate_required_columns(filepath, {"Record ID"})
    df = _read_file(filepath)
    _validate_unique_column(df, "Record ID", filepath)

    if not is_integer_dtype(df["Record ID"]):
        raise ValueError(
            f"Data file {filepath} contains non-integer values in the 'Record ID' column."
        )


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
        If the "Record ID" column is not unique in any file or if a non-file is present.
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

    - The file must contain three columns: "Input Record Dataset", "Input Record ID", and "Cluster ID".
    - "Input Record Dataset" and "Input Record ID", considered as a pair, must have unique values.

    Parameters
    ----------
    filepath : str
        The path to the file containing cluster data.

    Raises
    ------
    LookupError
        If the file is missing required columns.
    ValueError
        If the ("Input Record Dataset", "Input Record ID") pair is not unique.
    """
    _validate_required_columns(
        filepath, {"Input Record Dataset", "Input Record ID", "Cluster ID"}
    )
    df = _read_file(filepath)
    _validate_unique_column_set(df, {"Input Record Dataset", "Input Record ID"}, filepath)


def validate_links(filepath: str) -> None:
    """Validates a file containing link information.

    - The file must contain five columns: "Left Record Dataset", "Left Record ID", "Right Record Dataset", "Right Record ID", and "Probability".
    - "Left Record ID" and "Right Record ID" cannot be equal in a row where "Left Record Dataset" also equals "Right Record Dataset".
    - Rows must be unique, ignoring the Probability column.
    - "Left Record Dataset" must be alphabetically before (or equal to) "Right Record Dataset."
    - "Left Record ID" must be less than "Right Record ID" if "Left Record Dataset" equals "Right Record Dataset".
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
        - "Left Record ID" equals "Right Record ID" in any row where datasets match.
        - Duplicate rows exist with the same "Left Record Dataset", "Left Record ID", "Right Record Dataset", and "Right Record ID".
        - "Left Record Dataset" is not alphabetically before or equal to "Right Record Dataset".
        - "Left Record ID" is not less than "Right Record ID" when datasets match.
        - Values in the "Probability" column are not between 0 and 1 (inclusive).
    """
    _validate_required_columns(
        filepath,
        {
            "Left Record Dataset",
            "Left Record ID",
            "Right Record Dataset",
            "Right Record ID",
            "Probability",
        },
    )
    df = _read_file(filepath)

    _validate_pairs(df, filepath)

    if not df["Probability"].between(0, 1).all():
        raise ValueError(
            f"Data file {filepath} contains values in the 'Probability' column that are not between 0 and 1 (inclusive)."
        )


def _validate_pairs(df: pd.DataFrame, filepath: str) -> None:
    """
    Validates pairs in a DataFrame for link or pairs files.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to validate.
    filepath : str
        The path to the file being validated.

    Raises
    ------
    ValueError
        If any validation rule for pairs is violated.
    """
    if (
        (df["Left Record Dataset"] == df["Right Record Dataset"])
        & (df["Left Record ID"] == df["Right Record ID"])
    ).any():
        raise ValueError(
            f"Data file {filepath} contains rows where 'Left Record ID' is equal to 'Right Record ID' and 'Left Record Dataset' is equal to 'Right Record Dataset'."
        )

    _validate_unique_column_set(
        df,
        {"Left Record Dataset", "Left Record ID", "Right Record Dataset", "Right Record ID"},
        filepath,
    )

    if not all(df["Left Record Dataset"] <= df["Right Record Dataset"]):
        raise ValueError(
            f"Data file {filepath} contains rows where 'Left Record Dataset' is not alphabetically before or equal to 'Right Record Dataset'."
        )

    if not all(
        (df["Left Record ID"] < df["Right Record ID"])
        | (df["Left Record Dataset"] != df["Right Record Dataset"])
    ):
        raise ValueError(
            f"Data file {filepath} contains rows where 'Left Record ID' is not less than 'Right Record ID', though the records are from the same dataset."
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
    - Two columns must be called "Input Record Dataset" and "Input Record ID" and they must have unique values as a pair.

    Parameters
    ----------
    filepath : str
        The path to the file containing records.

    Raises
    ------
    LookupError
        If required columns are missing.
    ValueError
        If the ("Input Record Dataset", "Input Record ID") pair is not unique.
    """
    _validate_required_columns(filepath, {"Input Record Dataset", "Input Record ID"})
    df = _read_file(filepath)
    _validate_unique_column_set(df, {"Input Record Dataset", "Input Record ID"}, filepath)


def validate_blocks(filepath: str) -> None:
    """
    Validates a directory containing blocks.

    Each block subdirectory must contain exactly two files: a records file and a pairs file, both in tabular format.

    Validation checks include:
    - The parent directory must exist and be a directory.
    - Each block subdirectory must contain exactly one records file (filename contains "records") and one pairs file (filename contains "pairs").
    - The records file must have columns "Input Record Dataset" and "Input Record ID" with unique pairs.
    - The pairs file must have columns "Left Record Dataset", "Left Record ID", "Right Record Dataset", and "Right Record ID".
    - All values in ("Left Record Dataset", "Left Record ID") and ("Right Record Dataset", "Right Record ID") must exist in the records file.
    - No row in the pairs file may have "Left Record Dataset" == "Right Record Dataset" and "Left Record ID" == "Right Record ID".
    - All rows in the pairs file must be unique with respect to ("Left Record Dataset", "Left Record ID", "Right Record Dataset", "Right Record ID").
    - "Left Record Dataset" must be alphabetically before or equal to "Right Record Dataset".
    - "Left Record ID" must be less than "Right Record ID" if datasets match.
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
            - ("Input Record Dataset", "Input Record ID") is not unique in the records file.
            - ("Left Record Dataset", "Left Record ID") or ("Right Record Dataset", "Right Record ID") in the pairs file do not exist in the records file.
            - "Left Record Dataset" == "Right Record Dataset" and "Left Record ID" == "Right Record ID" in any row of the pairs file.
            - Duplicate rows exist in the pairs file.
            - "Left Record Dataset" is not alphabetically before or equal to "Right Record Dataset" in any row.
            - "Left Record ID" is not less than "Right Record ID" when datasets match.
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
        _validate_required_columns(records_file, {"Input Record Dataset", "Input Record ID"})
        records_df = _read_file(records_file)
        _validate_unique_column_set(
            records_df, {"Input Record Dataset", "Input Record ID"}, records_file
        )

        # Validate pairs file
        _validate_required_columns(
            pairs_file,
            {
                "Left Record Dataset",
                "Left Record ID",
                "Right Record Dataset",
                "Right Record ID",
            },
        )
        pairs_df = _read_file(pairs_file)

        # Check that all (dataset, ID) tuples in pairs exist in records
        record_tuples = set(
            records_df[["Input Record Dataset", "Input Record ID"]].itertuples(
                index=False, name=None
            )
        )
        missing_left = (
            set(
                pairs_df[["Left Record Dataset", "Left Record ID"]].itertuples(
                    index=False, name=None
                )
            )
            - record_tuples
        )
        missing_right = (
            set(
                pairs_df[["Right Record Dataset", "Right Record ID"]].itertuples(
                    index=False, name=None
                )
            )
            - record_tuples
        )
        if missing_left or missing_right:
            raise ValueError(
                f"In block {block_dir}, pairs file {pairs_file} contains records not found in records file {records_file}. "
                f"Missing left records: {missing_left}, missing right records: {missing_right}"
            )

        _validate_pairs(pairs_df, pairs_file)


def validate_dir(filepath: str) -> None:
    """
    Validates that the given path is a directory.

    Parameters
    ----------
    filepath : str
        The path to check.

    Raises
    ------
    NotADirectoryError
        If the path is not a directory.
    """
    input_path = Path(filepath)
    if not input_path.is_dir():
        raise NotADirectoryError(f"The path {filepath} is not a directory.")


def validate_dataset_dir(filepath: str) -> None:
    """
    Validates a directory containing a single dataset file.

    Parameters
    ----------
    filepath : str
        The path to the directory.

    Raises
    ------
    NotADirectoryError
        If the path is not a directory.
    ValueError
        If the directory contains more than one file.
    FileNotFoundError
        If the directory does not contain any files.
    """
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
