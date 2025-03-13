"""
========================
Data Splitting Utilities
========================

This module contains utility functions for splitting datasets into smaller datasets.
One primary use case for this is to run sections of the pipeline in an embarrassingly 
parallel manner.

Note that it is critical that all data splitting utility functions are definied
in this module; easylink will not be able to find them otherwise.

"""

import math
import os

import pandas as pd
from loguru import logger


def split_data_by_size(
    input_files: list[str], output_dir: str, desired_chunk_size_mb: int | float
) -> None:
    """Splits the data (from a single input slot) into chunks of desired size.

    This function takes all datasets from a single input slot, concatenates them,
    and then splits the resulting dataset into chunks of the desired size. Note
    that this will split the data as evenly as possible, but the final chunk may
    be smaller than the desired size if the input data does not divide evenly; it
    makes no effort to redistribute the lingering data.

    Parameters
    ----------
    input_files
        A list of input file paths to be concatenated and split.
    output_dir
        The directory where the resulting chunks will be saved.
    desired_chunk_size_mb
        The desired size of each chunk, in megabytes.
    """

    # concatenate all input files
    df = pd.DataFrame()
    input_file_size_mb = 0
    for file in input_files:
        input_file_size_mb += os.path.getsize(file) / 1024**2
        tmp = pd.read_parquet(file)
        df = pd.concat([df, tmp], ignore_index=True)

    # divide df into num_chunks and save each one out
    num_chunks = math.ceil(input_file_size_mb / desired_chunk_size_mb)
    chunk_size = math.ceil(len(df) / num_chunks)
    if num_chunks == 1:
        logger.info(f"Input data is already smaller than desired chunk size; not splitting")
    else:
        logger.info(
            f"Splitting a {round(input_file_size_mb, 2)} MB dataset ({len(df)} rows) into "
            f"into {num_chunks} chunks of size ~{desired_chunk_size_mb} MB each"
        )
    for i in range(num_chunks):
        start = i * chunk_size
        end = (i + 1) * chunk_size
        chunk = df.iloc[start:end]
        chunk_dir = os.path.join(output_dir, f"chunk_{i}")
        if not os.path.exists(chunk_dir):
            os.makedirs(chunk_dir)
        logger.debug(
            f"Writing out chunk {i+1}/{num_chunks} (rows {chunk.index[0]} to "
            f"{chunk.index[-1]})"
        )
        chunk.to_parquet(os.path.join(chunk_dir, "result.parquet"))
