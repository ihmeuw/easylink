"""
==========================
Data Aggregating Utilities
==========================

This module contains utility functions for aggregating datasets. One primary use 
case for this is to combine the results of sections that were automatically run
in parallel.

Note that it is critical that all data aggregating utility functions are definied
in this module; easylink will not be able to find them otherwise.

"""

import pandas as pd
from loguru import logger


def concatenate_datasets(input_files: list[str], output_filepath: str) -> None:
    """Concatenates multiple datasets into a single one.

    Parameters
    ----------
    input_files
        A list of input file paths to be concatenated.
    output_filepath
        The output filepath.
    """
    logger.info(f"Concatenating {len(input_files)} datasets")
    dfs = [pd.read_parquet(df) for df in input_files]
    df = pd.concat(dfs, ignore_index=True)
    df.to_parquet(output_filepath)
