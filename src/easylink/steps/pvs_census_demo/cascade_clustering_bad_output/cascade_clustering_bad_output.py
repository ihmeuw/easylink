import copy
import json
import os
import re

import jellyfish
import numpy as np
import pandas as pd
from splink.duckdb.linker import DuckDBLinker


def load_file(file_path, file_format=None):
    if file_format is None:
        file_format = file_path.split(".")[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    if file_format == "csv":
        return pd.read_csv(file_path)
    raise ValueError()


census_2030_path = os.environ["SIMULATED_CENSUS"].split(",")[0]
reference_file_path = os.environ["REFERENCE_FILE"].split(",")[1]
census_2030 = load_file(census_2030_path)
reference_file = load_file(reference_file_path)

census_2030_raw_input = census_2030.copy()
