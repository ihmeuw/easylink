# PIPELINE_SCHEMA: output_dir
# STEP_NAME: step_2_for_output_dir_example
# REQUIREMENTS: pandas==2.1.2 pyarrow

import os
import shutil
from pathlib import Path

import pandas as pd

dir_path = Path(os.environ["DUMMY_CONTAINER_MAIN_INPUT_DIR_PATH"])
saved = False

for i, f in enumerate([f for f in dir_path.iterdir() if f.is_file()]):
    if "snakemake" in str(f):
        continue

    if not saved:
        shutil.copy(f, os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"])
        saved = True

    print(pd.read_parquet(f))
