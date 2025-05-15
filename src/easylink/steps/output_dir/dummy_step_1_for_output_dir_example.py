# PIPELINE_SCHEMA: output_dir
# STEP_NAME: step_1_for_output_dir_example
# REQUIREMENTS: pandas==2.1.2 pyarrow

import os
from pathlib import Path

import pandas as pd

data = pd.read_parquet(os.environ["STEP_1_MAIN_INPUT_FILE_PATHS"])

print(data)

dir_path = Path(os.environ["DUMMY_CONTAINER_OUTPUT_PATHS"])
dir_path.mkdir(parents=True, exist_ok=True)

for i in range(3):
    data.to_parquet(dir_path / f"result_{i}.parquet")
