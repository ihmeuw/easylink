# STEP_NAME: evaluating_pairs
# REQUIREMENTS: pandas pyarrow

import os
from pathlib import Path

import pandas as pd

blocks_dir = Path(os.environ["BLOCKS_DIR_PATH"])
diagnostics_dir = Path(os.environ["DIAGNOSTICS_DIRECTORY"])
output_path = Path(os.environ["OUTPUT_PATHS"])
Path(output_path).parent.mkdir(exist_ok=True, parents=True)

all_predictions = []

for block_dir in blocks_dir.iterdir():
    if str(block_dir.stem).startswith("."):
        continue

    pairs = pd.read_parquet(block_dir / "pairs.parquet")

    all_predictions.append(pairs.assign(Probability=1.0))

all_predictions = pd.concat(all_predictions, ignore_index=True)
print(all_predictions)
all_predictions.to_parquet(output_path)
