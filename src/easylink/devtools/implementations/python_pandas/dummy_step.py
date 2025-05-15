import glob
import logging
import os

import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[logging.StreamHandler()],
)


def load_file(file_path, file_format=None):
    if file_format is None:
        file_format = file_path.split(".")[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    if file_format == "csv":
        return pd.read_csv(file_path)
    raise ValueError()


INPUT_ENV_VARS = os.getenv(
    "INPUT_ENV_VARS",
    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
).split(",")

diagnostics = {}
df = pd.DataFrame()

for env_var in INPUT_ENV_VARS:
    if env_var not in os.environ:
        logging.error(f"Missing required environment variable {env_var}")
        raise ValueError()

    logging.info(f"Loading files for {env_var}")
    file_paths = os.environ[env_var].split(",")
    diagnostics[f"num_files_{env_var.lower()}"] = len(file_paths)
    for path in file_paths:
        df = pd.concat([df, load_file(path)], ignore_index=True).fillna(0)


extra_implementation_specific_input_glob = glob.glob(
    "/extra_implementation_specific_input_data/input*"
)
extra_implementation_specific_input_file_path = os.getenv(
    "DUMMY_CONTAINER_EXTRA_IMPLEMENTATION_SPECIFIC_INPUT_FILE_PATH",
    (
        extra_implementation_specific_input_glob[0]
        if len(extra_implementation_specific_input_glob) > 0
        else None
    ),
)
diagnostics["extra_implementation_specific_input"] = (
    extra_implementation_specific_input_file_path is not None
)
if extra_implementation_specific_input_file_path is not None:
    logging.info("Loading extra, implementation-specific input")
    df = pd.concat(
        [df, load_file(extra_implementation_specific_input_file_path)], ignore_index=True
    ).fillna(0)

logging.info(f"Total input length is {len(df)}")

broken = os.getenv("DUMMY_CONTAINER_BROKEN", "false").lower() in ("true", "yes", "1")
diagnostics["broken"] = broken
if broken:
    df = df.rename(
        columns={
            "foo": "wrong",
            "bar": "column",
            "counter": "names",
        }
    )
else:
    increment = int(os.getenv("DUMMY_CONTAINER_INCREMENT", "1"))
    diagnostics["increment"] = increment
    logging.info(f"Increment is {increment}")
    df["counter"] = df.counter + increment

    added_columns_existing = [c for c in df.columns if "added_column_" in c]
    diagnostics["added_columns_existing"] = added_columns_existing
    added_columns_existing_ints = [int(c.split("_")[-1]) for c in added_columns_existing]
    max_added_column = max(added_columns_existing_ints, default=0) + increment
    min_added_column = max(max_added_column - 4, 0)
    added_columns_desired = range(min_added_column, max_added_column + 1)
    added_columns_desired_names = [f"added_column_{i}" for i in added_columns_desired]
    diagnostics["added_columns_desired_names"] = added_columns_desired_names
    diagnostics["new_columns"] = []
    for column_index, column_name in zip(added_columns_desired, added_columns_desired_names):
        if column_name not in df.columns:
            diagnostics["new_columns"].append(column_name)
            df[column_name] = column_index

    columns_to_drop = [
        c for c in df.columns if "added_column_" in c and c not in added_columns_desired_names
    ]
    diagnostics["columns_to_drop"] = columns_to_drop
    df.drop(columns=columns_to_drop, inplace=True)

output_file_format = os.getenv("DUMMY_CONTAINER_OUTPUT_FILE_FORMAT", "parquet")
output_file_paths = os.getenv(
    "DUMMY_CONTAINER_OUTPUT_PATHS", f"/results/result.{output_file_format}"
).split(",")

diagnostics["num_output_files"] = len(output_file_paths)
diagnostics["output_file_paths"] = output_file_paths

for output_file_path in output_file_paths:
    logging.info(f"Writing output to {output_file_path} in {output_file_format} format")
    if output_file_format == "parquet":
        df.to_parquet(output_file_path)
    elif output_file_format == "csv":
        df.to_csv(output_file_path, index=False)
    else:
        raise ValueError()

diagnostics_dir = os.getenv("DUMMY_CONTAINER_DIAGNOSTICS_DIRECTORY", "/diagnostics")
try:
    with open(f"{diagnostics_dir}/diagnostics.yaml", "w") as f:
        yaml.dump(diagnostics, f, default_flow_style=False)
except (PermissionError, OSError):
    pass
