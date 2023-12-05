import glob
import logging

import pandas as pd

logging_handlers = [logging.StreamHandler()]
logging_dir = "/logs/"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=logging_handlers,
)


def load_file(file_path, file_format=None):
    if file_format is None:
        file_format = file_path.split(".")[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    if file_format == "csv":
        return pd.read_csv(file_path)
    raise ValueError()


df = pd.DataFrame()
input_data = glob.glob("/input_data/*")
for path in input_data:
    logging.info(f"Loading input {path}")
    df = pd.concat([df, load_file(path)], ignore_index=True).fillna(0)

logging.info(f"Total input length is {len(df)}")

increment = 1
logging.info(f"Increment is {increment}")
df["counter"] = df.counter + increment

max_added_column = (
    max((int(c.split("_")[-1]) for c in df.columns if "added_column_" in c), default=0)
    + increment
)
min_added_column = max(max_added_column - 4, 0)
added_columns_desired = range(min_added_column, max_added_column + 1)
added_columns_desired_names = [f"added_column_{i}" for i in added_columns_desired]
for column_index, column_name in zip(added_columns_desired, added_columns_desired_names):
    if column_name not in df.columns:
        df[column_name] = column_index

columns_to_drop = [
    c for c in df.columns if "added_column_" in c and c not in added_columns_desired_names
]
df.drop(columns=columns_to_drop, inplace=True)

df.to_parquet("/results/output.parquet")
