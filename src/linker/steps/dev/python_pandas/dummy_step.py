import pandas as pd, numpy as np
import os, glob, logging

logging_handlers = [logging.StreamHandler()]
logging_dir = os.getenv("DUMMY_CONTAINER_LOGGING_DIRECTORY", "/extra_implementation_specific_results/")

try:
    log_file_path = logging_dir + 'out.log'
    open(log_file_path, 'a').close()
    logging_handlers += [logging.FileHandler(log_file_path)]
except (PermissionError, OSError):
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    handlers=logging_handlers,
)


def load_file(file_path, file_format=None):
    if file_format is None:
        file_format = file_path.split('.')[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    if file_format == "csv":
        return pd.read_csv(file_path)
    raise ValueError()


if "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS" in os.environ:    
    main_input_file_paths = os.environ["DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS"].split(",")
else:
    main_input_file_paths = glob.glob("/input_data/main_input*")

logging.info('Loading main input')
df = load_file(main_input_file_paths[0])

for path in main_input_file_paths[1:]:
    logging.info('Loading additional primary input')
    df = pd.concat([df, load_file(path)], ignore_index=True).fillna(0)

if "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS" in os.environ:    
    secondary_input_file_paths = os.environ["DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS"].split(",")
else:
    secondary_input_file_paths = glob.glob("/input_data/secondary_input*")

for path in secondary_input_file_paths:
    logging.info('Loading secondary input')
    df = pd.concat([df, load_file(path)], ignore_index=True).fillna(0)

extra_implementation_specific_input_glob = glob.glob("/extra_implementation_specific_input_data/input*")
extra_implementation_specific_input_file_path = os.getenv(
    "DUMMY_CONTAINER_EXTRA_IMPLEMENTATION_SPECIFIC_INPUT_FILE_PATH",
    extra_implementation_specific_input_glob[0] if len(extra_implementation_specific_input_glob) > 0 else None
)
if extra_implementation_specific_input_file_path is not None:
    logging.info('Loading extra, implementation-specific input')
    df = pd.concat([df, load_file(extra_implementation_specific_input_file_path)], ignore_index=True).fillna(0)

logging.info(f'Total input length is {len(df)}')

broken = os.getenv("DUMMY_CONTAINER_BROKEN", "false").lower() in ('true', 'yes', '1')
if broken:
    df = df.rename(columns={
        'foo': 'wrong',
        'bar': 'column',
        'counter': 'names',
    })
else:
    increment = int(os.getenv("DUMMY_CONTAINER_INCREMENT", "1"))
    logging.info(f'Increment is {increment}')
    df['counter'] = df.counter + increment

    max_added_column = max((int(c.split('_')[-1]) for c in df.columns if 'added_column_' in c), default=0) + increment
    min_added_column = max(max_added_column - 4, 0)
    added_columns_desired = range(min_added_column, max_added_column + 1)
    added_columns_desired_names = [f'added_column_{i}' for i in added_columns_desired]
    for column_index, column_name in zip(added_columns_desired, added_columns_desired_names):
        if column_name not in df.columns:
            df[column_name] = column_index

    columns_to_drop = [
        c for c in df.columns if 'added_column_' in c and c not in added_columns_desired_names
    ]
    df.drop(columns=columns_to_drop, inplace=True)

output_file_format = os.getenv("DUMMY_CONTAINER_OUTPUT_FILE_FORMAT", "parquet")
output_file_paths = os.getenv("DUMMY_CONTAINER_OUTPUT_PATHS", f"/results/result.{output_file_format}").split(",")

for output_file_path in output_file_paths:
    logging.info(f'Writing output to {output_file_path} in {output_file_format} format')
    if output_file_format == "parquet":
        df.to_parquet(output_file_path)
    elif output_file_format == "csv":
        df.to_csv(output_file_path, index=False)
    else:
        raise ValueError()