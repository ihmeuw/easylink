import functools
import os
import sys
from bdb import BdbQuit
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TextIO

import pandas as pd
import yaml
from loguru import logger
from pyarrow import parquet as pq


def handle_exceptions(
    func: Callable, exceptions_logger: Any, with_debugger: bool
) -> Callable:
    """Drops a user into an interactive debugger if func raises an error."""

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (BdbQuit, KeyboardInterrupt):
            raise
        except Exception as e:
            exceptions_logger.exception("Uncaught exception {}".format(e))
            if with_debugger:
                import pdb
                import traceback

                traceback.print_exc()
                pdb.post_mortem()
            raise

    return wrapped


def configure_logging_to_terminal(verbose: int):
    """Sets up logging to ``sys.stdout``.

    Parameters
    ----------
    verbose
        Verbosity of the logger.

    """
    logger.remove(0)  # Clear default configuration
    _add_logging_sink(sys.stdout, verbose, colorize=True)


def _add_logging_sink(
    sink: TextIO, verbose: int, colorize: bool = False, serialize: bool = False
):
    """Adds a logging sink to the global process logger.

    Parameters
    ----------
    sink
        Either a file or system file descriptor like ``sys.stdout``.
    verbose
        Verbosity of the logger.
    colorize
        Whether to use the colorization options from :mod:`loguru`.
    serialize
        Whether the logs should be converted to JSON before they're dumped
        to the logging sink.

    """
    message_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <green>{elapsed}</green> | "
        "<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    if verbose == 0:
        logger.add(
            sink,
            colorize=colorize,
            level="INFO",
            format=message_format,
            serialize=serialize,
        )
    elif verbose >= 1:
        logger.add(
            sink,
            colorize=colorize,
            level="DEBUG",
            format=message_format,
            serialize=serialize,
        )


def create_results_directory(output_dir: Optional[str], timestamp: bool) -> Path:
    results_dir = Path("results" if output_dir is None else output_dir).resolve()
    if timestamp:
        launch_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        results_dir = results_dir / launch_time
    _ = os.umask(0o002)
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "intermediate").mkdir(exist_ok=True)
    (results_dir / "diagnostics").mkdir(exist_ok=True)

    return results_dir


def load_yaml(filepath: Path) -> Dict:
    with open(filepath, "r") as file:
        data = yaml.safe_load(file)
    return data


def dummy_output_validator(filepath: Path) -> None:
    extension = filepath.suffix
    if extension == ".parquet":
        output_columns = set(pq.ParquetFile(filepath).schema.names)
    elif extension == ".csv":
        output_columns = set(pd.read_csv(filepath).columns)
    else:
        raise NotImplementedError(
            f"Data file type {extension} is not compatible. Convert to Parquet or CSV instead"
        )

    required_columns = {"foo", "bar", "counter"}
    missing_columns = required_columns - output_columns
    if missing_columns:
        raise RuntimeError(
            f"Data file {filepath} is missing required column(s) {missing_columns}"
        )


def input_file_validator(filepath: Path) -> None:
    "Wrap the output file validator for now, since it is the same"
    return dummy_output_validator(filepath)
