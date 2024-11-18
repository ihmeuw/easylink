import os
import shutil
from datetime import datetime
from pathlib import Path

import yaml


def modify_umask(func):
    """Decorator to wrap umask modification before making directories"""

    def wrapper(*args, **kwargs):
        old_umask = os.umask(0o002)
        try:
            return func(*args, **kwargs)
        finally:
            os.umask(old_umask)

    return wrapper


@modify_umask
def create_results_directory(results_dir: Path) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)


@modify_umask
def create_results_intermediates(results_dir: Path) -> None:
    (results_dir / "intermediate").mkdir(exist_ok=True)
    (results_dir / "diagnostics").mkdir(exist_ok=True)


def copy_configuration_files_to_results_directory(
    pipeline_specification: Path,
    input_data: Path,
    computing_environment: Path | None,
    results_dir: Path,
) -> None:
    shutil.copy(pipeline_specification, results_dir)
    shutil.copy(input_data, results_dir)
    if computing_environment:
        shutil.copy(computing_environment, results_dir)


def get_results_directory(output_dir: str | None, timestamp: bool) -> Path:
    results_dir = Path("results" if output_dir is None else output_dir).resolve()
    if timestamp:
        launch_time = _get_timestamp()
        results_dir = results_dir / launch_time
    return results_dir


def _get_timestamp():
    return datetime.now().strftime("%Y_%m_%d_%H_%M_%S")


def load_yaml(filepath: str | Path) -> dict:
    with open(filepath, "r") as file:
        data = yaml.safe_load(file)
    return data
