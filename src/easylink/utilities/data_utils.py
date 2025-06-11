# mypy: ignore-errors
"""
==============
Data Utilities
==============

This module contains utility functions for handling data files and directories.

"""

import hashlib
import os
import shutil
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import requests
import yaml
from loguru import logger
from tqdm import tqdm


def modify_umask(func: Callable) -> Callable:
    """Decorates a function to modify a process's umask temporarily before calling the function.

    This decorator sets the umask to 0o002, which grants write permission to the
    group while preserving the umask settings for the owner and others. It ensures
    that any file or directory created by the decorated function has group write
    permissions. After the function executes, the decorator restores the original
    umask.

    Parameters
    ----------
    func
        The function to be decorated. It can be any callable that might create files
        or directories during its execution.

    Returns
    -------
        A wrapper function that, when called, modifies the umask, calls the original
        function with the provided arguments, and finally restores the umask to its
        original value.
    """

    def wrapper(*args, **kwargs):
        old_umask = os.umask(0o002)
        try:
            return func(*args, **kwargs)
        finally:
            os.umask(old_umask)

    return wrapper


@modify_umask
def create_results_directory(results_dir: Path) -> None:
    """Creates a results directory.

    This creates the high-level results directory to be used for storing results
    (including any missing sub-directories).

    Parameters
    ----------
    results_dir
        The directory to be created.
    """
    results_dir.mkdir(parents=True, exist_ok=True)


@modify_umask
def create_results_intermediates(results_dir: Path) -> None:
    """Creates required sub-directories within a given run's results directory.

    Parameters
    ----------
    results_dir
        The results directory for the current run.
    """
    (results_dir / "intermediate").mkdir(exist_ok=True)
    (results_dir / "diagnostics").mkdir(exist_ok=True)


def copy_configuration_files_to_results_directory(
    pipeline_specification: Path,
    input_data: Path,
    computing_environment: Path | None,
    results_dir: Path,
) -> None:
    """Copies all configuration files into the results directory.

    Parameters
    ----------
    pipeline_specification
        The filepath to the pipeline specification file.
    input_data
        The filepath to the input data specification file (_not_ the paths to the
        input data themselves).
    computing_environment
        The filepath to the specification file defining the computing environment
        to run the pipeline on.
    results_dir
       The directory to write results and incidental files (logs, etc.) to.
    """
    shutil.copy(pipeline_specification, results_dir)
    shutil.copy(input_data, results_dir)
    if computing_environment:
        shutil.copy(computing_environment, results_dir)


def get_results_directory(output_dir: str | None, no_timestamp: bool) -> Path:
    """Determines the results directory path.

    This function determines the filepath for storing results by (optionally) appending
    a timestamp to the specified output directory. If no output directory is provided,
    it defaults to a directory named 'results' in the current working directory.

    Parameters
    ----------
    output_dir
        The directory to write results and incidental files (logs, etc.) to. If no
        value is provided, results will be written to a 'results/' directory in the
        current working directory.
    no_timestamp
        Whether or not to save the results in a timestamped sub-directory.

    Returns
    -------
        The fully resolved path to the results directory.
    """
    results_dir = Path("results" if output_dir is None else output_dir).resolve()
    if not no_timestamp:
        results_dir = results_dir / _get_timestamp()
    return results_dir


def _get_timestamp() -> str:
    return datetime.now().strftime("%Y_%m_%d_%H_%M_%S")


def load_yaml(filepath: str | Path) -> dict:
    """Loads and returns the contents of a YAML file.

    This function uses `yaml.safe_load` to parse the YAML file, which is designed
    to safely load a subset of YAML without executing arbitrary code.

    Parameters
    ----------
    filepath
        The path to the YAML file to be loaded.

    Returns
    -------
        The contents of the YAML file.
    """
    with open(filepath, "r") as file:
        data = yaml.safe_load(file)
    return data


@modify_umask
def download_image(
    images_dir: str | Path, record_id: int, filename: str, md5_checksum: str
) -> None:
    """Downloads an image from zenodo.

    Parameters
    ----------
    images_dir
        The directory to download the image to.
    record_id
        The zenodo record ID that the image is a part of.
    filename
        The name of the image file to download.
    md5_checksum
        The expected MD5 checksum of the image file.

    Raises
    ------
    FileNotFoundError
        If the image file was not downloaded.
    ValueError
        If the MD5 checksum of the downloaded file does not match the expected checksum.
    """

    images_dir = Path(images_dir).resolve()
    if not images_dir.exists():
        images_dir.mkdir(parents=True, exist_ok=True)

    url = f"https://zenodo.org/record/{record_id}/files/{filename}?download=1"

    response = requests.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("Content-Length", 0))
    output_path = images_dir / filename
    logger.info(f"Downloading {filename} to {output_path}...")
    with open(output_path, "wb") as file, tqdm(
        total=total_size, unit="B", unit_scale=True, desc=filename
    ) as progress_bar:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
                progress_bar.update(len(chunk))

    if not output_path.exists():
        raise FileNotFoundError(f"Failed to download the image: {filename}")

    # Verify MD5 checksum
    calculated_md5_checksum = calculate_md5_checksum(output_path)
    if calculated_md5_checksum != md5_checksum:
        raise ValueError(
            f"MD5 checksum does not match for {filename}.\n"
            f"Try manually downloading the image and then moving it to the {images_dir} directory.\n"
            f"Download the image by visiting this link: {url}"
        )


def calculate_md5_checksum(output_path: Path) -> str:
    md5_hash = hashlib.md5()
    with open(output_path, "rb") as file:
        while chunk := file.read(8192):
            md5_hash.update(chunk)

    calculated_md5_checksum = md5_hash.hexdigest()
    return calculated_md5_checksum
