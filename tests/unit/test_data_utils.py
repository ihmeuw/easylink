# mypy: ignore-errors
import os
from pathlib import Path

import pytest

from easylink.utilities.data_utils import (
    copy_configuration_files_to_results_directory,
    create_results_directory,
    create_results_intermediates,
    get_results_directory,
)


def test_create_results_directory(test_dir):
    results_dir = Path(test_dir + "/some/output/dir")
    create_results_directory(results_dir)
    assert results_dir.exists()

    create_results_intermediates(results_dir)
    assert (results_dir / "intermediate").exists()
    assert (results_dir / "diagnostics").exists()


@pytest.mark.parametrize(
    "output_dir_provided, no_timestamp",
    [
        (False, True),
        (False, False),
        (True, True),
        (True, False),
    ],
)
def test_get_results_directory(test_dir, output_dir_provided, no_timestamp, mocker):
    """Tests expected behavior. If directory is provided then a "results/" folder
    is created at the working directory. If no_timestamp is False, then a timestamped
    directory is created within the results directory.
    """
    if output_dir_provided:
        output_dir = test_dir
    else:
        output_dir = None
        # But change the working directory to the test_dir
        os.chdir(test_dir)
    mocker.patch(
        "easylink.utilities.data_utils._get_timestamp", return_value="2024_01_01_00_00_00"
    )
    results_dir = get_results_directory(output_dir, no_timestamp)

    expected_results_dir = Path(test_dir)
    if not output_dir_provided:
        expected_results_dir = expected_results_dir / "results"
    if not no_timestamp:
        expected_results_dir = expected_results_dir / "2024_01_01_00_00_00"

    assert expected_results_dir == results_dir
