import os
from pathlib import Path

import pytest

from linker.utilities.data_utils import create_results_directory


@pytest.mark.parametrize(
    "output_dir_provided, timestamp",
    [
        (False, False),
        (False, True),
        (True, False),
        (True, True),
    ],
)
def test_create_results_directory(test_dir, output_dir_provided, timestamp, mocker):
    """Tests expected behavior. If directory is provided then a "results/" folder
    is created at the working directory. If timestamp is True, then a timestamped
    directory is created within the results directory.
    """
    if output_dir_provided:
        output_dir = test_dir
    else:
        output_dir = None
        # But change the working directory to the test_dir
        os.chdir(test_dir)
    mocker.patch(
        "linker.utilities.data_utils._get_timestamp", return_value="2024_01_01_00_00_00"
    )
    results_dir = create_results_directory(output_dir, timestamp)

    expected_results_dir = Path(test_dir)
    if not output_dir_provided:
        expected_results_dir = expected_results_dir / "results"
    if timestamp:
        expected_results_dir = expected_results_dir / "2024_01_01_00_00_00"

    assert expected_results_dir == results_dir
    assert results_dir.exists()
    assert (results_dir / "intermediate").exists()
    assert (results_dir / "diagnostics").exists()
