import os
from pathlib import Path

import pytest

from linker.utilities.data_utils import (
    copy_configuration_files_to_results_directory,
    get_results_directory,
)


def test_copy_configuration_files_to_results_directory(default_config_params, test_dir):
    output_dir = Path(test_dir + "/some/output/dir")
    config_params = default_config_params
    config_params["results_dir"] = output_dir
    copy_configuration_files_to_results_directory(
        config_params["pipeline_specification"],
        config_params["input_data"],
        config_params["computing_environment"],
        output_dir,
    )
    assert output_dir.exists()
    assert (output_dir / "intermediate").exists()
    assert (output_dir / "diagnostics").exists()


@pytest.mark.parametrize(
    "output_dir_provided, timestamp",
    [
        (False, False),
        (False, True),
        (True, False),
        (True, True),
    ],
)
def test_get_results_directory(test_dir, output_dir_provided, timestamp, mocker):
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
    results_dir = get_results_directory(output_dir, timestamp)

    expected_results_dir = Path(test_dir)
    if not output_dir_provided:
        expected_results_dir = expected_results_dir / "results"
    if timestamp:
        expected_results_dir = expected_results_dir / "2024_01_01_00_00_00"

    assert expected_results_dir == results_dir
