# mypy: ignore-errors
import os
from pathlib import Path

import pytest

from easylink.configuration import Config, load_params_from_specification
from easylink.pipeline import Pipeline
from easylink.utilities.data_utils import (
    copy_configuration_files_to_results_directory,
    create_results_directory,
    create_results_intermediates,
)

PIPELINE_STRINGS = {
    "local": "rule_strings/pipeline_local.txt",
    "slurm": "rule_strings/pipeline_slurm.txt",
}


@pytest.mark.parametrize("computing_environment", ["local", "slurm"])
def test_build_snakefile(
    default_config_paths, mocker, test_dir, results_dir, computing_environment
):
    config_paths = default_config_paths
    if computing_environment == "slurm":
        config_paths["computing_environment"] = f"{test_dir}/spark_environment.yaml"

    config_params = load_params_from_specification(**config_paths)
    config = Config(config_params)
    mocker.patch("easylink.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(config)
    create_results_directory(results_dir)
    create_results_intermediates(results_dir)
    copy_configuration_files_to_results_directory(**config_paths)
    snakefile = pipeline.build_snakefile()
    expected_file_path = (
        Path(os.path.dirname(__file__)) / PIPELINE_STRINGS[computing_environment]
    )
    with open(expected_file_path) as expected_file:
        expected = expected_file.read()
    expected = expected.replace("{test_dir}", test_dir)
    snake_str = snakefile.read_text()
    snake_str_lines = snake_str.split("\n")
    expected_lines = expected.split("\n")
    assert len(snake_str_lines) == len(expected_lines)
    for i, expected_line in enumerate(expected_lines):
        assert snake_str_lines[i].strip() == expected_line.strip()
