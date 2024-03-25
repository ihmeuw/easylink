import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from linker.configuration import Config
from linker.pipeline import Pipeline
from linker.utilities.data_utils import copy_configuration_files_to_results_directory

PIPELINE_STRINGS = {
    "local": "rule_strings/pipeline_local.txt",
    "slurm": "rule_strings/pipeline_slurm.txt",
}


def test__get_implementations(default_config, mocker):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    implementation_names = [
        implementation.name for implementation in pipeline.implementations
    ]
    assert implementation_names == ["step_1_python_pandas", "step_2_python_pandas"]


def test_get_step_id(default_config, mocker):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    assert pipeline.get_step_id(pipeline.implementations[0]) == "1_step_1"
    assert pipeline.get_step_id(pipeline.implementations[1]) == "2_step_2"


def test_get_input_files(default_config, mocker, test_dir, results_dir):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    assert pipeline.get_input_files(pipeline.implementations[0]) == [
        test_dir + "/input_data1/file1.csv",
        test_dir + "/input_data2/file2.csv",
    ]
    assert pipeline.get_input_files(pipeline.implementations[1]) == [
        str(results_dir / "intermediate/1_step_1/result.parquet")
    ]


def test_get_output_dir(default_config, mocker, results_dir):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    assert pipeline.get_output_dir(pipeline.implementations[0]) == Path(
        results_dir / "intermediate/1_step_1"
    )
    assert pipeline.get_output_dir(pipeline.implementations[1]) == results_dir


def test_get_diagnostic_dir(default_config, mocker, results_dir):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    assert pipeline.get_diagnostics_dir(pipeline.implementations[0]) == Path(
        results_dir / "diagnostics/1_step_1"
    )
    assert pipeline.get_diagnostics_dir(pipeline.implementations[1]) == Path(
        results_dir / "diagnostics/2_step_2"
    )


@pytest.mark.parametrize("computing_environment", ["local", "slurm"])
def test_build_snakefile(default_config_params, mocker, test_dir, computing_environment):
    results_dir = test_dir + "/results_dir"
    config_params = default_config_params
    if computing_environment == "slurm":
        config_params["computing_environment"] = Path(f"{test_dir}/spark_environment.yaml")

    config = Config(**config_params)
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(config)
    copy_configuration_files_to_results_directory(**config_params)
    snakefile = pipeline.build_snakefile()
    expected_file_path = (
        Path(os.path.dirname(__file__)) / PIPELINE_STRINGS[computing_environment]
    )
    with open(expected_file_path) as expected_file:
        expected = expected_file.read()
    expected = expected.replace("{snake_dir}", results_dir)
    expected = expected.replace("{test_dir}", test_dir)
    snake_str = snakefile.read_text()
    snake_str_lines = snake_str.split("\n")
    expected_lines = expected.split("\n")
    assert len(snake_str_lines) == len(expected_lines)
    for i, expected_line in enumerate(expected_lines):
        assert snake_str_lines[i].strip() == expected_line.strip()
