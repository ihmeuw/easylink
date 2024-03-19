import os
from pathlib import Path
from tempfile import TemporaryDirectory

from linker.configuration import Config
from linker.pipeline import Pipeline

PIPELINE_STRINGS = {
    "pipeline_local": "rule_strings/pipeline_local.txt",
    "pipeline_slurm": "rule_strings/pipeline_slurm.txt",
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


def test_get_input_files(default_config, mocker, test_dir):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    assert pipeline.get_input_files(pipeline.implementations[0], Path("/tmp")) == [
        test_dir + "/input_data1/file1.csv",
        test_dir + "/input_data2/file2.csv",
    ]
    assert pipeline.get_input_files(pipeline.implementations[1], Path("/tmp")) == [
        "/tmp/intermediate/1_step_1/result.parquet"
    ]


def test_get_output_dir(default_config, mocker):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    assert pipeline.get_output_dir(pipeline.implementations[0], Path("/tmp")) == Path(
        "/tmp/intermediate/1_step_1"
    )
    assert pipeline.get_output_dir(pipeline.implementations[1], Path("/tmp")) == Path("/tmp")


def test_get_diagnostic_dir(default_config, mocker):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    assert pipeline.get_diagnostics_dir(pipeline.implementations[0], Path("/tmp")) == Path(
        "/tmp/diagnostics/1_step_1"
    )
    assert pipeline.get_diagnostics_dir(pipeline.implementations[1], Path("/tmp")) == Path(
        "/tmp/diagnostics/2_step_2"
    )


def test_build_snakefile_local(default_config, mocker, test_dir):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    with TemporaryDirectory() as snake_dir:
        snakefile = pipeline.build_snakefile(Path(snake_dir))
        expected_file_path = (
            Path(os.path.dirname(__file__)) / PIPELINE_STRINGS["pipeline_local"]
        )
        with open(expected_file_path) as expected_file:
            expected = expected_file.read()
        expected = expected.replace("{snake_dir}", snake_dir)
        expected = expected.replace("{test_dir}", test_dir)
        snake_str = snakefile.read_text()
        snake_str_lines = snake_str.split("\n")
        expected_lines = expected.split("\n")
    assert len(snake_str_lines) == len(expected_lines)
    for i, expected_line in enumerate(expected_lines):
        assert snake_str_lines[i].strip() == expected_line.strip()


def test_build_snakefile_slurm(default_config_params, mocker, test_dir):
    slurm_config_params = default_config_params
    slurm_config_params.update(
        {"computing_environment": Path(f"{test_dir}/spark_environment.yaml")}
    )
    slurm_config = Config(**slurm_config_params)
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(slurm_config)
    with TemporaryDirectory() as snake_dir:
        snakefile = pipeline.build_snakefile(Path(snake_dir))
        expected_file_path = (
            Path(os.path.dirname(__file__)) / PIPELINE_STRINGS["pipeline_slurm"]
        )
        with open(expected_file_path) as expected_file:
            expected = expected_file.read()
        expected = expected.replace("{snake_dir}", snake_dir)
        expected = expected.replace("{test_dir}", test_dir)
        snake_str = snakefile.read_text()
        snake_str_lines = snake_str.split("\n")
        expected_lines = expected.split("\n")
    assert len(snake_str_lines) == len(expected_lines)
    for i, expected_line in enumerate(expected_lines):
        assert snake_str_lines[i].strip() == expected_line.strip()
