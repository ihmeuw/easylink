import os
from pathlib import Path

import pytest

from easylink.configuration import Config, load_params_from_specification
from easylink.pipeline import Pipeline
from easylink.utilities.data_utils import copy_configuration_files_to_results_directory

PIPELINE_STRINGS = {
    "local": "rule_strings/pipeline_local.txt",
    "slurm": "rule_strings/pipeline_slurm.txt",
}


def test_implementation_nodes(default_config):
    pipeline = Pipeline(default_config)
    implementation_nodes = pipeline.implementation_nodes
    assert implementation_nodes == [
        "1_step_1",
        "2_step_2",
        "3_step_3",
        "4_step_4",
    ]
    implementation_names = [
        pipeline.pipeline_graph.nodes[node]["implementation"].name
        for node in implementation_nodes
    ]
    assert implementation_names == [
        "step_1_python_pandas",
        "step_2_python_pandas",
        "step_3_python_pandas",
        "step_4_python_pandas",
    ]


def test__get_pipeline_graph(default_config, test_dir):
    pipeline = Pipeline(default_config)
    assert set(pipeline.pipeline_graph.nodes) == {
        "input_data",
        "1_step_1",
        "2_step_2",
        "3_step_3",
        "4_step_4",
        "results",
    }
    expected_edges = [
        (
            "input_data",
            "1_step_1",
            {
                "files": [
                    f"{test_dir}/input_data1/file1.csv",
                    f"{test_dir}/input_data2/file2.csv",
                ]
            },
        ),
        ("1_step_1", "2_step_2", {"files": ["intermediate/1_step_1/result.parquet"]}),
        ("2_step_2", "3_step_3", {"files": ["intermediate/2_step_2/result.parquet"]}),
        ("3_step_3", "4_step_4", {"files": ["intermediate/3_step_3/result.parquet"]}),
        ("4_step_4", "results", {"files": ["result.parquet"]}),
    ]
    pipeline_edges = pipeline.pipeline_graph.edges(data=True)

    for edge in pipeline_edges:
        assert edge in expected_edges
    for edge in expected_edges:
        assert edge in pipeline_edges


def test_get_input_output_files(default_config, test_dir):
    pipeline = Pipeline(default_config)
    input_files, output_files = pipeline.get_input_output_files("1_step_1")
    assert input_files == [
        f"{test_dir}/input_data1/file1.csv",
        f"{test_dir}/input_data2/file2.csv",
    ]
    assert output_files == ["intermediate/1_step_1/result.parquet"]

    input_files, output_files = pipeline.get_input_output_files("2_step_2")
    assert input_files == ["intermediate/1_step_1/result.parquet"]
    assert output_files == ["intermediate/2_step_2/result.parquet"]

    input_files, output_files = pipeline.get_input_output_files("4_step_4")
    assert input_files == ["intermediate/3_step_3/result.parquet"]
    assert output_files == ["result.parquet"]


@pytest.mark.parametrize("computing_environment", ["local", "slurm"])
def test_build_snakefile(default_config_paths, mocker, test_dir, computing_environment):
    config_paths = default_config_paths
    if computing_environment == "slurm":
        config_paths["computing_environment"] = f"{test_dir}/spark_environment.yaml"

    config_params = load_params_from_specification(**config_paths)
    config = Config(config_params)
    mocker.patch("easylink.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(config)
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
