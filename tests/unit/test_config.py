# mypy: ignore-errors
from pathlib import Path
from typing import Any

import pytest
from layered_config_tree import LayeredConfigTree

from easylink.configuration import (
    DEFAULT_ENVIRONMENT,
    SPARK_DEFAULTS,
    Config,
    _load_computing_environment,
    _load_input_data_paths,
)
from easylink.pipeline_schema import PIPELINE_SCHEMAS


def test__get_schema(default_config: Config) -> None:
    """Test default config gets "development schema", without errors"""
    assert default_config.schema == PIPELINE_SCHEMAS[0]


def test_load_params_from_specification(
    test_dir: str, default_config_params: dict[str, dict[str, Any]]
) -> None:
    assert default_config_params == {
        "pipeline": {
            "steps": {
                "step_1": {"implementation": {"name": "step_1_python_pandas"}},
                "step_2": {"implementation": {"name": "step_2_python_pandas"}},
                "step_3": {"implementation": {"name": "step_3_python_pandas"}},
                "choice_section": {
                    "type": "simple",
                    "step_4": {"implementation": {"name": "step_4_python_pandas"}},
                },
            }
        },
        "input_data": {
            "file1": Path(f"{test_dir}/input_data1/file1.csv"),
            "file2": Path(f"{test_dir}/input_data2/file2.csv"),
        },
        "environment": {
            "computing_environment": "local",
            "container_engine": "undefined",
        },
        "results_dir": Path(f"{test_dir}/results_dir"),
    }


def test__load_input_data_paths(test_dir: str) -> None:
    paths = _load_input_data_paths(f"{test_dir}/input_data.yaml")
    assert paths == {
        "file1": Path(f"{test_dir}/input_data1/file1.csv"),
        "file2": Path(f"{test_dir}/input_data2/file2.csv"),
    }


@pytest.mark.parametrize(
    "environment_file, expected",
    [
        # good
        (
            "environment.yaml",
            {
                k: v
                for k, v in DEFAULT_ENVIRONMENT["environment"].items()
                if k in ["computing_environment", "container_engine"]
            },
        ),
        (None, {}),
    ],
)
def test__load_computing_environment(test_dir, environment_file, expected):
    filepath = Path(f"{test_dir}/{environment_file}") if environment_file else None
    env = _load_computing_environment(filepath)
    assert env == expected


def test_load_missing_computing_environment_fails():
    with pytest.raises(
        FileNotFoundError,
        match="Computing environment is expected to be a path to an existing specification file. .*",
    ):
        _load_computing_environment(Path("some/bogus/path.yaml"))


def test_input_data_configuration_requires_key_value_pairs(test_dir):
    with pytest.raises(
        TypeError, match="Input data should be submitted like 'key': path/to/file."
    ):
        _load_input_data_paths(f"{test_dir}/input_data_list.yaml")


@pytest.mark.parametrize(
    "computing_environment",
    [
        "bad/path/to/environment.yaml",
        Path("another/bad/path"),
    ],
)
def test_environment_configuration_not_found(computing_environment):
    with pytest.raises(FileNotFoundError):
        _load_computing_environment(computing_environment)


@pytest.mark.parametrize(
    "key, value",
    [
        ("computing_environment", None),
        ("computing_environment", "local"),
        ("computing_environment", "slurm"),
        ("container_engine", None),
        ("container_engine", "docker"),
        ("container_engine", "singularity"),
        ("container_engine", "undefined"),
    ],
)
def test_required_attributes(mocker, default_config_params, key, value):
    mocker.patch("easylink.configuration.Config._validate_environment")
    config_params = default_config_params
    if value:
        config_params["environment"][key] = value
    env_dict = {key: value} if value else {}
    retrieved = Config(config_params).environment[key]
    expected = DEFAULT_ENVIRONMENT["environment"].copy()
    expected.update(env_dict)
    assert retrieved == expected[key]


@pytest.mark.parametrize(
    "resource_request",
    [
        # missing
        None,
        # partially defined
        {"memory": 100},
        # fully defined
        {"memory": 100, "cpus": 200, "time_limit": 300},
    ],
)
def test_implementation_resource_requests(default_config_params, resource_request):
    key = "implementation_resources"
    config_params = default_config_params
    if resource_request:
        config_params["environment"][key] = resource_request
    config = Config(config_params)
    env_dict = {key: resource_request.copy()} if resource_request else {}
    retrieved = config.environment[key].to_dict()
    expected = DEFAULT_ENVIRONMENT["environment"][key].copy()
    if resource_request:
        expected.update(env_dict[key])
    assert retrieved == expected


@pytest.mark.parametrize(
    "spark_request",
    [
        # missing
        None,
        # partially defined (missing entire workers)
        {"keep_alive": "idk"},
        # partially defined (missing keep_alive and num_workers)
        {
            "workers": {
                "cpus_per_node": 200,
                "mem_per_node": 300,
                "time_limit": 400,
            }
        },
        # fully defined
        {
            "workers": {
                "num_workers": 100,
                "cpus_per_node": 200,
                "mem_per_node": 300,
                "time_limit": 400,
            },
            "keep_alive": "idk",
        },
    ],
)
@pytest.mark.parametrize("requires_spark", [True, False])
def test_spark_requests(default_config_params, spark_request, requires_spark):
    key = "spark"
    config_params = default_config_params
    if requires_spark:
        # Change step 1's implementation to python_pyspark
        config_params["pipeline"]["steps"]["step_1"]["implementation"][
            "name"
        ] = "step_1_python_pyspark"

    if spark_request:
        config_params["environment"][key] = spark_request
    retrieved = Config(config_params).environment[key].to_dict()
    expected_env_dict = {key: spark_request.copy()} if spark_request else {}
    expected = LayeredConfigTree(SPARK_DEFAULTS, layers=["initial_data", "user"])
    if spark_request:
        expected.update(expected_env_dict[key], layer="user")
    expected = expected.to_dict()
    assert retrieved == expected


@pytest.mark.parametrize("is_default", [True, False])
def test_combined_implementations(default_config_params, is_default):
    combined_dict = {"foo": "bar"}
    config_params = default_config_params
    if not is_default:
        config_params["pipeline"]["combined_implementations"] = combined_dict
    combined_implementations = Config(config_params).pipeline.combined_implementations
    expected = {} if is_default else combined_dict
    assert combined_implementations.to_dict() == expected
