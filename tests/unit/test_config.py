from pathlib import Path
from typing import Any, Dict

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
from easylink.utilities.data_utils import load_yaml


@pytest.mark.parametrize("requires_spark", [True, False])
def test__spark_is_required(test_dir, requires_spark):
    pipeline = load_yaml(f"{test_dir}/pipeline.yaml")
    if requires_spark:
        # Change step 1's implementation to python_pyspark
        pipeline["step_1"]["implementation"]["name"] = "step_1_python_pyspark_distributed"
    is_required = Config._spark_is_required(pipeline)
    assert is_required == requires_spark


def test__get_schema(default_config: Config) -> None:
    """Test default config gets "development schema", without errors"""
    assert default_config.schema == PIPELINE_SCHEMAS[1]


def test_load_params_from_specification(
    test_dir: str, default_config_params: Dict[str, Dict[str, Any]]
) -> None:
    assert default_config_params == {
        "pipeline": {
            "step_1": {"implementation": {"name": "step_1_python_pandas"}},
            "step_2": {"implementation": {"name": "step_2_python_pandas"}},
            "step_3": {"implementation": {"name": "step_3_python_pandas"}},
            "step_4": {"implementation": {"name": "step_4_python_pandas"}},
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
        match="Computing environment is expected to be a path to an existing yaml file. .*",
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
    "key, input",
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
def test_required_attributes(mocker, default_config_params, key, input):
    mocker.patch("easylink.configuration.Config._validate_environment")
    config_params = default_config_params
    if input:
        config_params["environment"][key] = input
    env_dict = {key: input} if input else {}
    retrieved = Config(config_params).environment[key]
    expected = DEFAULT_ENVIRONMENT["environment"].copy()
    expected.update(env_dict)
    assert retrieved == expected[key]


@pytest.mark.parametrize(
    "input",
    [
        # missing
        None,
        # partially defined
        {"memory": 100},
        # fully defined
        {"memory": 100, "cpus": 200, "time_limit": 300},
    ],
)
def test_implementation_resource_requests(default_config_params, input):
    key = "implementation_resources"
    config_params = default_config_params
    if input:
        config_params["environment"][key] = input
    config = Config(config_params)
    env_dict = {key: input.copy()} if input else {}
    retrieved = config.environment[key].to_dict()
    expected = DEFAULT_ENVIRONMENT["environment"][key].copy()
    if input:
        expected.update(env_dict[key])
    assert retrieved == expected


@pytest.mark.parametrize(
    "input",
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
def test_spark_requests(default_config_params, input, requires_spark):
    key = "spark"
    config_params = default_config_params
    if requires_spark:
        # Change step 1's implementation to python_pyspark
        config_params["pipeline"]["step_1"]["implementation"][
            "name"
        ] = "step_1_python_pyspark_distributed"

    if input:
        config_params["environment"][key] = input
    retrieved = Config(config_params).environment[key].to_dict()
    expected_env_dict = {key: input.copy()} if input else {}
    if requires_spark:
        # It's tricky to get the exact right behavior here without appealing to configtree
        # "workers" is a nested dictionary, so the normal dict update method doesn't work
        # for a partially specified environment here
        expected = LayeredConfigTree(SPARK_DEFAULTS, layers=["initial_data", "user"])
        if input:
            expected.update(expected_env_dict[key], layer="user")
        expected = expected.to_dict()
    else:
        expected = {}
    assert retrieved == expected


@pytest.mark.parametrize("includes_implementation_configuration", [False, True])
def test_get_implementation_specific_configuration(
    default_config_params, includes_implementation_configuration
):
    config_params = default_config_params
    step_1_config = {}
    step_2_config = {}
    if includes_implementation_configuration:
        step_2_config = {
            "SOME-CONFIGURATION": "some-value",
            "SOME-OTHER-CONFIGURATION": "some-other-value",
        }
        config_params["pipeline"]["step_2"]["implementation"]["configuration"] = step_2_config
    config = Config(config_params)
    assert config.pipeline.step_1.implementation.configuration.to_dict() == step_1_config
    assert config.pipeline.step_2.implementation.configuration.to_dict() == step_2_config
