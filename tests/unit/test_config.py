from pathlib import Path

import pytest

from linker.configuration import DEFAULT_ENVIRONMENT, Config
from linker.step import Step
from linker.utilities.data_utils import load_yaml


@pytest.mark.parametrize("requires_spark", [True, False])
def test__determine_if_spark_is_required(test_dir, requires_spark):
    pipeline = load_yaml(f"{test_dir}/pipeline.yaml")
    if requires_spark:
        # Change step 1's implementation to python_pyspark
        pipeline["steps"]["step_1"]["implementation"][
            "name"
        ] = "step_1_python_pyspark_distributed"
    is_required = Config._determine_if_spark_is_required(pipeline)
    assert is_required == requires_spark


def test__get_schema(default_config):
    """Test that the schema is correctly loaded from the pipeline.yaml"""
    assert default_config.schema.steps == [Step("step_1"), Step("step_2")]


def test__load_input_data_paths(test_dir):
    paths = Config._load_input_data_paths(f"{test_dir}/input_data.yaml")
    assert paths == [Path(f"{test_dir}/input_data{n}/file{n}.csv") for n in [1, 2]]


@pytest.mark.parametrize(
    "environment_file, expected",
    [
        # good
        (
            "environment.yaml",
            {
                k: v
                for k, v in DEFAULT_ENVIRONMENT.items()
                if k in ["computing_environment", "container_engine"]
            },
        ),
        (None, {}),
    ],
)
def test__load_computing_environment(test_dir, environment_file, expected):
    filepath = Path(f"{test_dir}/{environment_file}") if environment_file else None
    env = Config._load_computing_environment(filepath)
    assert env == expected


def test__load_missing_computing_environment_fails():
    with pytest.raises(
        FileNotFoundError,
        match="Computing environment is expected to be a path to an existing yaml file. .*",
    ):
        Config._load_computing_environment(Path("some/bogus/path.yaml"))


def test_input_data_configuration_requires_key_value_pairs(test_dir):
    config_params = {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
        "input_data": f"{test_dir}/input_data_list.yaml",
        "computing_environment": None,
    }
    with pytest.raises(
        TypeError, match="Input data should be submitted like 'key': path/to/file."
    ):
        Config(**config_params)


@pytest.mark.parametrize(
    "computing_environment",
    [
        "bad/path/to/environment.yaml",
        Path("another/bad/path"),
    ],
)
def test_environment_configuration_not_found(default_config_params, computing_environment):
    config_params = default_config_params
    config_params.update(
        {"input_data": "foo", "computing_environment": computing_environment}
    )
    with pytest.raises(FileNotFoundError):
        Config(**config_params)


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
def test__get_required_attribute(key, input):
    env_dict = {key: input} if input else {}
    retrieved = Config._get_required_attribute(env_dict, key)
    expected = DEFAULT_ENVIRONMENT.copy()
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
def test__get_implementation_resource_requests(input):
    key = "implementation_resources"
    env_dict = {key: input.copy()} if input else {}
    retrieved = Config._get_implementation_resource_requests(env_dict)
    if input:
        expected = DEFAULT_ENVIRONMENT[key].copy()
        expected.update(env_dict[key])
    else:
        expected = {}
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
def test__get_spark_requests(input):
    key = "spark"
    env_dict = {key: input.copy()} if input else {}
    for requires_spark in [False, True]:
        retrieved = Config._get_spark_requests(env_dict, requires_spark)
        if requires_spark:
            expected = DEFAULT_ENVIRONMENT[key].copy()
            if input:
                expected.update(env_dict[key])
        else:
            expected = {}
        assert retrieved == expected


@pytest.mark.parametrize("includes_implementation_configuration", [False, True])
def test_get_implementation_specific_configuration(
    default_config, includes_implementation_configuration
):
    config = default_config
    step_1_config = {}
    step_2_config = {}
    if includes_implementation_configuration:
        step_2_config = {
            "SOME-CONFIGURATION": "some-value",
            "SOME-OTHER-CONFIGURATION": "some-other-value",
        }
        config.pipeline["steps"]["step_2"]["implementation"]["configuration"] = step_2_config
    assert config.get_implementation_specific_configuration("step_1") == step_1_config
    assert config.get_implementation_specific_configuration("step_2") == step_2_config
