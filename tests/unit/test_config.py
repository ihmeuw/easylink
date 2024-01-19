from pathlib import Path

import pytest

from linker.configuration import Config
from tests.unit.conftest import ENV_CONFIG_DICT, PIPELINE_CONFIG_DICT


def test_config_instantiation(test_dir, config):
    assert config.pipeline == PIPELINE_CONFIG_DICT["good"]
    assert config.environment == ENV_CONFIG_DICT
    assert config.input_data == [
        Path(x) for x in [f"{test_dir}/input_data{n}/file{n}.csv" for n in [1, 2]]
    ]
    assert config.computing_environment == ENV_CONFIG_DICT["computing_environment"]
    assert config.container_engine == ENV_CONFIG_DICT["container_engine"]


@pytest.mark.parametrize("input_data", ["good", "bad"])
def test__load_input_data_paths(test_dir, input_data):
    if input_data == "good":
        paths = Config._load_input_data_paths(f"{test_dir}/input_data.yaml")
        assert paths == [Path(f"{test_dir}/input_data{n}/file{n}.csv") for n in [1, 2]]
    if input_data == "bad":
        with pytest.raises(RuntimeError, match=r"Cannot find input data: .*"):
            Config._load_input_data_paths(f"{test_dir}/bad_input_data.yaml")


@pytest.mark.parametrize(
    "computing_environment",
    [
        "foo",
        "bad/path/to/environment.yaml",
        Path("another/bad/path"),
    ],
)
def test_bad_computing_environment_fails(test_dir, computing_environment):
    with pytest.raises(FileNotFoundError):
        Config(f"{test_dir}/pipeline.yaml", "foo", computing_environment)


def test_default_computing_environment(test_dir):
    """The computing environment value should default to 'local'"""
    config = Config(f"{test_dir}/pipeline.yaml", f"{test_dir}/input_data.yaml", None)
    assert config.computing_environment == "local"


def test_default_container_engine(test_dir):
    """The container engine value should default to 'undefined'"""
    config = Config(f"{test_dir}/pipeline.yaml", f"{test_dir}/input_data.yaml", None)
    assert config.container_engine == "undefined"


def test_broken_input_files(test_dir):
    with pytest.raises(
        RuntimeError, match=r"^Data file .* is missing required column\(s\) .*"
    ):
        Config(f"{test_dir}/pipeline.yaml", f"{test_dir}/bad_columns_input_data.yaml", None)
