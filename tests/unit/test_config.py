from pathlib import Path

import pytest

from linker.configuration import Config
from tests.unit.conftest import ENV_CONFIG_DICT, PIPELINE_CONFIG_DICT

# TODO [MIC-4491]: beef these up


@pytest.mark.parametrize(
    "computing_environment",
    [
        "foo",
        "bad/path/to/environment.yaml",
        Path("another/bad/path"),
    ],
)
def test_bad_computing_environment_fails(config_path, computing_environment):
    with pytest.raises(FileNotFoundError):
        Config(f"{config_path}/pipeline.yaml", computing_environment, "foo")


def test_local_computing_environment(config_path):
    config = Config(f"{config_path}/pipeline.yaml", None, f"{config_path}/input_data.yaml")
    assert config.computing_environment == "local"


def test_get_specs(config_path):
    config = Config(
        f"{config_path}/pipeline.yaml",
        f"{config_path}/environment.yaml",
        f"{config_path}/input_data.yaml",
    )
    assert config.pipeline == PIPELINE_CONFIG_DICT
    assert config.environment == ENV_CONFIG_DICT
    assert config.input_data == [
        Path(x) for x in [f"{config_path}/input_data{n}/file{n}" for n in [1, 2]]
    ]
