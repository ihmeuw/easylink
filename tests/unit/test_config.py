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
def test_bad_computing_environment_fails(pipeline_config_path, computing_environment):
    match = (
        "Computing environment is expected to be either 'local' or a path to an "
        f"existing yaml file. Input is neither: '{computing_environment}'"
    )
    with pytest.raises(RuntimeError, match=match):
        Config(pipeline_config_path, computing_environment)


def test_local_computing_environment(pipeline_config_path):
    config = Config(pipeline_config_path, "local")
    assert config.computing_environment == "local"


def get_get_specs(pipeline_config_path, env_config_path):
    config = Config(pipeline_config_path, env_config_path)
    assert config.pipeline == PIPELINE_CONFIG_DICT
    assert config.environment == ENV_CONFIG_DICT
