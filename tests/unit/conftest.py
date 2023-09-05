import pytest
import yaml

ENV_CONFIG_DICT = {
    "computing_environment": "foo",
    "baz": {
        "qux",
        "quux",
    },
}

PIPELINE_CONFIG_DICT = {"implementation": "some_implementation"}


@pytest.fixture(scope="session")
def env_config_path(tmpdir_factory):
    tmp_path = str(tmpdir_factory.getbasetemp())
    filepath = f"{tmp_path}/environment.yaml"
    with open(filepath, "w") as file:
        yaml.dump(ENV_CONFIG_DICT, file)
    return filepath


@pytest.fixture(scope="session")
def pipeline_config_path(tmpdir_factory):
    tmp_path = str(tmpdir_factory.getbasetemp())
    filepath = f"{tmp_path}/pipeline.yaml"
    with open(filepath, "w") as file:
        yaml.dump(PIPELINE_CONFIG_DICT, file)
    return filepath
