import pytest
import yaml

ENV_CONFIG_DICT = {
    "foo": "bar",
    "baz": {
        "qux",
        "quux",
    },
}


@pytest.fixture(scope="session")
def env_config_path(tmpdir_factory):
    tmp_path = str(tmpdir_factory.getbasetemp())
    with open(f"{tmp_path}/environment.yaml", "w") as file:
        yaml.dump(ENV_CONFIG_DICT, file)
    return tmp_path
