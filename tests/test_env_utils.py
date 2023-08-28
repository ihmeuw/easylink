from pathlib import Path

import pytest

from linker.utilities.env_utils import get_compute_config


@pytest.mark.parametrize(
    "computing_environment",
    [
        "foo",
        "bad/path/to/environment.yaml",
        Path("another/bad/path"),
    ],
)
def test_get_compute_env_fails(computing_environment):
    match = (
        "Computing environment is expected to be either 'local' or a path to an "
        f"existing yaml file. Input is neither: '{computing_environment}'"
    )
    with pytest.raises(RuntimeError, match=match):
        get_compute_config(computing_environment=computing_environment)


def test_get_compute_env_local():
    assert get_compute_config("local")["computing_environment"] == "local"


def test_get_compute_env_yaml():
    assert isinstance(
        get_compute_config("src/linker/configuration/environment.yaml"), dict
    )
