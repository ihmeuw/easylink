from pathlib import Path

import pytest

from linker.implementation import Implementation


def test_no_container(mocker):
    mocker.patch(
        "linker.implementation.Implementation._get_container_location",
        return_value=Path("some/path/with/no/container"),
    )
    with pytest.raises(
        RuntimeError, match="Container 'some/path/with/no/container/pvs_like_python' does not exist."
    ):
        Implementation(step_name="pvs_like_case_study", implementation_name="pvs_like_python")


def test_implemenation_does_not_match_step(mocker):
    mocker.patch(
        "linker.implementation.Implementation._load_metadata",
        return_value={
            "some-implementation": {
                "step": "step-1",
                "path": "/some/path",
            },
        },
    )
    mocker.patch(
        "linker.implementation.Implementation._validate_container_exists", return_value=None
    )
    with pytest.raises(
        RuntimeError,
        match="Implementaton's metadata step 'step-1' does not match pipeline configuration's step 'step-2'",
    ):
        Implementation(step_name="step-2", implementation_name="some-implementation")
