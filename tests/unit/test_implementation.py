from pathlib import Path

import pytest

from linker.implementation import Implementation


def test_no_container(mocker):
    mocker.patch(
        "linker.implementation.Implementation._get_container_full_stem",
        return_value=Path("some/path/with/no/container"),
    )
    with pytest.raises(
        RuntimeError,
        match="Container 'some/path/with/no/container' does not exist.",
    ):
        Implementation(
            step_name="pvs_like_case_study",
            implementation_name="pvs_like_python",
            container_engine="undefined",
        )


def test_implemenation_does_not_match_step(mocker):
    mocker.patch(
        "linker.implementation.Implementation._load_metadata",
        return_value={
            "some-implementation": {
                "step": "step-1",
                "path": "/some/path",
                "name": "some-name",
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
        Implementation(
            step_name="step-2",
            implementation_name="some-implementation",
            container_engine="undefined",
        )


def test_implementation_is_missing_from_metadata(mocker):
    mocker.patch(
        "linker.implementation.Implementation._load_metadata",
        return_value={
            "some-implementation": {
                "step": "some-step",
                "path": "/some/path",
                "name": "some-name",
            },
        },
    )
    with pytest.raises(
        RuntimeError,
        match="Implementation 'some-other-implementation' is not defined in implementation_metadata.yaml",
    ):
        Implementation(
            step_name="some-step",
            implementation_name="some-other-implementation",
            container_engine="undefined",
        )
