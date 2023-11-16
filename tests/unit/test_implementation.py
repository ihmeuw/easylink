from pathlib import Path

import pytest

from linker.implementation import Implementation


def test_no_metadata_file():
    with pytest.raises(
        FileNotFoundError, match="Could not find metadata file for step 'pvs_like_case_study'"
    ):
        Implementation(
            step_name="pvs_like_case_study",
            implementation_name="no_metadata_file",
        )


def test_no_container(test_dir, mocker):
    mocker.patch(
        "linker.implementation.Implementation._get_implementation_directory",
        return_value=Path(test_dir) / "steps/foo/implementations/bar",
    )
    with pytest.raises(
        RuntimeError, match="Container 'some/path/to/container/directory/bar' does not exist."
    ):
        Implementation(step_name="foo", implementation_name="bad_implementation_name")


def test_implemenation_does_not_match_step(test_dir, mocker):
    mocker.patch(
        "linker.implementation.Implementation._get_implementation_directory",
        return_value=Path(test_dir) / "steps/foo/implementations/bar",
    )
    mocker.patch(
        "linker.implementation.Implementation._validate_container_exists", return_value=None
    )
    with pytest.raises(
        RuntimeError,
        match="Implementaton's metadata step 'foo' does not match pipeline configuration's step 'not-foo'",
    ):
        Implementation(step_name="not-foo", implementation_name="bad_implementation_name")


def test_implementation_directory(mocker):
    """Tests the hard-coding of the expected implementation directory"""
    # mock out validation so it runs on build
    mocker.patch("linker.implementation.Implementation._validate")
    implementation = Implementation(
        step_name="pvs_like_case_study", implementation_name="pvs_like_python"
    )
    assert "steps/pvs_like_case_study/implementations/pvs_like_python" in str(
        implementation._directory
    )
