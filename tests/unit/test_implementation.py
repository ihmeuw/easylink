from pathlib import Path

import pytest

from linker.implementation import Implementation
from linker.step import Step


def test_implementation_instantiation(test_dir):
    implementation_dir = (
        Path(test_dir) / "steps/pvs_like_case_study/implementations/pvs_like_python"
    )
    implementation = Implementation(
        step=Step("foo"),
        directory=implementation_dir,
    )
    assert implementation.name == "pvs_like_python"
    assert implementation.step == Step("foo")
    assert implementation.directory == implementation_dir
    assert (
        implementation.container_full_stem
        == "some/path/to/container/directory/pvs_like_python"
    )


def test_no_metadata_file():
    with pytest.raises(FileNotFoundError):
        Implementation(
            step=Step("foo"),
            directory=Path("/some/directory/"),
        )


@pytest.mark.skip(reason="TODO")
def test_implementation_run():
    pass


@pytest.mark.skip(reason="TODO")
def test__get_implementation_directory():
    pass
