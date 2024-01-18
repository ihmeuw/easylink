import pytest

from linker.implementation import Implementation
from linker.step import Step


def test_implementation_is_missing_from_metadata():
    with pytest.raises(
        RuntimeError,
        match="Implementation 'some-other-implementation' is not defined in implementation_metadata.yaml",
    ):
        Implementation(
            step=Step("some-step"),
            implementation_name="some-other-implementation",
            container_engine="undefined",
            implementation_config=None,
        )
