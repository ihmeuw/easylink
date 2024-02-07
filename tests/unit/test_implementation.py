from pathlib import Path

import pytest

from linker.implementation import Implementation
from linker.runner import run_container
from linker.step import Step


def test_implementation_is_missing_from_metadata():
    with pytest.raises(
        RuntimeError,
        match="Implementation 'some-other-implementation' is not defined in implementation_metadata.yaml",
    ):
        Implementation(
            step=Step("some-step"),
            implementation_name="some-other-implementation",
            implementation_config=None,
            container_engine="undefined",
            resources={"foo": "bar"},
        )


def test_fails_when_missing_results(mocker):
    implementation = Implementation(
        step=Step("step_1"),
        implementation_name="step_1_python_pandas",
        implementation_config=None,
        container_engine="singularity",
        resources={"foo": "bar"},
    )
    mocker.patch("linker.runner.run_container", return_value=None)
    mocker.patch("linker.runner.run_with_singularity", return_value=None)
    with pytest.raises(RuntimeError, match="No results found"):
        implementation.run(
            session=None,
            runner=run_container,
            step_id="step_1",
            input_data=[],
            results_dir=Path("some-path"),
            diagnostics_dir=Path("some-path"),
        )
