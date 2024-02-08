from pathlib import Path

import pytest

from linker.implementation import Implementation
from linker.runner import run_container
from linker.step import Step


def test_implementation_is_missing_from_metadata(default_config, mocker):
    mocker.patch(
        "linker.configuration.Config.get_implementation_name",
        return_value="some-step-implementation",
    )
    mocker.patch(
        "linker.configuration.Config.get_implementation_specific_configuration",
        return_value=None,
    )
    with pytest.raises(
        RuntimeError,
        match="Implementation 'some-step-implementation' is not defined in implementation_metadata.yaml",
    ):
        Implementation(
            config=default_config,
            step=Step("some-step"),
        )


def test_fails_when_missing_results(default_config, mocker):
    implementation = Implementation(
        config=default_config,
        step=Step("step_1"),
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
