import errno
import re
from pathlib import Path

import pytest

from linker.configuration import Config
from linker.implementation import Implementation
from linker.pipeline import Pipeline
from linker.step import Step


def test__get_steps(config, mocker):
    mocker.patch("linker.pipeline.Pipeline._validate")
    mocker.patch("linker.implementation.Implementation._load_metadata")
    mocker.patch("linker.implementation.Implementation._get_container_full_stem")
    mocker.patch("linker.implementation.Implementation.validate")
    config.pipeline = {
        "steps": {
            "step1": {
                "implementation": "implementation1",
            },
            "step2": {
                "implementation": "implementation2",
            },
        },
    }
    pipeline = Pipeline(config)
    assert pipeline.steps == (Step("step1"), Step("step2"))


def test_unsupported_step(test_dir, caplog, mocker):
    mocker.patch("linker.implementation.Implementation._load_metadata")
    mocker.patch("linker.implementation.Implementation._get_container_full_stem")
    mocker.patch("linker.implementation.Implementation.validate", return_value=[])
    config = Config(
        f"{test_dir}/bad_step_pipeline.yaml",  # pipeline with unsupported step
        f"{test_dir}/input_data.yaml",
        f"{test_dir}/environment.yaml",
    )
    check_expected_validation_exit(
        config=config,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_record_length=1,
        expected_msg=(
            [
                "PIPELINE ERRORS: pvs_like_case_study: - 'Step 1: the pipeline "
                "schema expects step ''pvs_like_case_study'' but the provided "
                "pipeline specifies ''foo''. Check step order and spelling in the "
                "pipeline configuration yaml.'"
            ]
        ),
    )


def test_no_container(test_dir, caplog, mocker):
    mocker.patch(
        "linker.implementation.Implementation._get_container_full_stem",
        return_value=Path("some/path/with/no/container"),
    )
    mocker.PropertyMock(
        "linker.implementation.Implementation._container_engine", return_value="unknown"
    )
    config = Config(
        f"{test_dir}/pipeline.yaml",  # pipeline with unsupported step
        f"{test_dir}/input_data.yaml",
        f"{test_dir}/environment.yaml",
    )
    check_expected_validation_exit(
        config=config,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_record_length=1,
        expected_msg=(
            [
                "IMPLEMENTATION ERRORS: pvs_like_python: - Container "
                "'some/path/with/no/container' does not exist."
            ]
        ),
    )


def test_implemenation_does_not_match_step(test_dir, caplog, mocker):
    mocker.patch(
        "linker.implementation.Implementation._load_metadata",
        return_value={
            "pvs_like_python": {
                "step": "step-1",
                "path": "/some/path",
                "name": "some-name",
            },
        },
    )
    mocker.patch(
        "linker.implementation.Implementation._validate_container_exists", return_value=None
    )
    config = Config(
        f"{test_dir}/pipeline.yaml",  # pipeline with unsupported step
        f"{test_dir}/input_data.yaml",
        f"{test_dir}/environment.yaml",
    )
    check_expected_validation_exit(
        config=config,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_record_length=1,
        expected_msg=(
            [
                "IMPLEMENTATION ERRORS: pvs_like_python: - Container "
                "'some/path/with/no/container' does not exist."
            ]
        ),
    )


@pytest.mark.skip(reason="TODO when multiple steps are implemented")
def test_bad_step_order():
    pass


@pytest.mark.skip(reason="TODO when multiple steps are implemented")
def test_missing_a_step():
    pass


@pytest.mark.skip(reason="TODO [MIC-4735]")
def test_batch_validation():
    pass


####################
# HELPER FUNCTIONS #
####################


def check_expected_validation_exit(
    config, caplog, error_no, expected_record_length, expected_msg
):
    try:
        Pipeline(config)
    except SystemExit as e:
        assert e.code == error_no
        # We should only have one error message
        assert len(caplog.record_tuples) == expected_record_length
        # Extract error message
        msg = caplog.text.split("Validation errors found. Please see below.")[1].split(
            "Validation errors found. Please see above."
        )[0]
        # Should only have one list item ("-" in yaml)
        assert msg.count("-") == len(expected_msg)
        # Should have the correct error message
        msg = re.sub("\n+", " ", msg)
        msg = re.sub(" +", " ", msg).strip()
        assert msg in expected_msg
