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
                "implementation": {
                    "name": "implementation1",
                },
            },
            "step2": {
                "implementation": {
                    "name": "implementation2",
                },
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
        expected_msg={
            "PIPELINE ERRORS": {
                "development": [
                    "- Expected 2 steps but found 1 implementations.",
                ],
                "pvs_like_case_study": [
                    "- 'Step 1: the pipeline schema expects step 'pvs_like_case_study' "
                    "but the provided pipeline specifies 'foo'. Check step order "
                    "and spelling in the pipeline configuration yaml.'",
                ],
            }
        },
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
        expected_msg={
            "IMPLEMENTATION ERRORS": {
                "pvs_like_python": [
                    "- Container 'some/path/with/no/container' does not exist.",
                ],
            },
        },
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
        "linker.implementation.Implementation._validate_container_exists",
        side_effect=lambda x: x,
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
        expected_msg={
            "IMPLEMENTATION ERRORS": {
                "pvs_like_python": [
                    "- Implementaton metadata step 'step-1' does not match pipeline configuration step 'pvs_like_case_study'"
                ]
            },
        },
    )


def test_bad_input_data(test_dir, caplog, mocker):
    mocker.patch(
        "linker.implementation.Implementation._validate_container_exists",
        side_effect=lambda x: x,
    )
    config = Config(
        f"{test_dir}/pipeline.yaml", f"{test_dir}/bad_columns_input_data.yaml", None
    )
    check_expected_validation_exit(
        config=config,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            "INPUT DATA ERRORS": {
                ".*/broken_file1.csv": ["Data file .* is missing required column\\(s\\) .*"],
                ".*/broken_file2.csv": ["Data file .* is missing required column\\(s\\) .*"],
            }
        },
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


def check_expected_validation_exit(config, caplog, error_no, expected_msg):
    with pytest.raises(SystemExit) as e:
        Pipeline(config)
        
    assert e.value.code == error_no
    # We should only have one record
    assert len(caplog.record_tuples) == 1
    # Extract error message
    msg = caplog.text.split("Validation errors found. Please see below.")[1].split(
        "Validation errors found. Please see above."
    )[0]
    msg = re.sub("\n+", " ", msg)
    msg = re.sub(" +", " ", msg).strip()
    msg = re.sub("''", "'", msg)
    all_matches = []
    for error_type, schemas in expected_msg.items():
        expected_pattern = [error_type + ":"]
        for schema, messages in schemas.items():
            expected_pattern.append(" " + schema + ":")
            for message in messages:
                expected_pattern.append(" " + message)
        pattern = re.compile("".join(expected_pattern))
        # regex_patterns.append(pattern)
        match = pattern.search(msg)
        assert match
        all_matches.append(match)

    covered_text = "".join(match.group(0) for match in all_matches)
    assert len(covered_text) == len(msg)
