""" This module contains tests for the validation methods for the various
classes in this package. These pseudo-unit tests have been separated from the
class- and module-specific test modules simply to make them easier to find and
maintain.
"""

import errno
import re
from pathlib import Path

import pytest

from linker.configuration import (
    ENVIRONMENT_ERRORS_KEY,
    INPUT_DATA_ERRORS_KEY,
    PIPELINE_ERRORS_KEY,
    Config,
)
from linker.pipeline import Pipeline
from linker.utilities import paths
from linker.utilities.data_utils import load_yaml


def _check_expected_validation_exit(error, caplog, error_no, expected_msg):
    """Check that the validation messages are as expected. It's hacky."""
    assert error.value.code == error_no
    # Extract error message
    msg = caplog.text.split("Validation errors found. Please see below.")[1].split(
        "Validation errors found. Please see above."
    )[0]
    msg = re.sub("\n+", " ", msg)
    msg = re.sub(" +", " ", msg).strip()
    # Remove single quotes from msg and expected b/c they're difficult to handle and not that important
    msg = re.sub("'+", "", msg)
    all_matches = []
    for error_type, context in expected_msg.items():
        expected_pattern = [error_type + ":"]
        for item, messages in context.items():
            expected_pattern.append(" " + item + ":")
            for message in messages:
                message = re.sub("'+", "", message)
                expected_pattern.append(" " + message)
        pattern = re.compile("".join(expected_pattern))
        match = pattern.search(msg)
        assert match
        all_matches.append(match)

    covered_text = "".join(match.group(0) for match in all_matches)
    assert len(covered_text) == len(msg)


@pytest.mark.skip(reason="TODO [MIC-4735]")
def test_batch_validation():
    pass


######################
# Config validations #
######################


@pytest.mark.parametrize(
    "pipeline, expected_msg",
    [
        # missing 'implementation' key
        (
            "missing_implementation_pipeline.yaml",
            {
                PIPELINE_ERRORS_KEY: {
                    "step step_1": ["Does not contain an 'implementation'."]
                },
            },
        ),
        # missing implementation 'name' key
        (
            "missing_implementation_name_pipeline.yaml",
            {
                PIPELINE_ERRORS_KEY: {
                    "step step_1": ["The implementation does not contain a 'name'."]
                },
            },
        ),
        # steps are out of order
        (
            "out_of_order_pipeline.yaml",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": [
                        "- Step 1: the pipeline schema expects step step_1 but "
                        "the provided pipeline specifies step_2. Check step order "
                        "and spelling in the pipeline configuration yaml. "
                        "- Step 2: the pipeline schema expects step step_2 but "
                        "the provided pipeline specifies step_1. Check step order "
                        "and spelling in the pipeline configuration yaml."
                    ],
                    "pvs_like_case_study": [
                        "- Expected 1 steps but found 2 implementations. Check "
                        "that all steps are accounted for \\(and there are no extraneous "
                        "ones\\) in the pipeline configuration yaml."
                    ],
                },
            },
        ),
        # missing a step
        (
            "missing_step_pipeline.yaml",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": [
                        "- Expected 2 steps but found 1 implementations. Check that "
                        "all steps are accounted for \\(and there are no extraneous "
                        "ones\\) in the pipeline configuration yaml.",
                    ],
                    "pvs_like_case_study": [
                        "- 'Step 1: the pipeline schema expects step 'pvs_like_case_study' "
                        "but the provided pipeline specifies 'step_2'. Check step order "
                        "and spelling in the pipeline configuration yaml.'",
                    ],
                },
            },
        ),
    ],
)
def test_pipeline_validation(pipeline, expected_msg, test_dir, caplog, mocker):
    mocker.patch(
        "linker.configuration.Config._determine_if_spark_is_required", return_value=False
    )
    config_params = {
        "pipeline_specification": Path(f"{test_dir}/{pipeline}"),
        "input_data": Path(f"{test_dir}/input_data.yaml"),
        "computing_environment": Path(f"{test_dir}/environment.yaml"),
    }

    with pytest.raises(SystemExit) as e:
        Config(**config_params)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg=expected_msg,
    )


def test_unsupported_step(test_dir, caplog, mocker):
    mocker.patch("linker.implementation.Implementation._load_metadata")
    mocker.patch("linker.implementation.Implementation._get_container_full_stem")
    mocker.patch("linker.implementation.Implementation.validate", return_value=[])
    config_params = {
        "pipeline_specification": Path(f"{test_dir}/bad_step_pipeline.yaml"),
        "input_data": Path(f"{test_dir}/input_data.yaml"),
        "computing_environment": Path(f"{test_dir}/environment.yaml"),
    }

    with pytest.raises(SystemExit) as e:
        Config(**config_params)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            PIPELINE_ERRORS_KEY: {
                "development": [
                    "- Expected 2 steps but found 1 implementations. Check that "
                    "all steps are accounted for \\(and there are no extraneous "
                    "ones\\) in the pipeline configuration yaml.",
                ],
                "pvs_like_case_study": [
                    "- 'Step 1: the pipeline schema expects step 'pvs_like_case_study' "
                    "but the provided pipeline specifies 'foo'. Check step order "
                    "and spelling in the pipeline configuration yaml.'",
                ],
            }
        },
    )


def test_unsupported_implementation(test_dir, caplog, mocker):
    mocker.patch("linker.implementation.Implementation._load_metadata")
    mocker.patch("linker.implementation.Implementation._get_container_full_stem")
    mocker.patch("linker.implementation.Implementation.validate", return_value=[])
    mocker.patch(
        "linker.configuration.Config._determine_if_spark_is_required", return_value=False
    )
    config_params = {
        "pipeline_specification": Path(f"{test_dir}/bad_implementation_pipeline.yaml"),
        "input_data": Path(f"{test_dir}/input_data.yaml"),
        "computing_environment": Path(f"{test_dir}/environment.yaml"),
    }

    with pytest.raises(SystemExit) as e:
        Config(**config_params)

    implementation_metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
    supported_implementations = (
        str(list(implementation_metadata.keys())).replace("[", "\\[").replace("]", "\\]")
    )
    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            PIPELINE_ERRORS_KEY: {
                "step step_1": [
                    f"Implementation 'foo' is not supported. Supported implementations are: {supported_implementations}."
                ],
            }
        },
    )


def test_pipeline_schema_bad_input_data_type(test_dir, caplog):
    config_params = {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
        "input_data": f"{test_dir}/bad_type_input_data.yaml",
        "computing_environment": None,
    }

    with pytest.raises(SystemExit) as e:
        Config(**config_params)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            INPUT_DATA_ERRORS_KEY: {
                ".*/file1.oops": ["- Data file type .oops is not supported. Convert to .*"],
                ".*/file2.oops": ["- Data file type .oops is not supported. Convert to .*"],
            },
        },
    )


def test_pipeline_schema_bad_input_data(test_dir, caplog):
    config_params = {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
        "input_data": f"{test_dir}/bad_columns_input_data.yaml",
        "computing_environment": None,
    }

    with pytest.raises(SystemExit) as e:
        Config(**config_params)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            INPUT_DATA_ERRORS_KEY: {
                ".*/broken_file1.csv": [
                    "- Data file .* is missing required column\\(s\\) .*"
                ],
                ".*/broken_file2.csv": [
                    "- Data file .* is missing required column\\(s\\) .*"
                ],
            }
        },
    )


def test_pipeline_schema_missing_input_file(test_dir, caplog):
    config_params = {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
        "input_data": f"{test_dir}/missing_input_data.yaml",
        "computing_environment": None,
    }
    with pytest.raises(SystemExit) as e:
        Config(**config_params)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            INPUT_DATA_ERRORS_KEY: {
                ".*/missing_file1.csv": ["File not found."],
                ".*/missing_file2.csv": ["File not found."],
            },
        },
    )


# Environment validations
def test_unsupported_container_engine(test_dir, caplog, mocker):
    config_params = {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
        "input_data": f"{test_dir}/input_data.yaml",
        "computing_environment": None,
    }
    mocker.patch(
        "linker.configuration.Config._get_required_attribute",
        side_effect=lambda _env, attribute: "foo"
        if attribute == "container_engine"
        else None,
    )
    with pytest.raises(SystemExit) as e:
        Config(**config_params)
    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            "ENVIRONMENT ERRORS": {
                "container_engine": ["The value 'foo' is not supported."],
            },
        },
    )


def test_missing_slurm_details(test_dir, caplog, mocker):
    mocker.patch(
        "linker.configuration.Config._get_required_attribute",
        side_effect=lambda _env, attribute: "slurm"
        if attribute == "computing_environment"
        else "undefined",
    )
    config_params = {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
        "input_data": f"{test_dir}/input_data.yaml",
        "computing_environment": None,
    }
    with pytest.raises(SystemExit) as e:
        Config(**config_params)
    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            ENVIRONMENT_ERRORS_KEY: {
                "slurm": [
                    "The environment configuration file must include a 'slurm' key "
                    "defining slurm resources if the computing_environment is 'slurm'."
                ],
            },
        },
    )


############
# pipeline #
############


def test_no_container(default_config, caplog, mocker):
    mocker.patch(
        "linker.implementation.Implementation._get_container_full_stem",
        return_value=Path("some/path/with/no/container"),
    )
    mocker.PropertyMock(
        "linker.implementation.Implementation._container_engine", return_value="undefined"
    )
    with pytest.raises(SystemExit) as e:
        Pipeline(default_config)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            "IMPLEMENTATION ERRORS": {
                "step_1_python_pandas": [
                    "- Container 'some/path/with/no/container' does not exist.",
                ],
                "step_2_python_pandas": [
                    "- Container 'some/path/with/no/container' does not exist.",
                ],
            },
        },
    )


def test_implemenation_does_not_match_step(test_dir, caplog, mocker):
    mocker.patch(
        "linker.implementation.Implementation._load_metadata",
        return_value={
            "step_1_python_pandas": {
                "step": "not-the-step-1-name",
                "image_path": "/some/path",
                "name": "some-name",
            },
            "step_2_python_pandas": {
                "step": "not-the-step-2-name",
                "image_path": "/some/path",
                "name": "some-name",
            },
        },
    )
    mocker.patch(
        "linker.implementation.Implementation._validate_container_exists",
        side_effect=lambda x: x,
    )
    config_params = {
        "pipeline_specification": Path(f"{test_dir}/pipeline.yaml"),
        "input_data": Path(f"{test_dir}/input_data.yaml"),
        "computing_environment": Path(f"{test_dir}/environment.yaml"),
    }
    config = Config(**config_params)
    with pytest.raises(SystemExit) as e:
        Pipeline(config)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            "IMPLEMENTATION ERRORS": {
                "step_1_python_pandas": [
                    "- Implementaton metadata step 'not-the-step-1-name' does not match pipeline configuration step 'step_1'"
                ],
                "step_2_python_pandas": [
                    "- Implementaton metadata step 'not-the-step-2-name' does not match pipeline configuration step 'step_2'"
                ],
            },
        },
    )
