""" This module contains tests for the validation methods for the various
classes in this package. These pseudo-unit tests have been separated from the
class- and module-specific test modules simply to make them easier to find and
maintain.
"""

import errno
import re

import pytest

from easylink.configuration import (
    ENVIRONMENT_ERRORS_KEY,
    INPUT_DATA_ERRORS_KEY,
    PIPELINE_ERRORS_KEY,
    Config,
    load_params_from_specification,
)
from easylink.pipeline import Pipeline
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml
from tests.unit.conftest import PIPELINE_CONFIG_DICT


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

    def append_to_pattern(pattern, context):
        for item, messages in context.items():
            pattern.append(" " + item + ":")
            if isinstance(messages, dict):
                append_to_pattern(pattern, messages)
            else:
                for message in messages:
                    message = re.sub("'+", "", message)
                    pattern.append(" - " + message)
        return pattern

    all_matches = []
    for error_type, context in expected_msg.items():
        expected_pattern = append_to_pattern([error_type + ":"], context)
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
            "missing_implementations",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step step_1": [
                            "The step configuration does not contain an 'implementation' key."
                        ]
                    },
                },
            },
        ),
        # missing implementation 'name' key
        (
            "missing_implementation_name",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step step_1": [
                            "The implementation configuration does not contain a 'name' key."
                        ]
                    },
                },
            },
        ),
        # missing a step
        (
            "missing_step",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step step_1": ["The step is not configured."],
                        "step step_3": ["The step is not configured."],
                        "step step_4": ["The step is not configured."],
                    },
                },
            },
        ),
        (
            "missing_loop_nodes",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step step_3": ["No loops configured under iterate key."],
                    },
                },
            },
        ),
        (
            "bad_loop_formatting",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step step_3": [
                            "Loops must be formatted as a sequence in the pipeline configuration."
                        ],
                    },
                },
            },
        ),
        (
            "missing_substep_keys",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step step_1a": [
                            "The implementation configuration does not contain a 'name' key."
                        ],
                        "step step_1b": ["The step is not configured."],
                    },
                },
            },
        ),
    ],
)
def test_pipeline_validation(pipeline, default_config_params, expected_msg, caplog):
    config_params = default_config_params
    config_params["pipeline"] = PIPELINE_CONFIG_DICT[pipeline]

    with pytest.raises(SystemExit) as e:
        Config(config_params)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg=expected_msg,
    )


def test_out_of_order_steps(default_config_params):
    # Make sure we DON'T raise an exception even if the steps are out of order
    config_params = default_config_params
    config_params["pipeline"] = PIPELINE_CONFIG_DICT["out_of_order"]
    Config(config_params)


def test_unsupported_step(default_config_params, caplog, mocker):
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    mocker.patch("easylink.implementation.Implementation.validate", return_value=[])
    config_params = default_config_params
    config_params["pipeline"] = PIPELINE_CONFIG_DICT["bad_step"]

    with pytest.raises(SystemExit) as e:
        Config(config_params)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            PIPELINE_ERRORS_KEY: {
                "development": {
                    "step foo": ["foo is not a valid step."],
                }
            }
        },
    )


def test_unsupported_implementation(default_config_params, caplog, mocker):
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    mocker.patch("easylink.implementation.Implementation.validate", return_value=[])
    config_params = default_config_params
    config_params["pipeline"] = PIPELINE_CONFIG_DICT["bad_implementation"]

    with pytest.raises(SystemExit) as e:
        Config(config_params)

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
                "development": {
                    "step step_1": [
                        f"Implementation 'foo' is not supported. Supported implementations are: {supported_implementations}."
                    ],
                },
            }
        },
    )


def test_pipeline_schema_bad_input_data_type(default_config_paths, test_dir, caplog):
    config_paths = default_config_paths
    config_paths.update(
        {"input_data": f"{test_dir}/bad_type_input_data.yaml", "computing_environment": None}
    )
    config_params = load_params_from_specification(**config_paths)
    with pytest.raises(SystemExit) as e:
        Config(config_params)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            INPUT_DATA_ERRORS_KEY: {
                ".*/file1.oops": ["Data file type .oops is not supported. Convert to .*"],
            },
        },
    )


def test_pipeline_schema_bad_input_data(default_config_paths, test_dir, caplog):
    config_paths = default_config_paths
    config_paths.update(
        {
            "input_data": f"{test_dir}/bad_columns_input_data.yaml",
            "computing_environment": None,
        }
    )
    config_params = load_params_from_specification(**config_paths)
    with pytest.raises(SystemExit) as e:
        Config(config_params)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            INPUT_DATA_ERRORS_KEY: {
                ".*/broken_file1.csv": ["Data file .* is missing required column\\(s\\) .*"],
            }
        },
    )


def test_pipeline_schema_missing_input_file(default_config_paths, test_dir, caplog):
    config_paths = default_config_paths
    config_paths.update(
        {"input_data": f"{test_dir}/missing_input_data.yaml", "computing_environment": None}
    )
    config_params = load_params_from_specification(**config_paths)
    with pytest.raises(SystemExit) as e:
        Config(config_params)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            INPUT_DATA_ERRORS_KEY: {
                ".*/missing_file1.csv": ["File not found."],
            },
        },
    )


# Environment validations
def test_unsupported_container_engine(default_config_params, caplog):
    config_params = default_config_params
    config_params["environment"] = {"container_engine": "foo"}
    with pytest.raises(SystemExit) as e:
        Config(config_params)
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


def test_missing_slurm_details(default_config_params, caplog):
    config_params = default_config_params
    config_params["environment"] = {"computing_environment": "slurm"}
    with pytest.raises(SystemExit) as e:
        Config(config_params)
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
    metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
    metadata["step_1_python_pandas"]["image_path"] = "some/path/with/no/container.sif"
    metadata["step_2_python_pandas"]["image_path"] = "some/path/with/no/container_2.sif"
    metadata["step_3_python_pandas"]["image_path"] = "some/path/with/no/container_3.sif"
    metadata["step_4_python_pandas"]["image_path"] = "some/path/with/no/container_4.sif"
    mocker.patch("easylink.implementation.load_yaml", return_value=metadata)
    mocker.PropertyMock(
        "easylink.implementation.Implementation._container_engine", return_value="undefined"
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
                    "Container 'some/path/with/no/container.sif' does not exist.",
                ],
                "step_2_python_pandas": [
                    "Container 'some/path/with/no/container_2.sif' does not exist.",
                ],
                "step_3_python_pandas": [
                    "Container 'some/path/with/no/container_3.sif' does not exist.",
                ],
                "step_4_python_pandas": [
                    "Container 'some/path/with/no/container_4.sif' does not exist.",
                ],
            },
        },
    )


def test_implemenation_does_not_match_step(default_config, caplog, mocker):
    metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
    metadata["step_1_python_pandas"]["step"] = "not-the-step-1-name"
    metadata["step_2_python_pandas"]["step"] = "not-the-step-2-name"
    mocker.patch("easylink.implementation.load_yaml", return_value=metadata)
    mocker.patch(
        "easylink.implementation.Implementation._validate_container_exists",
        side_effect=lambda x: x,
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
                    "Implementaton metadata step 'not-the-step-1-name' does not match pipeline configuration step 'step_1'"
                ],
                "step_2_python_pandas": [
                    "Implementaton metadata step 'not-the-step-2-name' does not match pipeline configuration step 'step_2'"
                ],
            },
        },
    )
