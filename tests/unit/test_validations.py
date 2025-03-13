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
from easylink.pipeline import IMPLEMENTATION_ERRORS_KEY, Pipeline
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml


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
        (
            "missing_implementations",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step step_1": [
                            "The step configuration does not contain an 'implementation' key or a reference to a combined implementation."
                        ]
                    },
                },
            },
        ),
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
        (
            "missing_step",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step choice_section": ["The step is not configured."],
                        "step step_1": ["The step is not configured."],
                        "step step_3": ["The step is not configured."],
                    },
                },
            },
        ),
        (
            "missing_loop_nodes",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step step_3": ["No loop instances configured under iterate key."],
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
                            "Loop instances must be formatted as a sequence in the pipeline configuration."
                        ],
                    },
                },
            },
        ),
        (
            "missing_substeps",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step step_4a": [
                            "The implementation configuration does not contain a 'name' key."
                        ],
                        "step step_4b": ["The step is not configured."],
                    },
                },
            },
        ),
        (
            "wrong_parallel_split_keys",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step step_1": {
                            "parallel_split_1": {
                                "Input Data Key": [
                                    "Input data file 'foo' not found in input data configuration."
                                ],
                            },
                        },
                    },
                },
            },
        ),
        (
            "bad_combined_implementations",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step step_3": [
                            "The step refers to a combined implementation but 'foo' is not a valid combined implementation."
                        ]
                    }
                }
            },
        ),
        (
            "missing_type_key",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step choice_section": ["The step requires a 'type' key."]
                    }
                }
            },
        ),
        (
            "bad_type_key",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step choice_section": [
                            "'foo' is not a supported 'type'. Valid choices are: \['simple', 'complex'\]."
                        ]
                    }
                }
            },
        ),
        (
            "type_config_mismatch",
            {
                PIPELINE_ERRORS_KEY: {
                    "development": {
                        "step choice_section": [
                            "'step_4' is not configured. Confirm you have specified the correct steps for the 'simple' type."
                        ]
                    }
                }
            },
        ),
    ],
)
def test_pipeline_validation(
    pipeline, expected_msg, default_config_params, unit_test_specifications_dir, caplog
):
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_{pipeline}.yaml"
    )

    with pytest.raises(SystemExit) as e:
        Config(config_params)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg=expected_msg,
    )


def test_out_of_order_steps(default_config_params, unit_test_specifications_dir):
    # Make sure we DON'T raise an exception even if the steps are out of order
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_out_of_order.yaml"
    )
    Config(config_params)


def test_unsupported_step(
    default_config_params, unit_test_specifications_dir, caplog, mocker
):
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    mocker.patch("easylink.implementation.Implementation.validate", return_value=[])
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_bad_step.yaml"
    )

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


def test_unsupported_implementation(
    default_config_params, unit_test_specifications_dir, caplog, mocker
):
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    mocker.patch("easylink.implementation.Implementation.validate", return_value=[])
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_bad_implementation.yaml"
    )

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
    config_params = load_params_from_specification(**config_paths)
    config_params["input_data"] = {}
    with pytest.raises(SystemExit) as e:
        Config(config_params)

    _check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={INPUT_DATA_ERRORS_KEY: ["No input data is configured."]},
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
                r".*/file1.oops": [r"Data file type .oops is not supported. Convert to .*"],
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
                ".*/broken_file1.csv": [r"Data file .* is missing required column\(s\) .*"],
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
            IMPLEMENTATION_ERRORS_KEY: {
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
    metadata["step_1_python_pandas"]["steps"] = ["not-the-step-1-name"]
    metadata["step_2_python_pandas"]["steps"] = ["not-the-step-2-name"]
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
            IMPLEMENTATION_ERRORS_KEY: {
                "step_1_python_pandas": [
                    r"Pipeline configuration nodes \['step_1'\] do not match metadata steps \['not-the-step-1-name'\]."
                ],
                "step_2_python_pandas": [
                    r"Pipeline configuration nodes \['step_2'\] do not match metadata steps \['not-the-step-2-name'\]."
                ],
            },
        },
    )
