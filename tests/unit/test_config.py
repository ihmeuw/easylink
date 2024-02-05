import errno
from pathlib import Path

import pytest

from linker.configuration import Config
from linker.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema
from linker.pipeline_schema_constants import ALLOWED_SCHEMA_PARAMS
from linker.step import Step
from tests.unit.conftest import (
    ENV_CONFIG_DICT,
    PIPELINE_CONFIG_DICT,
    check_expected_validation_exit,
)


def test_config_instantiation(test_dir, default_config):
    assert default_config.pipeline == PIPELINE_CONFIG_DICT["good"]
    assert default_config.environment == ENV_CONFIG_DICT
    assert list(default_config.input_data.values()) == [
        Path(x) for x in [f"{test_dir}/input_data{n}/file{n}.csv" for n in [1, 2]]
    ]
    assert default_config.computing_environment == ENV_CONFIG_DICT["computing_environment"]
    assert default_config.container_engine == ENV_CONFIG_DICT["container_engine"]


def test__get_schema(default_config):
    """Test that the schema is correctly loaded from the pipeline.yaml"""
    assert default_config.schema == PIPELINE_SCHEMAS[1]


@pytest.mark.parametrize("input_data", ["good", "bad"])
def test__load_input_data_paths(test_dir, input_data):
    if input_data == "good":
        paths = Config._load_input_data_paths(f"{test_dir}/input_data.yaml")
        assert list(paths.values()) == [
            Path(f"{test_dir}/input_data{n}/file{n}.csv") for n in [1, 2]
        ]
    if input_data == "bad":
        with pytest.raises(RuntimeError, match=r"Cannot find input data: .*"):
            Config._load_input_data_paths(f"{test_dir}/bad_input_data.yaml")


@pytest.mark.parametrize(
    "computing_environment",
    [
        "foo",
        "bad/path/to/environment.yaml",
        Path("another/bad/path"),
    ],
)
def test_bad_computing_environment_fails(default_config_params, computing_environment):
    config_params = default_config_params
    config_params.update(
        {"input_data": "foo", "computing_environment": computing_environment}
    )

    with pytest.raises(FileNotFoundError):
        Config(**config_params)


def test_default_computing_environment(default_config_params):
    """The computing environment value should default to 'local'"""
    config_params = default_config_params
    config_params.update({"computing_environment": None})
    config = Config(**config_params)
    assert config.computing_environment == "local"


def test_default_container_engine(default_config_params):
    """The container engine value should default to 'undefined'"""
    config_params = default_config_params
    config_params.update({"computing_environment": None})
    config = Config(**config_params)
    assert config.container_engine == "undefined"


def test_unsupported_step(test_dir, caplog, mocker):
    mocker.patch("linker.implementation.Implementation._load_metadata")
    mocker.patch("linker.implementation.Implementation._get_container_full_stem")
    mocker.patch("linker.implementation.Implementation.validate", return_value=[])
    config_params = {
        "pipeline_specification": f"{test_dir}/bad_step_pipeline.yaml",
        "input_data": f"{test_dir}/input_data.yaml",
        "computing_environment": f"{test_dir}/environment.yaml",
    }

    with pytest.raises(SystemExit) as e:
        Config(**config_params)

    check_expected_validation_exit(
        error=e,
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


def test_bad_input_data(test_dir, caplog):
    config_params = {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
        "input_data": f"{test_dir}/bad_columns_input_data.yaml",
        "computing_environment": None,
    }

    with pytest.raises(SystemExit) as e:
        Config(**config_params)

    check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            "INPUT DATA ERRORS": {
                ".*/broken_file1.csv": [
                    "- Data file .* is missing required column\\(s\\) .*"
                ],
                ".*/broken_file2.csv": [
                    "- Data file .* is missing required column\\(s\\) .*"
                ],
            }
        },
    )


def test_missing_step_input(default_config_params, caplog, mocker):
    mock_input_dependency = "file2"
    mock_pipeline_schemas = ALLOWED_SCHEMA_PARAMS
    mock_pipeline_schemas["development"]["steps"]["step_2"]["inputs"][
        "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS"
    ]["input_filenames"] = [mock_input_dependency]
    mocker.patch("linker.pipeline_schema.ALLOWED_SCHEMA_PARAMS", mock_pipeline_schemas)
    mocker.patch("linker.configuration.PIPELINE_SCHEMAS", PipelineSchema._get_schemas())
    config_params = default_config_params
    config_params.update({"computing_environment": None})

    with pytest.raises(SystemExit) as e:
        Config(**config_params)

    check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            "STEP INPUT ERRORS": {
                "step_2": [
                    f"- Step requires input data key {mock_input_dependency} but it was not found in the input data."
                ],
            }
        },
    )
