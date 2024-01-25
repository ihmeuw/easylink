import errno
import re
from pathlib import Path

import pytest

from linker.configuration import Config
from linker.step import Step
from tests.unit.conftest import ENV_CONFIG_DICT, PIPELINE_CONFIG_DICT


def test_config_instantiation(test_dir, default_config):
    assert default_config.pipeline == PIPELINE_CONFIG_DICT["good"]
    assert default_config.environment == ENV_CONFIG_DICT
    assert default_config.input_data == [
        Path(x) for x in [f"{test_dir}/input_data{n}/file{n}.csv" for n in [1, 2]]
    ]
    assert default_config.computing_environment == ENV_CONFIG_DICT["computing_environment"]
    assert default_config.container_engine == ENV_CONFIG_DICT["container_engine"]


def test__get_schema(default_config):
    """Test that the schema is correctly loaded from the pipeline.yaml"""
    assert default_config.schema.steps == [Step("pvs_like_case_study")]


def test__get_implementations(default_config):
    implementation_names = [
        implementation.name for implementation in default_config.implementations
    ]
    assert implementation_names == ["pvs_like_python"]


@pytest.mark.parametrize("input_data", ["good", "bad"])
def test__load_input_data_paths(test_dir, input_data):
    if input_data == "good":
        paths = Config._load_input_data_paths(f"{test_dir}/input_data.yaml")
        assert paths == [Path(f"{test_dir}/input_data{n}/file{n}.csv") for n in [1, 2]]
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
def test_bad_computing_environment_fails(
    default_config_params, dummy_config, computing_environment
):
    config_params = default_config_params
    config_params.update(
        {"input_data": "foo", "computing_environment": computing_environment}
    )

    with pytest.raises(FileNotFoundError):
        dummy_config(config_params)


def test_default_computing_environment(default_config_params, dummy_config):
    """The computing environment value should default to 'local'"""
    config_params = default_config_params
    config_params.update({"computing_environment": None})
    config = dummy_config(config_params)
    assert config.computing_environment == "local"


def test_default_container_engine(default_config_params, dummy_config):
    """The container engine value should default to 'undefined'"""
    config_params = default_config_params
    config_params.update({"computing_environment": None})
    config = dummy_config(config_params)
    assert config.container_engine == "undefined"


def test_unsupported_step(test_dir, caplog, dummy_config, mocker):
    mocker.patch("linker.implementation.Implementation._load_metadata")
    mocker.patch("linker.implementation.Implementation._get_container_full_stem")
    mocker.patch("linker.implementation.Implementation.validate", return_value=[])
    config_params = {
        "pipeline_specification": f"{test_dir}/bad_step_pipeline.yaml",
        "input_data": f"{test_dir}/input_data.yaml",
        "computing_environment": f"{test_dir}/environment.yaml",
    }

    with pytest.raises(SystemExit) as e:
        dummy_config(config_params)

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


def test_no_container(test_dir, caplog, mocker):
    mocker.patch(
        "linker.implementation.Implementation._get_container_full_stem",
        return_value=Path("some/path/with/no/container"),
    )
    mocker.PropertyMock(
        "linker.implementation.Implementation._container_engine", return_value="unknown"
    )
    config_params = {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
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
    config_params = {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
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
            "IMPLEMENTATION ERRORS": {
                "pvs_like_python": [
                    "- Implementaton metadata step 'step-1' does not match pipeline configuration step 'pvs_like_case_study'"
                ]
            },
        },
    )


def test_bad_input_data(test_dir, dummy_config, caplog):
    config_params = {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
        "input_data": f"{test_dir}/bad_columns_input_data.yaml",
        "computing_environment": None,
    }

    with pytest.raises(SystemExit) as e:
        dummy_config(config_params)

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


####################
# HELPER FUNCTIONS #
####################


def check_expected_validation_exit(error, caplog, error_no, expected_msg):
    assert error.value.code == error_no
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
