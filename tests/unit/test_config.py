import errno
from pathlib import Path

import pytest

from linker.configuration import DEFAULT_ENVIRONMENT_VALUES, Config
from linker.step import Step
from tests.unit.conftest import (
    ENV_CONFIG_DICT,
    PIPELINE_CONFIG_DICT,
    check_expected_validation_exit,
)


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
    assert default_config.schema.steps == [Step("step_1"), Step("step_2")]


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
        "pipeline_specification": Path(f"{test_dir}/bad_step_pipeline.yaml"),
        "input_data": Path(f"{test_dir}/input_data.yaml"),
        "computing_environment": Path(f"{test_dir}/environment.yaml"),
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


FULLY_DEFINED_ENV_CONFIG = {
    "computing_environment": "foo-env",
    "container_engine": "foo-container",
    "slurm": {
        "account": "foo-account",
        "partition": "foo-partition",
    },
    "spark": {
        "workers": {
            "cpus_per_node": "foo-cpus",
            "mem_per_node": "foo-mem",
            "time_limit": "foo-time",
        },
        "keep_alive": "foo-keep-alive",
    },
}


@pytest.mark.parametrize(
    "environment_config, expected_config",
    [
        (
            # should result in minimum default environment
            {},
            {
                key: value
                for key, value in DEFAULT_ENVIRONMENT_VALUES.items()
                if key in ["computing_environment", "container_engine"]
            },
        ),
        (
            # check nested value: should add the missing slurm 'partition'
            {
                "slurm": {"account": DEFAULT_ENVIRONMENT_VALUES["slurm"]["account"]},
            },
            {
                key: value
                for key, value in DEFAULT_ENVIRONMENT_VALUES.items()
                if key in ["computing_environment", "container_engine", "slurm"]
            },
        ),
        (
            # check a double-nested value: should add the missing 'spark: workers: num_workers' and 'spark: keep_alive: True'
            {
                "spark": {
                    "workers": {
                        key: value
                        for key, value in DEFAULT_ENVIRONMENT_VALUES["spark"][
                            "workers"
                        ].items()
                        if key in ["cpus_per_node", "mem_per_node", "time_limit"]
                    },
                },
            },
            {
                key: value
                for key, value in DEFAULT_ENVIRONMENT_VALUES.items()
                if key in ["computing_environment", "container_engine", "spark"]
            },
        ),
        (
            # check that nothing is overwritten if fully defined
            FULLY_DEFINED_ENV_CONFIG,
            FULLY_DEFINED_ENV_CONFIG,
        ),
    ],
)
def test__assign_environment_defaults(
    default_config_params, environment_config, expected_config, mocker
):
    config_params = default_config_params.copy()
    config_params.update({"computing_environment": environment_config})
    # mock out the _load_computing_environment method to return the dict directly
    mocker.patch.object(
        Config,
        "_load_computing_environment",
        return_value=config_params["computing_environment"],
    )
    mocker.patch.object(
        Config,
        "_validate",
        return_value=[],
    )
    config = Config(**config_params)

    assert config.environment == expected_config

    # Check that values got assigned to attributes
    for key, value in expected_config.items():
        assert getattr(config, key) == value
