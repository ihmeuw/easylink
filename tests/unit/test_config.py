import errno
from pathlib import Path

import pytest

from linker.configuration import (
    DEFAULT_ENVIRONMENT,
    ENVIRONMENT_ERRORS_KEY,
    INPUT_DATA_ERRORS_KEY,
    PIPELINE_ERRORS_KEY,
    Config,
)
from linker.step import Step
from linker.utilities import paths
from linker.utilities.data_utils import load_yaml
from tests.unit.conftest import check_expected_validation_exit


@pytest.mark.parametrize("requires_spark", [True, False])
def test__determine_if_spark_is_required(test_dir, requires_spark):
    pipeline = load_yaml(f"{test_dir}/pipeline.yaml")
    if requires_spark:
        # Change step 1's implementation to python_pyspark
        pipeline["steps"]["step_1"]["implementation"][
            "name"
        ] = "step_1_python_pyspark_distributed"
    is_required = Config._determine_if_spark_is_required(pipeline)
    assert is_required == requires_spark


def test__get_schema(default_config):
    """Test that the schema is correctly loaded from the pipeline.yaml"""
    assert default_config.schema.steps == [Step("step_1"), Step("step_2")]


def test__load_input_data_paths(test_dir):
    paths = Config._load_input_data_paths(f"{test_dir}/input_data.yaml")
    assert paths == [Path(f"{test_dir}/input_data{n}/file{n}.csv") for n in [1, 2]]


def test_input_data_configuration_requires_key_value_pairs(test_dir):
    config_params = {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
        "input_data": f"{test_dir}/input_data_list.yaml",
        "computing_environment": None,
    }
    with pytest.raises(
        TypeError, match="Input data should be submitted like 'key': path/to/file."
    ):
        Config(**config_params)


@pytest.mark.parametrize(
    "computing_environment",
    [
        "bad/path/to/environment.yaml",
        Path("another/bad/path"),
    ],
)
def test_environment_configuration_not_found(default_config_params, computing_environment):
    config_params = default_config_params
    config_params.update(
        {"input_data": "foo", "computing_environment": computing_environment}
    )
    with pytest.raises(FileNotFoundError):
        Config(**config_params)


@pytest.mark.parametrize(
    "key, input",
    [
        ("computing_environment", None),
        ("computing_environment", "local"),
        ("computing_environment", "slurm"),
        ("container_engine", None),
        ("container_engine", "docker"),
        ("container_engine", "singularity"),
        ("container_engine", "undefined"),
    ],
)
def test__get_required_attribute(key, input):
    env_dict = {key: input} if input else {}
    retrieved = Config._get_required_attribute(env_dict, key)
    expected = DEFAULT_ENVIRONMENT.copy()
    expected.update(env_dict)
    assert retrieved == expected[key]


@pytest.mark.parametrize(
    "input",
    [
        # missing
        None,
        # partially defined
        {"memory": 100},
        # fully defined
        {"memory": 100, "cpus": 200, "time_limit": 300},
    ],
)
def test__get_implementation_resource_requests(input):
    key = "implementation_resources"
    env_dict = {key: input.copy()} if input else {}
    retrieved = Config._get_implementation_resource_requests(env_dict)
    if input:
        expected = DEFAULT_ENVIRONMENT[key].copy()
        expected.update(env_dict[key])
    else:
        expected = {}
    assert retrieved == expected


@pytest.mark.parametrize(
    "input",
    [
        # missing
        None,
        # partially defined (missing entire workers)
        {"keep_alive": "idk"},
        # partially defined (missing keep_alive and num_workers)
        {
            "workers": {
                "cpus_per_node": 200,
                "mem_per_node": 300,
                "time_limit": 400,
            }
        },
        # fully defined
        {
            "workers": {
                "num_workers": 100,
                "cpus_per_node": 200,
                "mem_per_node": 300,
                "time_limit": 400,
            },
            "keep_alive": "idk",
        },
    ],
)
def test__get_spark_requests(input):
    key = "spark"
    env_dict = {key: input.copy()} if input else {}
    for requires_spark in [False, True]:
        retrieved = Config._get_spark_requests(env_dict, requires_spark)
        if requires_spark:
            expected = DEFAULT_ENVIRONMENT[key].copy()
            if input:
                expected.update(env_dict[key])
        else:
            expected = {}
        assert retrieved == expected


@pytest.mark.parametrize("includes_implementation_configuration", [False, True])
def test_get_implementation_specific_configuration(
    default_config, includes_implementation_configuration
):
    config = default_config
    step_1_config = {}
    step_2_config = {}
    if includes_implementation_configuration:
        step_2_config = {
            "SOME-CONFIGURATION": "some-value",
            "SOME-OTHER-CONFIGURATION": "some-other-value",
        }
        config.pipeline["steps"]["step_2"]["implementation"]["configuration"] = step_2_config
    assert config.get_implementation_specific_configuration("step_1") == step_1_config
    assert config.get_implementation_specific_configuration("step_2") == step_2_config


####################
# validation tests #
####################


# Pipeline validations
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

    check_expected_validation_exit(
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

    check_expected_validation_exit(
        error=e,
        caplog=caplog,
        error_no=errno.EINVAL,
        expected_msg={
            PIPELINE_ERRORS_KEY: {
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
    check_expected_validation_exit(
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


# Input data validations
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


def test_missing_input_file(test_dir, caplog):
    config_params = {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
        "input_data": f"{test_dir}/missing_input_data.yaml",
        "computing_environment": None,
    }
    with pytest.raises(SystemExit) as e:
        Config(**config_params)

    check_expected_validation_exit(
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
    check_expected_validation_exit(
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
    check_expected_validation_exit(
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
