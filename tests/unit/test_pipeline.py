import errno
from pathlib import Path

import pytest

from linker.configuration import Config
from linker.pipeline import Pipeline
from tests.unit.conftest import check_expected_validation_exit


@pytest.mark.skip(reason="TODO when multiple steps are implemented")
def test_bad_step_order():
    pass


@pytest.mark.skip(reason="TODO when multiple steps are implemented")
def test_missing_a_step():
    pass


@pytest.mark.skip(reason="TODO [MIC-4735]")
def test_batch_validation():
    pass


def test__get_implementations(default_config):
    pipeline = Pipeline(default_config)
    implementation_names = [
        implementation.name for implementation in pipeline.implementations
    ]
    assert implementation_names == ["pvs_like_python"]


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
    config = Config(**config_params)
    with pytest.raises(SystemExit) as e:
        Pipeline(config)

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
    config = Config(**config_params)
    with pytest.raises(SystemExit) as e:
        Pipeline(config)

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
