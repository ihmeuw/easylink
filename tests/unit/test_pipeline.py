import pytest

from linker.configuration import Config
from linker.pipeline import Pipeline
from linker.step import Step


def test__get_steps(config, mocker):
    mocker.patch("linker.pipeline.Pipeline._validate")
    mocker.patch("linker.implementation.Implementation._load_metadata")
    mocker.patch("linker.implementation.Implementation._get_container_full_stem")
    mocker.patch("linker.implementation.Implementation._validate")
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


def test_unsupported_step(test_dir, mocker):
    mocker.patch("linker.implementation.Implementation._load_metadata")
    mocker.patch("linker.implementation.Implementation._get_container_full_stem")
    mocker.patch("linker.implementation.Implementation._validate")
    config = Config(
        f"{test_dir}/bad_step_pipeline.yaml",  # pipeline with unsupported step
        f"{test_dir}/input_data.yaml",
        f"{test_dir}/environment.yaml",
    )
    with pytest.raises(RuntimeError, match="Pipeline is not valid."):
        Pipeline(config)


@pytest.mark.skip(reason="TODO when multiple steps are implemented")
def test_bad_step_order():
    pass


@pytest.mark.skip(reason="TODO when multiple steps are implemented")
def test_missing_a_step():
    pass
