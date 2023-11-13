from pathlib import Path

from linker.configuration import Config
from linker.implementation import Implementation
from linker.pipeline import Pipeline
from linker.step import Step


def test_pipeline_instantiation(test_dir, config, mocker):
    mocker.patch(
        "linker.pipeline.Pipeline._get_steps_root_dir", return_value=Path(test_dir) / "steps"
    )
    pipeline = Pipeline(config)
    assert pipeline.config == config
    assert pipeline.steps == (Step("pvs_like_case_study"),)
    # Cannot assert identical b/c differing locations in memory
    assert type(pipeline.implementations) == tuple
    assert len(pipeline.implementations) == 1
    implementation = pipeline.implementations[0]
    assert type(implementation) == Implementation
    assert implementation.name == "pvs_like_python"
    assert implementation.step == Step("pvs_like_case_study")
    assert (
        implementation.directory
        == Path(test_dir) / "steps/pvs_like_case_study/implementations/pvs_like_python"
    )
    assert (
        implementation.container_full_stem
        == "some/path/to/container/directory/pvs_like_python"
    )


def test__get_steps(test_dir, mocker):
    # mock out _get_implementations() b/c it's tested elsewhere
    mocker.patch("linker.pipeline.Pipeline._get_implementations", return_value=None)
    config = Config(
        f"{test_dir}/pipeline.yaml",
        f"{test_dir}/input_data.yaml",
        f"{test_dir}/environment.yaml",
    )
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


def test_set_runner(config):
    pipeline = Pipeline(config)
    assert pipeline.runner is None
    pipeline.set_runner(lambda x: x)
    assert pipeline.runner is not None
    assert pipeline.runner("foo") == "foo"
