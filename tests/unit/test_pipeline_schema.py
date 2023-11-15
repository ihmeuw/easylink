from pathlib import Path

import pytest

from linker.configuration import Config
from linker.pipeline import Pipeline
from linker.pipeline_schema import PipelineSchema, validate_pipeline


def test_schema_instantiation():
    schema = PipelineSchema("foo")
    assert schema.name == "foo"
    assert schema.steps == []  # Have not requested supported schemas yet


def test_get_schemas():
    schema = PipelineSchema("foo")
    supported_schemas = schema.get_schemas()
    assert type(supported_schemas) == list
    # Ensure list is populated
    assert supported_schemas
    # Check one of them
    one_supported_schema = supported_schemas[0]
    assert one_supported_schema.name
    assert type(one_supported_schema.steps) == list
    assert one_supported_schema.steps


def test_validate_pipeline_valid(config):
    assert validate_pipeline(Pipeline(config))


def test__add_step():
    schema = PipelineSchema("bad-schema")
    assert schema.steps == []
    schema._add_step("foo")
    assert schema.steps == ["foo"]
    schema._add_step("bar")
    assert schema.steps == ["foo", "bar"]


def test_validate_pipeline_bad_step(test_dir, mocker):
    """Test that a pipeline with a bad step fails validation."""
    mocker.patch(
        "linker.pipeline.Pipeline._get_steps_root_dir", return_value=Path(test_dir) / "steps"
    )
    config = Config(
        f"{test_dir}/bad_step_pipeline.yaml",  # bad pipeline definition
        f"{test_dir}/input_data.yaml",
        f"{test_dir}/environment.yaml",
    )
    pipeline = Pipeline(config)
    with pytest.raises(RuntimeError, match="Pipeline is not valid."):
        validate_pipeline(pipeline)


@pytest.mark.skip(reason="TODO when multiple steps are implemented")
def test_bad_step_order():
    pass


@pytest.mark.skip(reason="TODO when multiple steps are implemented")
def test_missing_a_step():
    pass