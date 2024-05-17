from typing import Callable

from easylink.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema
from easylink.step import Step


def test__add_step():
    schema = PipelineSchema("test_schema", lambda *_: None)
    assert schema.steps == []
    schema._add_step("foo")._add_step("bar")
    assert schema.steps == ["foo", "bar"]


def test__generate_schema():
    schema = PipelineSchema._generate_schema(
        "test_schema",
        lambda *_: None,
        Step("step_1", prev_input=True, input_files=False),
        Step("step_2", prev_input=True, input_files=False),
    )
    assert schema.name == "test_schema"
    assert isinstance(schema.validate_input, Callable)
    assert schema.steps == [Step("step_1", prev_input=True, input_files=False), Step("step_2", prev_input=True, input_files=False)]


def test_get_schemas():
    supported_schemas = PIPELINE_SCHEMAS
    assert isinstance(supported_schemas, list)
    # Ensure list is populated
    assert supported_schemas
    # Check basic structure
    for schema in supported_schemas:
        assert schema.name
        assert schema.steps
        assert isinstance(schema.validate_input, Callable)
        assert isinstance(schema.steps, list)
        for step in schema.steps:
            assert isinstance(step, Step)
            assert step.name
            assert isinstance(step.input_validator, Callable)


def test__add_step():
    schema = PipelineSchema("bad-schema", lambda *_: None)
    assert schema.steps == []
    schema._add_step("foo")
    assert schema.steps == ["foo"]
    schema._add_step("bar")
    assert schema.steps == ["foo", "bar"]
